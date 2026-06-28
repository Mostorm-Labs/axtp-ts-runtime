// AxtpSession：RPC Session 门面（会话语义，暴露给上层）。
// 组合子组件（HandshakeOrchestrator/RpcExchange/StreamManager/HandlerRouter），
// 创建+持有 Connection，订阅其事件。
//
// 单一职责：Session 只做"组合编排 + 生命周期 + 会话重建"。
// 重连会话重建：Connection 管传输重连（onReconnect），Session 监听后重建握手（HandshakeOrchestrator.reset）。
// handler 表在 Session 里，重连不换 Session 实例，表自然保留——无需快照迁移。

import { Connection, type ConnectionOptions } from "../connection/connection.js";
import { RpcOp } from "../protocol/generated/axtp_ids_generated.js";
import type { RpcPayload, StreamPayload } from "../protocol/model.js";
import type { CloseReason, ITransport, LogicalRole, PhysicalRole } from "../transport/transport.js";
import { AxtpError, ErrorCode } from "../types/error.js";
import { EventStream } from "../types/events.js";
import type {
  EventName,
  EventPayload,
  MethodName,
  MethodRequest,
  MethodResponse
} from "../types/registry.js";
import { HandlerRouter } from "./handler/handlerRouter.js";
import type { SessionState } from "./handshake/handshake.js";
import { HandshakeOrchestrator } from "./handshake/handshakeOrchestrator.js";
import { RpcExchange } from "./rpc/rpcExchange.js";
import type { Stream } from "./stream/stream.js";
import { StreamManager } from "./stream/streamManager.js";
import type {
  CallContext,
  CallOptions,
  EventHandler,
  MethodHandler,
  SessionCloseInfo,
  SessionIO,
  SessionOptions,
  UntypedEventHandler,
  UntypedMethodHandler
} from "./types.js";

// 重新导出类型（公共 API）
export type {
  CallContext,
  CallOptions,
  EventHandler,
  GlobalHandlerSource,
  MethodHandler,
  SessionCloseInfo,
  SessionOptions,
  UntypedEventHandler,
  UntypedMethodHandler
} from "./types.js";

let nextSessionId = 1;

export class AxtpSession {
  private conn: Connection;
  private readonly logicalRole: LogicalRole;
  private readonly physicalRole: PhysicalRole;

  // 子组件
  private readonly router: HandlerRouter;
  private readonly handshakeOrch: HandshakeOrchestrator;
  private readonly rpc: RpcExchange;
  private readonly streamMgr: StreamManager;

  // SessionIO：子组件通过此发送（转发到 Connection）
  private readonly io: SessionIO & { sendStream: (p: StreamPayload) => void };

  private readonly onReadyStream = new EventStream<void>();
  private readonly onCloseStream = new EventStream<SessionCloseInfo>();
  private readonly onReconnectStream = new EventStream<{
    attempt: number;
    totalDowntimeMs: number;
  }>();
  private readonly onErrorStream = new EventStream<AxtpError>();

  private ready = false;
  private closed = false;
  private readonly defaultTimeoutMs: number;
  private handshakeTimer: ReturnType<typeof setTimeout> | undefined;
  /** 公开 id（仅在创建它的 server/client 内有效，非全局唯一）。 */
  readonly id: number;

  constructor(transport: ITransport, options: SessionOptions = {}) {
    const physicalRole = options.physicalRole ?? "client";
    const logicalRole = options.logicalRole ?? "server";
    this.physicalRole = physicalRole;
    this.logicalRole = logicalRole;
    this.defaultTimeoutMs = options.defaultTimeoutMs ?? 10000;
    this.id = nextSessionId++;

    // 构造 ConnectionOptions（不暴露 negotiationParams 等链路细节给用户）
    const connOptions: ConnectionOptions = {
      heartbeatIntervalMs: options.heartbeatIntervalMs,
      heartbeatTimeoutMs: options.heartbeatTimeoutMs,
      maxFrameSize: options.maxFrameSize,
      reconnect: options.reconnect
    };
    this.conn = new Connection(physicalRole, transport, connOptions, options.transportFactory);

    // SessionIO：子组件通过此发送（转发到 Connection）
    this.io = {
      sendRpc: (p) => this.conn.sendRpc(p),
      sendStream: (p) => this.conn.sendStream(p)
    };

    // 子组件
    this.router = new HandlerRouter(options.globalHandlers);
    this.handshakeOrch = new HandshakeOrchestrator(logicalRole, this.io, options.handshakeSeed);
    this.rpc = new RpcExchange(
      this.io,
      this.router,
      () => this.handshakeOrch.sid,
      (requestId) => this.makeCallContext(requestId)
    );
    this.streamMgr = new StreamManager(this.io);

    // 订阅 Connection 事件
    this.conn.onPayload.subscribe((p) => this.ingest(p));
    this.conn.onStream.subscribe((s) => this.streamMgr.onData(s));
    this.conn.onLinkReady.subscribe(() => this.onLinkReady());
    this.conn.onClose.subscribe((r) => this.handleClose(r));
    this.conn.onReconnect.subscribe((info) => this.handleReconnect(info.attempt));
    this.conn.onError.subscribe((err) => this.onErrorStream.emit(err));

    // H3：握手超时（防止 onReady 永不 resolve）
    const timeoutMs = options.handshakeTimeoutMs ?? 15000;
    this.handshakeTimer = setTimeout(() => {
      if (!this.ready && !this.closed) {
        this.handleClose({
          code: 3, // CloseCode.HandshakeFailed
          reason: "handshake timeout",
          remote: false
        });
      }
    }, timeoutMs);

    // onReady Promise：握手完成 resolve；close 前 reject（若有人 await）。
    // noop catch 防止 unhandled rejection（测试中 session close 但无人 await onReady）。
    this.onReady = new Promise<void>((resolve, reject) => {
      this.onReadyReject = reject;
      this.onReadyStream.subscribe(() => {
        if (this.handshakeTimer !== undefined) {
          clearTimeout(this.handshakeTimer);
          this.handshakeTimer = undefined;
        }
        resolve();
      });
    });
    this.onReady.catch(() => {});

    this.conn.start();
  }

  // ===== 生命周期 =====

  private onReadyReject?: (err: AxtpError) => void;
  readonly onReady: Promise<void>;

  get onReadyEvent(): EventStream<void> {
    return this.onReadyStream;
  }

  get onClose(): EventStream<SessionCloseInfo> {
    return this.onCloseStream;
  }

  get onError(): EventStream<AxtpError> {
    return this.onErrorStream;
  }

  get onReconnect(): EventStream<{ attempt: number; totalDowntimeMs: number }> {
    return this.onReconnectStream;
  }

  get isReady(): boolean {
    return this.ready;
  }

  get isClosed(): boolean {
    return this.closed;
  }

  get sid(): string {
    return this.handshakeOrch.sid;
  }

  get state(): SessionState {
    return this.handshakeOrch.state as SessionState;
  }

  close(): void {
    // M5：只调 conn.close()，让 handleClose 统一清理
    this.conn.close();
  }

  // ===== typed 四件套（重载）=====

  /** call：内置 typed / vendor untyped */
  call<K extends MethodName>(
    method: K,
    params: MethodRequest<K>,
    options?: CallOptions
  ): Promise<MethodResponse<K>>;
  call(method: string, params: unknown, options?: CallOptions): Promise<unknown>;
  call(method: string, params: unknown, options?: CallOptions): Promise<unknown> {
    this.requireReady();
    return this.rpc.call(method, params, options?.timeoutMs ?? this.defaultTimeoutMs);
  }

  /** handle：内置 typed / vendor untyped */
  handle<K extends MethodName>(method: K, handler: MethodHandler<K>): () => void;
  handle(method: string, handler: UntypedMethodHandler): () => void;
  handle(
    method: string,
    handler: (ctx: CallContext, params: unknown) => unknown | Promise<unknown>
  ): () => void {
    return this.router.setMethod(method, handler as UntypedMethodHandler);
  }

  removeHandler(method: string, handler: UntypedMethodHandler): void {
    this.router.removeMethod(method, handler);
  }

  /** emit：内置 typed / vendor untyped */
  emit<K extends EventName>(event: K, payload: EventPayload<K>): Promise<void>;
  emit(event: string, payload: unknown): Promise<void>;
  emit(event: string, payload: unknown): Promise<void> {
    this.requireReady();
    return this.rpc.emitEvent(event, payload);
  }

  /** on：内置 typed / vendor untyped */
  on<K extends EventName>(event: K, handler: EventHandler<K>): () => void;
  on(event: string, handler: UntypedEventHandler): () => void;
  on(event: string, handler: UntypedEventHandler): () => void {
    return this.router.addEventListener(event, handler);
  }

  // ===== STREAM =====

  async openStream(
    method: string,
    params: unknown,
    options?: CallOptions
  ): Promise<{ streamId: number; response: unknown; stream: Stream }> {
    this.requireReady();
    return this.streamMgr.openStream(
      (m, p) => this.rpc.call(m, p, options?.timeoutMs ?? this.defaultTimeoutMs),
      method,
      params
    );
  }

  onStream(
    method: string,
    handler: (ctx: CallContext, params: unknown) => unknown | Promise<unknown>
  ): () => void {
    // M3：onStreamCreated 把 Stream 存到闭包变量（handler 可在返回后通过外部引用访问）
    const streamHolder: { stream?: Stream } = {};
    return this.handle(method, ((ctx: unknown, params: unknown) =>
      this.streamMgr.wrapStreamHandler(
        async (p) => handler(ctx as CallContext, p),
        (stream) => {
          streamHolder.stream = stream;
        }
      )(ctx, params)) as UntypedMethodHandler);
  }

  // ===== 入站总入口 =====

  private ingest(payload: RpcPayload): void {
    if (this.closed) return;

    // 会话握手
    if (!this.ready && HandshakeOrchestrator.isHandshakeOp(payload.op)) {
      const becameReady = this.handshakeOrch.ingest(payload);
      if (becameReady) {
        this.ready = true;
        this.onReadyStream.emit(undefined);
      }
      return;
    }

    // 未 ready 的业务请求 -> CONTROL_OPEN_REQUIRED
    if (!this.ready) {
      this.rpc.rejectNotReady(payload);
      return;
    }

    // APP_READY 业务分发
    switch (payload.op) {
      case RpcOp.Request:
        this.rpc.dispatchRequest(payload);
        break;
      case RpcOp.RequestResponse:
        this.rpc.resolveResponse(payload);
        break;
      case RpcOp.Event:
        this.rpc.dispatchEvent(payload);
        break;
    }
  }

  private onLinkReady(): void {
    this.handshakeOrch.onLinkReady();
  }

  /** Connection 传输重连成功后：重建会话（重置握手，重新走 Hello/Identify）。 */
  private handleReconnect(_attempt: number): void {
    this.ready = false;
    this.handshakeOrch.reset();
    this.onReconnectStream.emit({ attempt: _attempt, totalDowntimeMs: 0 });
  }

  private makeCallContext(requestId: number): CallContext {
    return {
      requestId,
      sid: this.sid,
      emit: (event, payload) => this.emit(event, payload)
    };
  }

  private requireReady(): void {
    if (this.closed) throw new AxtpError(ErrorCode.TransportDisconnected, "session closed");
    if (!this.ready) throw new AxtpError(ErrorCode.InvalidState, "session not ready");
  }

  /** M4+M5：接收完整 CloseReason，统一清理所有资源 */
  private handleClose(reason: CloseReason): void {
    if (this.closed) return;
    this.closed = true;
    this.ready = false;

    // 清理握手超时 timer
    if (this.handshakeTimer !== undefined) {
      clearTimeout(this.handshakeTimer);
      this.handshakeTimer = undefined;
    }

    // 清理 pending + stream
    this.rpc.rejectAll(
      new AxtpError(ErrorCode.TransportDisconnected, `connection closed: ${reason.reason}`)
    );
    this.streamMgr.abortAll(`connection closed: ${reason.reason}`);

    // 如果 onReady 还没 resolve（握手未完成），reject 它
    if (!this.ready && this.onReadyReject !== undefined) {
      this.onReadyReject(
        new AxtpError(ErrorCode.TransportDisconnected, "session closed before ready")
      );
    }

    // M4：保留完整 CloseCode
    this.onCloseStream.emit({
      code: reason.code,
      reason: reason.reason,
      remote: reason.remote
    });

    // M5：统一关闭所有 EventStream
    this.onReadyStream.close();
    this.onCloseStream.close();
    this.onReconnectStream.close();
    this.onErrorStream.close();
  }
}
