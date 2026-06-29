// AxtpSession：RPC Session 门面（会话语义，暴露给上层）。
// 组合子组件（HandshakeOrchestrator/RpcExchange/StreamManager/HandlerRouter），
// 创建+持有 Connection，订阅其事件。
//
// 单一职责：Session 只做"组合编排 + 生命周期 + 会话重建"。
// 重连会话重建：Connection 管传输重连（onReconnect），Session 监听后重建握手（HandshakeOrchestrator.reset）。
// handler 表在 Session 里，重连不换 Session 实例，表自然保留——无需快照迁移。

import { Connection, type ConnectionOptions } from "../connection/connection.js";
import { RpcOp } from "../protocol/generated/axtp_ids_generated.js";
import type { RpcPayload } from "../protocol/model.js";
import type { CloseReason, ITransport } from "../transport/transport.js";
import { CloseCode } from "../transport/transport.js";
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
  SessionConfig,
  SessionIO,
  UntypedEventHandler,
  UntypedMethodHandler
} from "./types.js";

// 重新导出类型（公共 API）
export type {
  CallContext,
  CallOptions,
  CommonOptions,
  EventHandler,
  GlobalHandlerSource,
  MethodHandler,
  SessionCloseInfo,
  SessionConfig,
  SessionInternalConfig,
  SessionOptions,
  UntypedEventHandler,
  UntypedMethodHandler
} from "./types.js";

let nextSessionId = 1;

export class AxtpSession {
  private conn: Connection;

  // 子组件
  private readonly router: HandlerRouter;
  private readonly handshakeOrch: HandshakeOrchestrator;
  private readonly rpc: RpcExchange;
  private readonly streamMgr: StreamManager;

  // SessionIO：子组件通过此发送（转发到 Connection）
  private readonly io: SessionIO;

  private readonly onReadyStream = new EventStream<void>();
  private readonly onCloseStream = new EventStream<SessionCloseInfo>();
  private readonly onReconnectStream = new EventStream<{
    attempt: number;
  }>();
  private readonly onReconnectFailedStream = new EventStream<void>();
  private readonly onErrorStream = new EventStream<AxtpError>();

  private ready = false;
  private closed = false;
  private readonly defaultTimeoutMs: number;
  private handshakeTimeoutMs: number;
  private handshakeTimer: ReturnType<typeof setTimeout> | undefined;
  /** 公开 id（简短随机字符串，仅在创建它的 server/client 内有效，非全局唯一）。 */
  readonly id: number;

  constructor(transport: ITransport, config: SessionConfig) {
    const physicalRole = config.physicalRole ?? "client";
    this.defaultTimeoutMs = config.defaultTimeoutMs ?? 10000;
    this.id = nextSessionId++; // B6: 自增计数器，避免随机碰撞

    // 构造 ConnectionOptions（不暴露 negotiationParams 等链路细节给用户）
    const connOptions: ConnectionOptions = {
      heartbeatIntervalMs: config.heartbeatIntervalMs,
      heartbeatTimeoutMs: config.heartbeatTimeoutMs,
      maxFrameSize: config.maxFrameSize,
      reconnect: config.reconnect
    };
    this.conn = new Connection(physicalRole, transport, connOptions, config.transportFactory);

    // SessionIO：子组件通过此发送（转发到 Connection）
    this.io = {
      sendRpc: (p) => this.conn.sendRpc(p),
      sendStream: (streamId, data, seqId) =>
        this.conn.sendStream({ streamId, seqId, cursor: 0n, data })
    };

    // 子组件
    this.router = new HandlerRouter(config.globalHandlers);
    const logicalRole = config.logicalRole ?? "server";
    this.handshakeOrch = new HandshakeOrchestrator(
      logicalRole,
      this.io,
      config.handshakeSeed
    );
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
    this.conn.onReconnectFailed.subscribe(() => this.onReconnectFailedStream.emit(undefined));
    this.conn.onError.subscribe((err) => this.onErrorStream.emit(err));

    // 握手超时配置（重连时复用）
    this.handshakeTimeoutMs = config.handshakeTimeoutMs ?? 15000;

    // arm 首次握手超时
    this.armHandshakeTimer();

    // onReady Promise：握手完成 resolve。
    // 生命周期通过 onClose 事件传达。
    this.onReady = new Promise<void>((resolve) => {
      this.onReadyResolve = resolve;
      this.onReadyStream.subscribe(() => {
        if (this.handshakeTimer !== undefined) {
          clearTimeout(this.handshakeTimer);
          this.handshakeTimer = undefined;
        }
        resolve();
      });
    });

    this.conn.start();
  }

  // ===== 生命周期 =====

  private onReadyResolve?: () => void;
  readonly onReady: Promise<void>;

  get onClose(): EventStream<SessionCloseInfo> {
    return this.onCloseStream;
  }

  get onError(): EventStream<AxtpError> {
    return this.onErrorStream;
  }

  get onReconnect(): EventStream<{ attempt: number }> {
    return this.onReconnectStream;
  }

  get onReconnectFailed(): EventStream<void> {
    return this.onReconnectFailedStream;
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
    return this.handshakeOrch.state;
  }

  close(code: CloseCode = CloseCode.Normal, reason = "local close"): void {
    // 通过 Connection.close() 关闭底层 socket，再由 conn.onClose → handleClose 统一清理
    this.conn.close(code, reason);
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

  /**
   * 注册建流 handler（server 端）。handler 返回含 streamId 的 result，
   * StreamManager 自动创建 receive Stream，通过 onStreamReady 回调交给调用方。
   */
  onStream(
    method: string,
    handler: (ctx: CallContext, params: unknown) => unknown | Promise<unknown>,
    onStreamReady?: (stream: Stream) => void
  ): () => void {
    return this.handle(method, (async (ctx: unknown, params: unknown) => {
      const { result, stream } = await this.streamMgr.wrapStreamHandler(async (p) =>
        handler(ctx as CallContext, p)
      )(ctx, params);
      onStreamReady?.(stream);
      return result;
    }) as UntypedMethodHandler);
  }

  // ===== 入站总入口 =====

  private ingest(payload: RpcPayload): void {
    if (this.closed) return;

    // 会话握手
    if (!this.ready && HandshakeOrchestrator.isHandshakeOp(payload.op)) {
      const result = this.handshakeOrch.ingest(payload);
      if (result.error) {
        // 握手错误（如 axtpVersion 不兼容、畸形 body）→ 关闭连接
        this.conn.close(CloseCode.HandshakeFailed, result.error.message);
        return;
      }
      if (result.becameReady) {
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
    // B2: 重连后旧 sid 已失效，pending call/stream 必须全部失败
    this.rpc.rejectAll(new AxtpError(ErrorCode.TransportDisconnected, "connection reconnecting"));
    this.streamMgr.abortAll("connection reconnecting");
    this.handshakeOrch.reset();
    // 重新 arm 握手超时定时器（首次的 timer 已在握手完成时清除）
    this.armHandshakeTimer();
    this.onReconnectStream.emit({ attempt: _attempt });
  }

  /** Arm 握手超时定时器：超时后 close session（防止握手卡住永久 hang）。 */
  private armHandshakeTimer(): void {
    if (this.handshakeTimer !== undefined) clearTimeout(this.handshakeTimer);
    this.handshakeTimer = setTimeout(() => {
      try {
        if (!this.ready && !this.closed) {
          this.close(CloseCode.HandshakeFailed, "handshake timeout");
        }
      } catch (err) {
        // timer 回调里的异常不能冒泡到 Node 进程，转发到 onError
        this.onErrorStream.emit(
          err instanceof AxtpError
            ? err
            : new AxtpError(ErrorCode.InternalError, "handshake timer error", err)
        );
      }
    }, this.handshakeTimeoutMs);
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
    this.onReconnectFailedStream.close();
    this.onErrorStream.close();
  }
}
