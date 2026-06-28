// AxtpSession：RPC Session 门面（会话语义，暴露给上层）。
// 组合子组件（HandshakeOrchestrator/RpcExchange/StreamManager/HandlerRouter），
// 创建+持有 Connection，订阅其事件。
//
// 单一职责：Session 只做"组合编排 + 生命周期 + 会话重建"。
// 重连会话重建：Connection 管传输重连（onReconnect），Session 监听后重建握手（HandshakeOrchestrator.reset）。
// handler 表在 Session 里，重连不换 Session 实例，表自然保留——无需快照迁移。

import { Connection, type ConnectionOptions } from "../protocol/connection.js";
import type { SessionState } from "../protocol/engine/handshake.js";
import { RpcOp } from "../protocol/generated/axtp_ids_generated.js";
import type { RpcPayload, StreamPayload } from "../protocol/model.js";
import type { ITransport, LogicalRole, PhysicalRole } from "../transport/transport.js";
import { AxtpError, ErrorCode } from "../types/error.js";
import { EventStream } from "../types/events.js";
import { HandlerRouter } from "./handlerRouter.js";
import { HandshakeOrchestrator, type SessionIO } from "./handshakeOrchestrator.js";
import { RpcExchange } from "./rpcExchange.js";
import type { Stream } from "./stream.js";
import { StreamManager } from "./streamManager.js";
import type {
  CallContext,
  CallOptions,
  SessionOptions,
  UntypedEventHandler,
  UntypedMethodHandler
} from "./types.js";

// 重新导出类型（公共 API）
export type {
  CallContext,
  CallOptions,
  EventHandler, GlobalHandlerSource, MethodHandler,
  SessionOptions,
  UntypedEventHandler,
  UntypedMethodHandler
} from "./types.js";

let nextSessionId = 1;

export class AxtpSession {
  private conn: Connection;
  private readonly options: SessionOptions;
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
  private readonly onCloseStream = new EventStream<{ reason: string; remote: boolean }>();
  private readonly onReconnectStream = new EventStream<{
    attempt: number;
    totalDowntimeMs: number;
  }>();

  private ready = false;
  private closed = false;
  private readonly defaultTimeoutMs: number;
  /** 公开 id（供 server.call(sessionId) / getSessions 使用）。 */
  readonly id: number;

  constructor(transport: ITransport, options: SessionOptions = {}) {
    this.options = options;
    this.logicalRole = options.logicalRole ?? "server";
    this.physicalRole = options.physicalRole ?? "client";
    this.defaultTimeoutMs = options.defaultTimeoutMs ?? 10000;
    this.id = nextSessionId++;

    // 构造 ConnectionOptions（不暴露 negotiationParams 等链路细节给用户）
    const connOptions: ConnectionOptions = {
      heartbeatIntervalMs: options.heartbeatIntervalMs,
      heartbeatTimeoutMs: options.heartbeatTimeoutMs,
      maxFrameSize: options.maxFrameSize,
      reconnect: options.reconnect
    };
    this.conn = new Connection(
      this.physicalRole!,
      transport,
      connOptions,
      options.transportFactory
    );

    // SessionIO：子组件通过此发送（转发到 Connection）
    this.io = {
      sendRpc: (p) => this.conn.sendRpc(p),
      sendStream: (p) => this.conn.sendStream(p)
    };

    // 子组件
    this.router = new HandlerRouter(options.globalHandlers);
    this.handshakeOrch = new HandshakeOrchestrator(
      this.logicalRole!,
      this.io,
      options.handshakeSeed
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
    this.conn.onClose.subscribe((r) => this.handleClose(r.reason, r.remote));
    this.conn.onReconnect.subscribe((info) => this.handleReconnect(info.attempt));

    this.conn.start();
  }

  // ===== 生命周期 =====

  readonly onReady: Promise<void> = new Promise((resolve) => {
    this.onReadyStream.subscribe(() => resolve());
  });

  get onReadyEvent(): EventStream<void> {
    return this.onReadyStream;
  }

  get onClose(): EventStream<{ reason: string; remote: boolean }> {
    return this.onCloseStream;
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
    if (this.closed) return;
    this.rpc.rejectAll(new AxtpError(ErrorCode.TransportDisconnected, "session closed"));
    this.streamMgr.abortAll("session closed");
    this.conn.close();
    this.handleClose("local close", false);
  }

  // ===== 四件套（转发子组件）=====

  call(method: string, params: unknown, options?: CallOptions): Promise<unknown> {
    this.requireReady();
    return this.rpc.call(method, params, options?.timeoutMs ?? this.defaultTimeoutMs);
  }

  handle(
    method: string,
    handler: (ctx: CallContext, params: unknown) => unknown | Promise<unknown>
  ): () => void {
    return this.router.setMethod(method, handler as UntypedMethodHandler);
  }

  removeHandler(method: string, handler: UntypedMethodHandler): void {
    this.router.removeMethod(method, handler);
  }

  emit(event: string, payload: unknown): Promise<void> {
    this.requireReady();
    return this.rpc.emitEvent(event, payload);
  }

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
    return this.handle(method, ((ctx: unknown, params: unknown) =>
      this.streamMgr.wrapStreamHandler(
        async (p) => handler(ctx as CallContext, p),
        () => {}
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
      reply: (event, payload) => this.emit(event, payload)
    };
  }

  private requireReady(): void {
    if (this.closed) throw new AxtpError(ErrorCode.TransportDisconnected, "session closed");
    if (!this.ready) throw new AxtpError(ErrorCode.InvalidState, "session not ready");
  }

  private handleClose(reason: string, remote: boolean): void {
    if (this.closed) return;
    this.closed = true;
    this.ready = false;
    this.rpc.rejectAll(
      new AxtpError(ErrorCode.TransportDisconnected, `connection closed: ${reason}`)
    );
    this.streamMgr.abortAll(`connection closed: ${reason}`);
    this.onCloseStream.emit({ reason, remote });
    this.onReadyStream.close();
  }
}
