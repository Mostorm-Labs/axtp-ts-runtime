// AxtpSession：RPC Session 门面（会话语义，暴露给上层）。
// 组合子组件（HandshakeOrchestrator/RpcExchange/StreamManager/HandlerRouter），
// 创建+持有 Connection，订阅其事件。
//
// 显式状态机：connecting → ready → reconnecting → connecting → ready / → closed
// onReady: EventStream<void>（每次握手成功都 emit，支持重连后再次 ready）
// onStateChange: EventStream<SessionLifecycleState>（状态转换通知）

import { Connection, type ConnectionOptions } from "../connection/connection.js";
import type { RpcPayload } from "../protocol/model.js";
import { RpcOp } from "../protocol/model.js";
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
import { HandshakeOrchestrator } from "./handshake/handshakeOrchestrator.js";
import { RpcExchange } from "./rpc/rpcExchange.js";
import type { Stream } from "./stream/stream.js";
import { StreamManager } from "./stream/streamManager.js";
import type {
  CallContext,
  CallOptions,
  CommonOptions,
  EventHandler,
  GlobalHandlerSource,
  MethodHandler,
  SessionCloseInfo,
  SessionConfig,
  SessionIO,
  UntypedEventHandler,
  UntypedMethodHandler
} from "./types.js";

/** Session 生命周期状态机。 */
export type SessionLifecycleState =
  | "connecting" // 正在建立连接+握手（含首次和重连后）
  | "ready" // APP_READY
  | "reconnecting" // 传输断开，Connection 正在重连
  | "closed"; // 终态

export type {
  CallContext,
  CallOptions,
  CommonOptions,
  EventHandler,
  GlobalHandlerSource,
  MethodHandler,
  SessionCloseInfo,
  SessionConfig,
  UntypedEventHandler,
  UntypedMethodHandler
};

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

  // 事件流
  readonly onReady = new EventStream<void>();
  readonly onStateChange = new EventStream<SessionLifecycleState>();
  readonly onClose = new EventStream<SessionCloseInfo>();
  readonly onReconnect = new EventStream<{ attempt: number }>();
  readonly onReconnectFailed = new EventStream<void>();
  readonly onError = new EventStream<AxtpError>();

  // 状态
  private sessionState: SessionLifecycleState = "connecting";
  private readonly defaultTimeoutMs: number;
  private handshakeTimeoutMs: number;
  private handshakeTimer: ReturnType<typeof setTimeout> | undefined;
  /** 公开 id */
  readonly id: number;

  constructor(transport: ITransport, config: SessionConfig) {
    const physicalRole = config.physicalRole ?? "client";
    this.defaultTimeoutMs = config.defaultTimeoutMs ?? 10000;
    this.id = nextSessionId++;

    // 构造 ConnectionOptions
    const connOptions: ConnectionOptions = {
      heartbeatIntervalMs: config.heartbeatIntervalMs,
      heartbeatTimeoutMs: config.heartbeatTimeoutMs,
      maxFrameSize: config.maxFrameSize,
      reconnect: config.reconnect
    };
    this.conn = new Connection(physicalRole, transport, connOptions, config.transportFactory);

    // SessionIO
    this.io = {
      sendRpc: (p) => this.conn.sendRpc(p),
      sendStream: (streamId, data, seqId, cursor) =>
        this.conn.sendStream({ streamId, seqId, cursor: cursor ?? 0n, data })
    };

    // 子组件
    this.router = new HandlerRouter(config.globalHandlers);
    const logicalRole = config.logicalRole ?? "server";
    this.handshakeOrch = new HandshakeOrchestrator(
      logicalRole,
      this.io,
      config.handshakeSeed,
      config.eventMasks
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
    this.conn.onDisconnect.subscribe((r) => this.handleDisconnect(r));
    this.conn.onClose.subscribe((r) => this.handleClose(r));
    this.conn.onReconnect.subscribe((info) => this.handleReconnect(info.attempt));
    this.conn.onReconnectFailed.subscribe(() => this.onReconnectFailed.emit(undefined));
    this.conn.onError.subscribe((err) => this.onError.emit(err));

    // 握手超时
    this.handshakeTimeoutMs = config.handshakeTimeoutMs ?? 15000;
    this.armHandshakeTimer();

    // 初始状态
    this.setState("connecting");

    this.conn.start();
  }

  // ===== 状态机 =====

  private setState(newState: SessionLifecycleState): void {
    if (this.sessionState === newState) return;
    if (this.sessionState === "closed") return;
    this.sessionState = newState;
    this.onStateChange.emit(newState);
  }

  get state(): SessionLifecycleState {
    return this.sessionState;
  }

  get isReady(): boolean {
    return this.sessionState === "ready";
  }

  get isClosed(): boolean {
    return this.sessionState === "closed";
  }

  get isReconnecting(): boolean {
    return this.sessionState === "reconnecting";
  }

  get sid(): string {
    return this.handshakeOrch.sid;
  }

  close(code: CloseCode = CloseCode.Normal, reason = "local close"): void {
    this.conn.close(code, reason);
  }

  // ===== 四件套 =====

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

  handle<K extends MethodName>(method: K, handler: MethodHandler<K>): () => void;
  handle(method: string, handler: UntypedMethodHandler): () => void;
  handle(
    method: string,
    handler: (ctx: CallContext, params: unknown) => unknown | Promise<unknown>
  ): () => void {
    return this.router.setMethod(method, handler as UntypedMethodHandler);
  }

  emit<K extends EventName>(event: K, payload: EventPayload<K>): Promise<void>;
  emit(event: string, payload: unknown): Promise<void>;
  emit(event: string, payload: unknown): Promise<void> {
    this.requireReady();
    this.rpc.emitEvent(event, payload);
    return Promise.resolve();
  }

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
    handler: (ctx: CallContext, params: unknown, stream: Stream) => unknown | Promise<unknown>
  ): () => void {
    return this.handle(method, (async (ctx: unknown, params: unknown) => {
      const { result } = await this.streamMgr.wrapStreamHandler(
        async (p, stream) => handler(ctx as CallContext, p, stream)
      )(ctx, params);
      return result;
    }) as UntypedMethodHandler);
  }

  // ===== 入站总入口 =====

  private ingest(payload: RpcPayload): void {
    if (this.sessionState === "closed") return;

    // 会话握手
    if (
      (this.sessionState === "connecting" || this.sessionState === "reconnecting") &&
      HandshakeOrchestrator.isHandshakeOp(payload.op)
    ) {
      const result = this.handshakeOrch.ingest(payload);
      if (result.error) {
        this.conn.close(CloseCode.HandshakeFailed, result.error.message);
        return;
      }
      if (result.becameReady) {
        this.setState("ready");
        this.clearHandshakeTimer();
        this.onReady.emit(undefined);
      }
      return;
    }

    // 未 ready 的业务请求 -> CONTROL_OPEN_REQUIRED
    if (this.sessionState !== "ready") {
      this.rpc.respondOpenRequired(payload);
      return;
    }

    // APP_READY 后校验 sid（spec:211: malformed/empty/non-hex/zero/缺失的 sid MUST 拒绝）
    const payloadSid = payload.jsonSid ?? "";
    if (payloadSid !== this.handshakeOrch.sid) {
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

  /** Connection 断连通知（有/无重连策略都触发）。 */
  private handleDisconnect(reason: CloseReason): void {
    // 进入 reconnecting 状态
    this.setState("reconnecting");
    // 立即重置握手状态（不等 onReconnect），确保重连链路重建时握手从头开始
    this.handshakeOrch.reset();
    // pending call/stream 全部失败
    this.rpc.rejectAll(
      new AxtpError(ErrorCode.TransportDisconnected, `connection disconnected: ${reason.reason}`)
    );
    this.streamMgr.abortAll(`connection disconnected: ${reason.reason}`);
  }

  /** Connection 传输重连成功后：重建会话。 */
  private handleReconnect(attempt: number): void {
    // 进入 connecting（重新握手）——握手状态已在 handleDisconnect 里 reset
    this.setState("connecting");
    this.armHandshakeTimer();
    this.onReconnect.emit({ attempt });
  }

  // ===== 握手超时 =====

  private armHandshakeTimer(): void {
    this.clearHandshakeTimer();
    this.handshakeTimer = setTimeout(() => {
      try {
        if (this.sessionState === "connecting" || this.sessionState === "reconnecting") {
          this.close(CloseCode.HandshakeFailed, "handshake timeout");
        }
      } catch (err) {
        this.onError.emit(
          err instanceof AxtpError
            ? err
            : new AxtpError(ErrorCode.InternalError, "handshake timer error", err)
        );
      }
    }, this.handshakeTimeoutMs);
  }

  private clearHandshakeTimer(): void {
    if (this.handshakeTimer !== undefined) {
      clearTimeout(this.handshakeTimer);
      this.handshakeTimer = undefined;
    }
  }

  // ===== 内部 =====

  private makeCallContext(requestId: number): CallContext {
    return {
      requestId,
      sid: this.sid,
      emit: (event, payload) => this.emit(event, payload)
    };
  }

  private requireReady(): void {
    if (this.sessionState === "closed")
      throw new AxtpError(ErrorCode.TransportDisconnected, "session closed");
    if (this.sessionState === "reconnecting")
      throw new AxtpError(ErrorCode.TransportDisconnected, "session reconnecting");
    if (this.sessionState !== "ready")
      throw new AxtpError(ErrorCode.InvalidState, "session not ready");
  }

  private handleClose(reason: CloseReason): void {
    if (this.sessionState === "closed") return;
    this.setState("closed");

    this.clearHandshakeTimer();

    this.rpc.rejectAll(
      new AxtpError(ErrorCode.TransportDisconnected, `connection closed: ${reason.reason}`)
    );
    this.streamMgr.abortAll(`connection closed: ${reason.reason}`);

    this.onClose.emit({
      code: reason.code,
      reason: reason.reason,
      remote: reason.remote
    });

    // 关闭所有 EventStream
    this.onReady.close();
    this.onStateChange.close();
    this.onClose.close();
    this.onReconnect.close();
    this.onReconnectFailed.close();
    this.onError.close();
  }
}
