// AxtpClient：单 Session 封装（SDK 层，不知 Connection）。
// 显式状态机：idle → connecting → ready → reconnecting → connecting/ready/closed
// connect() 带超时保护，避免握手卡住永久 hang。
// 订阅 session.onStateChange 驱动 client 状态 + emit 对应事件。

import type { ReconnectPolicy } from "../connection/reconnect/reconnect.js";
import {
  AxtpSession,
  type CallContext,
  type CallOptions,
  type CommonOptions,
  type SessionCloseInfo,
  type SessionLifecycleState,
  type UntypedEventHandler,
  type UntypedMethodHandler
} from "../session/session.js";
import type { Stream } from "../session/stream/stream.js";
import type { IClientTransport } from "../transport/transport.js";
import { AxtpError, ErrorCode } from "../types/error.js";
import { EventStream } from "../types/events.js";
import type {
  EventName,
  EventPayload,
  MethodName,
  MethodRequest,
  MethodResponse
} from "../types/registry.js";

/** SDK Client 生命周期状态机。 */
export type ClientState =
  | "idle" // 构造后未 connect
  | "connecting" // 正在建立连接+握手（含首次和重连后）
  | "ready" // session ready
  | "reconnecting" // 传输断开，重连中
  | "closed"; // 终态

export interface ClientOptions extends CommonOptions {
  reconnect?: ReconnectPolicy;
}

/** connect() 默认超时 ms（覆盖 transport 连接 + 握手全过程）。 */
const DEFAULT_CONNECT_TIMEOUT_MS = 30000;

export class AxtpClient {
  private session: AxtpSession | undefined;
  private clientState: ClientState = "idle";
  /** 首次 onReady 是否已收到（用于区分 onConnect vs onReconnect）。 */
  private firstReady = true;

  // 事件流
  readonly onStateChange = new EventStream<ClientState>();
  readonly onConnect = new EventStream<void>();
  readonly onDisconnect = new EventStream<SessionCloseInfo>();
  readonly onReconnect = new EventStream<{ attempt: number }>();
  readonly onReconnectFailed = new EventStream<void>();
  readonly onError = new EventStream<AxtpError>();

  constructor(
    private readonly transport: IClientTransport,
    private readonly options: ClientOptions = {}
  ) {}

  // ===== 状态 =====

  get state(): ClientState {
    return this.clientState;
  }

  get isReady(): boolean {
    return this.clientState === "ready";
  }

  get isReconnecting(): boolean {
    return this.clientState === "reconnecting";
  }

  get isClosed(): boolean {
    return this.clientState === "closed";
  }

  get sid(): string {
    return this.session?.sid ?? "";
  }

  private setState(newState: ClientState): void {
    if (this.clientState === newState) return;
    if (this.clientState === "closed") return;
    this.clientState = newState;
    this.onStateChange.emit(newState);
  }

  // ===== 连接 =====

  /** 首次连接。带超时保护——transport 连接 + 握手超时后 reject。 */
  async connect(timeoutMs: number = DEFAULT_CONNECT_TIMEOUT_MS): Promise<void> {
    if (this.clientState !== "idle") {
      throw new AxtpError(ErrorCode.InvalidState, `cannot connect from state ${this.clientState}`);
    }
    this.setState("connecting");

    try {
      const transport = await this.transport.connect();
      this.session = new AxtpSession(transport, {
        physicalRole: "client",
        logicalRole: this.options.logicalRole ?? "server",
        defaultTimeoutMs: this.options.defaultTimeoutMs,
        handshakeTimeoutMs: this.options.handshakeTimeoutMs,
        reconnect: this.options.reconnect,
        heartbeatIntervalMs: this.options.heartbeatIntervalMs,
        heartbeatTimeoutMs: this.options.heartbeatTimeoutMs,
        maxFrameSize: this.options.maxFrameSize,
        transportFactory: () => this.transport.connect()
      });

      // 订阅 session 事件
      this.session.onStateChange.subscribe((s) => this.onSessionStateChange(s));
      this.session.onClose.subscribe((info) => this.onDisconnect.emit(info));
      this.session.onReconnect.subscribe((info) => this.onReconnect.emit(info));
      this.session.onReconnectFailed.subscribe(() => this.onReconnectFailed.emit(undefined));
      this.session.onError.subscribe((err) => this.onError.emit(err));

      // 等待首次 ready（带超时）
      await this.waitForReady(timeoutMs);
      this.setState("ready");
      this.onConnect.emit(undefined);
    } catch (err) {
      this.setState("closed");
      throw err instanceof AxtpError
        ? err
        : new AxtpError(ErrorCode.TransportDisconnected, "connect failed", err);
    }
  }

  /** 等待 session 首次 ready，带超时。 */
  private waitForReady(timeoutMs: number): Promise<void> {
    const session = this.session;
    if (session === undefined) {
      return Promise.reject(new AxtpError(ErrorCode.InvalidState, "session not created"));
    }
    if (session.isReady) return Promise.resolve();

    return new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => {
        unsub();
        reject(new AxtpError(ErrorCode.Timeout, `connect timed out after ${timeoutMs}ms`));
      }, timeoutMs);

      const unsub = session.onReady.subscribe(() => {
        clearTimeout(timer);
        unsub();
        resolve();
      });

      // 如果 session 在等待期间关闭，reject
      const closeUnsub = session.onClose.subscribe(() => {
        clearTimeout(timer);
        unsub();
        closeUnsub();
        reject(new AxtpError(ErrorCode.TransportDisconnected, "session closed before ready"));
      });
    });
  }

  /** Session 状态变化 → 更新 Client 状态 + emit 事件。 */
  private onSessionStateChange(sessionState: SessionLifecycleState): void {
    switch (sessionState) {
      case "ready":
        if (this.firstReady) {
          this.firstReady = false;
        }
        this.setState("ready");
        break;
      case "reconnecting":
        this.setState("reconnecting");
        break;
      case "connecting":
        // 重连后重新握手（非首次）
        if (!this.firstReady) {
          this.setState("connecting");
        }
        break;
      case "closed":
        this.setState("closed");
        break;
    }
  }

  // ===== 四件套（转发 Session）=====

  call<K extends MethodName>(
    method: K,
    params: MethodRequest<K>,
    options?: CallOptions
  ): Promise<MethodResponse<K>>;
  call(method: string, params: unknown, options?: CallOptions): Promise<unknown>;
  call(method: string, params: unknown, options?: CallOptions): Promise<unknown> {
    this.requireUsable();
    return this.session!.call(method, params, options);
  }

  handle<K extends MethodName>(
    method: K,
    handler: (ctx: CallContext, params: MethodRequest<K>) => MethodResponse<K> | Promise<MethodResponse<K>>
  ): () => void;
  handle(method: string, handler: UntypedMethodHandler): () => void;
  handle(
    method: string,
    handler: (ctx: CallContext, params: unknown) => unknown | Promise<unknown>
  ): () => void {
    this.requireConnected();
    return this.session!.handle(method, handler as UntypedMethodHandler);
  }

  emit<K extends EventName>(event: K, payload: EventPayload<K>): Promise<void>;
  emit(event: string, payload: unknown): Promise<void>;
  emit(event: string, payload: unknown): Promise<void> {
    this.requireUsable();
    return this.session!.emit(event, payload);
  }

  on<K extends EventName>(event: K, handler: (payload: EventPayload<K>) => void): () => void;
  on(event: string, handler: UntypedEventHandler): () => void;
  on(event: string, handler: UntypedEventHandler): () => void {
    this.requireConnected();
    return this.session!.on(event, handler);
  }

  // ===== Stream =====

  openStream(
    method: string,
    params: unknown,
    options?: CallOptions
  ): Promise<{ streamId: number; response: unknown; stream: Stream }> {
    this.requireUsable();
    return this.session!.openStream(method, params, options);
  }

  onStream(
    method: string,
    handler: (ctx: CallContext, params: unknown) => unknown | Promise<unknown>
  ): () => void {
    this.requireConnected();
    return this.session!.onStream(method, handler);
  }

  /** 主动关闭，不再重连。 */
  close(): void {
    this.session?.close();
    this.setState("closed");
  }

  private requireUsable(): void {
    if (this.clientState === "closed")
      throw new AxtpError(ErrorCode.TransportDisconnected, "client closed");
    if (this.clientState === "reconnecting")
      throw new AxtpError(ErrorCode.TransportDisconnected, "client reconnecting");
    if (this.clientState !== "ready")
      throw new AxtpError(ErrorCode.InvalidState, "client not ready");
    if (this.session === undefined)
      throw new AxtpError(ErrorCode.InvalidState, "client not ready");
  }

  private requireConnected(): void {
    if (this.clientState === "closed")
      throw new AxtpError(ErrorCode.TransportDisconnected, "client closed");
    if (this.session === undefined)
      throw new AxtpError(ErrorCode.InvalidState, "client not connected");
  }
}
