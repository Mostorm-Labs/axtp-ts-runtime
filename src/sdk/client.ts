// AxtpClient：单 Session 封装（SDK 层，不知 Connection）。
// 显式状态机：idle → connecting → ready → reconnecting → connecting/ready/closed
// connect() 带超时保护，避免握手卡住永久 hang。
// 订阅 session.onStateChange 驱动 client 状态 + emit 对应事件。

import { resolvePolicy, type ReconnectPolicy } from "../connection/reconnect/reconnect.js";
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
import type { IClientTransport } from "../transport/contract.js";
import { AxtpError, ErrorCode } from "../types/error.js";
import { EventStream } from "../types/events.js";
import type {
  EventName,
  EventPayload,
  MethodName,
  MethodRequest,
  MethodResponse
} from "../types/registry.js";
import { computeEventMasks } from "../types/registry.js";

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
  /** connect() 被 close() 中断标志：置位后 connect() 静默退出不 throw。 */
  private connectAborted = false;
  /** 已注册的事件名（用于计算 eventMasks 订阅意图，在 connect/重连时注入 Identify）。 */
  private readonly subscribedEvents = new Set<string>();

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

  /**
   * 首次连接。transport 建立与重连统一交给 Connection（factory 模式）：
   *   - reconnect 启用：首次失败也按策略退避重试（无硬超时，持续到 ready 或 maxAttempts 耗尽）
   *   - reconnect 未启用：transport 连接 + 握手超时后 reject
   */
  async connect(timeoutMs: number = DEFAULT_CONNECT_TIMEOUT_MS): Promise<void> {
    if (this.clientState !== "idle") {
      throw new AxtpError(ErrorCode.InvalidState, `cannot connect from state ${this.clientState}`);
    }
    this.connectAborted = false;
    this.setState("connecting");

    const policy = resolvePolicy(this.options.reconnect);
    this.session = new AxtpSession(() => this.transport.connect(), {
      physicalRole: "client",
      // 默认 logical server（发 Hello）：对应 AXTP-WS-CLOUD-REVERSE（Device=physical client=logical server）。
      // 标准 AXTP-TCP（App=physical client=logical client，发 Identify）需显式传 logicalRole:"client"。
      logicalRole: this.options.logicalRole ?? "server",
      defaultTimeoutMs: this.options.defaultTimeoutMs,
      handshakeTimeoutMs: this.options.handshakeTimeoutMs,
      reconnect: this.options.reconnect,
      heartbeatIntervalMs: this.options.heartbeatIntervalMs,
      heartbeatTimeoutMs: this.options.heartbeatTimeoutMs,
      maxFrameSize: this.options.maxFrameSize,
      eventMasks: this.computeEventMasks()
    });
    this.wireSessionEvents();

    try {
      await this.waitForReady(policy.enabled ? Number.POSITIVE_INFINITY : timeoutMs);
      this.setState("ready");
      this.onConnect.emit(undefined);
    } catch (err) {
      // 关闭已创建的 session（含 Connection/心跳定时器/transport），避免资源泄漏。
      this.session?.close();
      this.session = undefined;
      this.setState("closed");
      if (this.connectAborted) return; // 用户 close() 中断，静默退出
      throw err instanceof AxtpError
        ? err
        : new AxtpError(ErrorCode.TransportDisconnected, "connect failed", err);
    }
  }

  /** 订阅 session 事件并转发为 client 事件流。 */
  private wireSessionEvents(): void {
    const session = this.session;
    if (session === undefined) return;
    session.onStateChange.subscribe((s) => this.onSessionStateChange(s));
    session.onClose.subscribe((info) => this.onDisconnect.emit(info));
    session.onReconnect.subscribe((info) => this.onReconnect.emit(info));
    session.onReconnectFailed.subscribe(() => this.onReconnectFailed.emit(undefined));
    session.onError.subscribe((err) => this.onError.emit(err));
  }

  /**
   * 等待 session 首次 ready。
   * timeoutMs=Infinity（reconnect 启用）：不武装超时，持续等到 ready 或 session 关闭（重连耗尽）。
   * 否则：超时后 reject，保护首次 transport 连接 + 握手。
   */
  private waitForReady(timeoutMs: number): Promise<void> {
    const session = this.session;
    if (session === undefined) {
      return Promise.reject(new AxtpError(ErrorCode.InvalidState, "session not created"));
    }
    if (session.isReady) return Promise.resolve();

    return new Promise<void>((resolve, reject) => {
      const armed = timeoutMs !== Number.POSITIVE_INFINITY;
      const timer = armed
        ? setTimeout(() => {
            unsub();
            closeUnsub();
            reject(new AxtpError(ErrorCode.Timeout, `connect timed out after ${timeoutMs}ms`));
          }, timeoutMs)
        : undefined;
      const clear = (): void => {
        if (timer !== undefined) clearTimeout(timer);
      };

      const unsub = session.onReady.subscribe(() => {
        clear();
        unsub();
        closeUnsub();
        resolve();
      });

      // session 关闭（重连耗尽 / 被动关闭 / 用户 close）→ reject
      const closeUnsub = session.onClose.subscribe(() => {
        clear();
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
    const session = this.session;
    if (session === undefined) throw new AxtpError(ErrorCode.InvalidState, "client not ready");
    return session.call(method, params, options);
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
    const session = this.session;
    if (session === undefined) throw new AxtpError(ErrorCode.InvalidState, "client not connected");
    return session.handle(method, handler as UntypedMethodHandler);
  }

  emit<K extends EventName>(event: K, payload: EventPayload<K>): Promise<void>;
  emit(event: string, payload: unknown): Promise<void>;
  emit(event: string, payload: unknown): Promise<void> {
    this.requireUsable();
    const session = this.session;
    if (session === undefined) throw new AxtpError(ErrorCode.InvalidState, "client not ready");
    return session.emit(event, payload);
  }

  /**
   * 订阅事件。注意：eventMasks 仅在 connect / 重连握手时注入 Identify；
   * connect 之后新增的订阅不会立即通知对端（REIDENTIFY 是 spec draft，Phase 1 未实现），
   * 需等下次重连握手才生效。运行时订阅意图重发参见 {@link updateSubscriptions}。
   */
  on<K extends EventName>(event: K, handler: (payload: EventPayload<K>) => void): () => void;
  on(event: string, handler: UntypedEventHandler): () => void;
  on(event: string, handler: UntypedEventHandler): () => void {
    this.requireConnected();
    const session = this.session;
    if (session === undefined) throw new AxtpError(ErrorCode.InvalidState, "client not connected");
    this.subscribedEvents.add(event);
    const unsub = session.on(event, handler);
    return () => {
      this.subscribedEvents.delete(event);
      unsub();
    };
  }

  /** 从已注册事件名计算 eventMasks（hex 编码，供 Identify 携带）。 */
  private computeEventMasks(): string | undefined {
    if (this.subscribedEvents.size === 0) return undefined;
    return computeEventMasks([...this.subscribedEvents]);
  }

  /**
   * 重发当前事件订阅意图（eventMasks）给对端。
   *
   * Phase 1 未实现 REIDENTIFY（spec draft），无法在握手后动态更新订阅；调用始终抛
   * NotImplemented。connect 后新增的订阅只能等下次重连握手生效。
   */
  updateSubscriptions(): void {
    throw new AxtpError(
      ErrorCode.NotImplemented,
      "updateSubscriptions requires REIDENTIFY (spec draft, not supported in Phase 1)"
    );
  }

  // ===== Stream =====

  openStream(
    method: string,
    params: unknown,
    options?: CallOptions
  ): Promise<{ streamId: number; response: unknown; stream: Stream }> {
    this.requireUsable();
    const session = this.session;
    if (session === undefined) throw new AxtpError(ErrorCode.InvalidState, "client not ready");
    return session.openStream(method, params, options);
  }

  onStream(
    method: string,
    handler: (ctx: CallContext, params: unknown) => unknown | Promise<unknown>
  ): () => void {
    this.requireConnected();
    const session = this.session;
    if (session === undefined) throw new AxtpError(ErrorCode.InvalidState, "client not connected");
    return session.onStream(method, handler);
  }

  /** 主动关闭，不再重连。中断进行中的 connect()（connectAborted 使其静默退出）。 */
  async close(): Promise<void> {
    this.connectAborted = true;
    const session = this.session;
    this.setState("closed");
    session?.close();
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
