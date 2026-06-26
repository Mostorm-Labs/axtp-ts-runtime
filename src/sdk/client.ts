// AxtpClient：单/重连 Session 封装。
// connect() 建立连接 + 握手。自动重连（指数退避+抖动），全新 session，迁移 handle/on 订阅。
// pending call/stream 在断连时 reject（不可恢复，spec 无 RESUME）。
// unsubscribe 操作 Client snapshot（不捕获具体 session，修正重连后失效）。
// attachSession 统一挂载 onClose→重连编排（首连和重连都用）。

import type { ConnectionOptions } from "../protocol/connection.js";
import { Connection } from "../protocol/connection.js";
import type {
  EventName,
  EventPayload,
  MethodName,
  MethodRequest,
  MethodResponse
} from "../protocol/generated/registry.js";
import {
  AxtpSession,
  type CallContext,
  type CallOptions,
  type UntypedEventHandler,
  type UntypedMethodHandler
} from "../session/session.js";
import type { IClientTransport, ITransport, LogicalRole, PhysicalRole } from "../transport/transport.js";
import { AxtpError, ErrorCode } from "../types/error.js";
import { EventStream } from "../types/events.js";
import { computeEventMasks } from "../types/registry.js";
import { nextDelay, resolvePolicy, type ReconnectInfo, type ReconnectPolicy } from "./reconnect.js";

export interface ClientOptions extends ConnectionOptions {
  reconnect?: ReconnectPolicy;
  /** call 默认超时。 */
  defaultTimeoutMs?: number;
  /** Logical 角色：默认 "server"（发 Hello、分配 sid，Cloud Reverse 主场景：发起连接方=能力提供方）。
   *  经典场景（客户端消费对端能力）设为 "client"。 */
  logicalRole?: LogicalRole;
}

type ClientState = "idle" | "connecting" | "connected" | "reconnecting" | "closed";

export class AxtpClient {
  private readonly transport: IClientTransport;
  private readonly options: ClientOptions;
  private readonly policy: ReturnType<typeof resolvePolicy>;

  private session: AxtpSession | undefined;
  private state: ClientState = "idle";

  // handler/event 快照（重连迁移；unsubscribe 也操作此，不绑定具体 session）
  private readonly methodSnapshot = new Map<string, UntypedMethodHandler>();
  private readonly eventSnapshot = new Map<string, Set<UntypedEventHandler>>();
  private lastEventMasks: string | undefined;

  private reconnectAttempts = 0;
  private reconnectTimer: ReturnType<typeof setTimeout> | undefined;

  private readonly onConnectStream = new EventStream<void>();
  private readonly onDisconnectStream = new EventStream<{ reason: string; remote: boolean }>();
  private readonly onReconnectStream = new EventStream<ReconnectInfo>();
  private readonly onReconnectFailedStream = new EventStream<AxtpError>();

  constructor(transport: IClientTransport, options: ClientOptions = {}) {
    this.transport = transport;
    this.options = options;
    this.policy = resolvePolicy(options.reconnect);
  }

  // ===== 生命周期 =====

  get onConnect(): EventStream<void> {
    return this.onConnectStream;
  }
  get onDisconnect(): EventStream<{ reason: string; remote: boolean }> {
    return this.onDisconnectStream;
  }
  get onReconnect(): EventStream<ReconnectInfo> {
    return this.onReconnectStream;
  }
  get onReconnectFailed(): EventStream<AxtpError> {
    return this.onReconnectFailedStream;
  }

  get isReady(): boolean {
    return this.session?.isReady ?? false;
  }

  get sid(): string {
    return this.session?.sid ?? "";
  }

  /** 首次连接。 */
  async connect(): Promise<void> {
    if (this.state !== "idle") throw new Error(`cannot connect from state ${this.state}`);
    this.state = "connecting";
    try {
      const transport = await this.transport.connect();
      await this.establishSession(transport);
    } catch (err) {
      this.state = "closed";
      throw err;
    }
  }

  /** 建立 Session（首连/重连共用）。 */
  private async establishSession(transport: ITransport): Promise<void> {
    const physicalRole: PhysicalRole = "client"; // client 固定发起传输连接
    const logicalRole: LogicalRole = this.options.logicalRole ?? "server"; // 默认 Logical Server（Cloud Reverse）
    const conn = new Connection(physicalRole, transport, this.options);
    // 计算当前订阅的 eventMasks（从 eventSnapshot 推导）
    const eventNames = [...this.eventSnapshot.keys()] as EventName[];
    this.lastEventMasks = eventNames.length > 0 ? computeEventMasks(eventNames) : undefined;

    const session = new AxtpSession(logicalRole, conn, {
      defaultTimeoutMs: this.options.defaultTimeoutMs ?? 10000,
      eventMasks: this.lastEventMasks
    });

    // 迁移 handler/event 快照到新 session
    this.migrateSnapshots(session);

    // 等 ready
    await session.onReady;

    this.attachSession(session);
    this.state = "connected";
    if (this.reconnectAttempts > 0) {
      // 重连成功
      this.onReconnectStream.emit({ attempt: this.reconnectAttempts, totalDowntimeMs: 0 });
    } else {
      this.onConnectStream.emit(undefined);
    }

    // 成功后重置退避
    if (this.policy.resetBackoffOnSuccess) {
      this.reconnectAttempts = 0;
    }
  }

  /** 统一挂载 session：onClose → 触发重连编排（首连和重连都用）。 */
  private attachSession(session: AxtpSession): void {
    this.session = session;
    session.onClose.subscribe((info) => {
      if (this.state === "closed") return; // 用户主动关闭，不重连
      this.onDisconnectStream.emit(info);
      this.handleDisconnect();
    });
  }

  /** 断连处理：触发重连（若启用）。 */
  private handleDisconnect(): void {
    if (!this.policy.enabled) {
      this.state = "closed";
      return;
    }
    this.state = "reconnecting";
    this.scheduleReconnect();
  }

  private scheduleReconnect(): void {
    if (this.reconnectAttempts >= this.policy.maxAttempts) {
      this.state = "closed";
      this.onReconnectFailedStream.emit(
        new AxtpError(ErrorCode.TransportDisconnected, "max reconnect attempts reached")
      );
      return;
    }
    const delay = nextDelay(this.policy, this.reconnectAttempts);
    this.reconnectAttempts += 1;
    this.reconnectTimer = setTimeout(() => {
      this.attemptReconnect().catch(() => {
        // 单次重连失败，继续下一次
        if (this.state !== "closed") this.scheduleReconnect();
      });
    }, delay);
  }

  private async attemptReconnect(): Promise<void> {
    try {
      const transport = await this.transport.connect();
      await this.establishSession(transport);
    } catch (err) {
      // 握手失败区分：永久错误（如协议不符）→ 停止；临时错误 → 继续
      if (err instanceof AxtpError) {
        const permanent =
          err.code === ErrorCode.ControlOpenRejected ||
          err.code === ErrorCode.ControlNegotiationFailed;
        if (permanent) {
          this.state = "closed";
          this.onReconnectFailedStream.emit(err);
          return;
        }
      }
      throw err;
    }
  }

  /** 迁移快照到新 session。 */
  private migrateSnapshots(session: AxtpSession): void {
    for (const [method, handler] of this.methodSnapshot) {
      session.handle(method, handler);
    }
    for (const [event, handlers] of this.eventSnapshot) {
      for (const handler of handlers) {
        session.on(event, handler);
      }
    }
  }

  // ===== 四件套（转发 session）=====

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
    return session.call(method as never, params as never, options);
  }

  handle<K extends MethodName>(
    method: K,
    handler: (
      ctx: CallContext,
      params: MethodRequest<K>
    ) => MethodResponse<K> | Promise<MethodResponse<K>>
  ): () => void;
  handle(method: string, handler: UntypedMethodHandler): () => void;
  handle(method: string, handler: UntypedMethodHandler): () => void {
    this.methodSnapshot.set(method, handler);
    this.session?.handle(method, handler);
    return () => {
      this.methodSnapshot.delete(method);
      this.session?.removeHandler(method, handler);
    };
  }

  emit<K extends EventName>(event: K, payload: EventPayload<K>): Promise<void>;
  emit(event: string, payload: unknown): Promise<void>;
  emit(event: string, payload: unknown): Promise<void> {
    this.requireUsable();
    const session = this.session;
    if (session === undefined) throw new AxtpError(ErrorCode.InvalidState, "client not ready");
    return session.emit(event as never, payload as never);
  }

  on<K extends EventName>(event: K, handler: (payload: EventPayload<K>) => void): () => void;
  on(event: string, handler: UntypedEventHandler): () => void;
  on(event: string, handler: UntypedEventHandler): () => void {
    const handlerSet = this.eventSnapshot.get(event) ?? new Set<UntypedEventHandler>();
    if (!this.eventSnapshot.has(event)) this.eventSnapshot.set(event, handlerSet);
    handlerSet.add(handler);
    const unsub = this.session?.on(event, handler);
    return () => {
      handlerSet.delete(handler);
      unsub?.();
    };
  }

  /** 主动关闭，不再重连。 */
  async close(): Promise<void> {
    this.state = "closed";
    if (this.reconnectTimer !== undefined) clearTimeout(this.reconnectTimer);
    this.session?.close();
  }

  private requireUsable(): void {
    if (this.state === "closed")
      throw new AxtpError(ErrorCode.TransportDisconnected, "client closed");
    if (this.state === "reconnecting")
      throw new AxtpError(ErrorCode.InvalidState, "client reconnecting");
    if (!this.session?.isReady) throw new AxtpError(ErrorCode.InvalidState, "client not ready");
  }
}
