// AxtpServer：多 client 管理 + 广播/单播（SDK 层，不知 Connection）。
// 每 client 一个 AxtpSession（内含 Connection），Session 是上层一等对象。
// handle/on 全局生效：注册到 HandlerRegistry，所有 session 委托查询。
// call(sessionId, ...) 单播；emit(...) 广播。

import { HandlerRegistry } from "../session/handler/handlerRegistry.js";
import {
  AxtpSession,
  type CallContext,
  type CallOptions,
  type UntypedEventHandler,
  type UntypedMethodHandler
} from "../session/session.js";
import type { IServerTransport, ITransport, LogicalRole } from "../transport/transport.js";
import { AxtpError, ErrorCode } from "../types/error.js";
import { EventStream } from "../types/events.js";
import type {
  EventName,
  EventPayload,
  MethodName,
  MethodRequest,
  MethodResponse
} from "../types/registry.js";

/**
 * Server 选项（独立定义，不继承 SessionOptions）。
 */
export interface ServerOptions {
  /** Logical 角色：默认 "client"（收 Hello、发 Identify，Cloud Reverse 主场景）。 */
  logicalRole?: LogicalRole;
  /** call 默认超时 ms。 */
  defaultTimeoutMs?: number;
  /** 握手超时 ms。 */
  handshakeTimeoutMs?: number;
  /** 心跳间隔 ms（透传给每个 session 的 Connection）。 */
  heartbeatIntervalMs?: number;
  /** 心跳超时 ms。 */
  heartbeatTimeoutMs?: number;
  /** 最大帧大小。 */
  maxFrameSize?: number;
}

export class AxtpServer {
  private readonly sessions = new Map<number, AxtpSession>();
  private readonly handlers = new HandlerRegistry();
  private readonly onConnectStream = new EventStream<AxtpSession>();
  private readonly onCloseStream = new EventStream<void>();
  private closed = false;

  constructor(
    private readonly transport: IServerTransport,
    private readonly options: ServerOptions = {}
  ) {}

  async listen(): Promise<void> {
    this.transport.onConnection.subscribe((t) => this.adoptConnection(t));
    await this.transport.listen();
  }

  /** 新连接到达：建 Session（内含 Connection，SDK 不知 Connection）。 */
  private adoptConnection(t: ITransport): void {
    const logicalRole = this.options.logicalRole ?? "client";
    const session = new AxtpSession(t, {
      physicalRole: "server",
      logicalRole,
      defaultTimeoutMs: this.options.defaultTimeoutMs ?? 10000,
      handshakeTimeoutMs: this.options.handshakeTimeoutMs,
      heartbeatIntervalMs: this.options.heartbeatIntervalMs,
      heartbeatTimeoutMs: this.options.heartbeatTimeoutMs,
      maxFrameSize: this.options.maxFrameSize,
      globalHandlers: this.handlers
    });
    this.sessions.set(session.id, session);
    session.onClose.subscribe(() => this.sessions.delete(session.id));
    this.onConnectStream.emit(session);
  }

  get onConnect(): EventStream<AxtpSession> {
    return this.onConnectStream;
  }

  get onClose(): EventStream<void> {
    return this.onCloseStream;
  }

  call<K extends MethodName>(
    sessionId: number,
    method: K,
    params: MethodRequest<K>,
    options?: CallOptions
  ): Promise<MethodResponse<K>>;
  call(sessionId: number, method: string, params: unknown, options?: CallOptions): Promise<unknown>;
  call(
    sessionId: number,
    method: string,
    params: unknown,
    options?: CallOptions
  ): Promise<unknown> {
    const session = this.sessions.get(sessionId);
    if (session === undefined)
      return Promise.reject(new AxtpError(ErrorCode.NotFound, `session ${sessionId} not found`));
    return session.call(method, params, options);
  }

  async emit<K extends EventName>(event: K, payload: EventPayload<K>): Promise<void>;
  async emit<K extends EventName>(
    event: K,
    payload: EventPayload<K>,
    filter: (session: AxtpSession) => boolean
  ): Promise<void>;
  async emit(
    event: string,
    payload: unknown,
    filter?: (session: AxtpSession) => boolean
  ): Promise<void> {
    const targets = [...this.sessions.values()].filter((s) => {
      if (!s.isReady) return false;
      if (filter !== undefined && !filter(s)) return false;
      return true;
    });
    // M12：用 allSettled 避免单个 session 失败拖垮整体广播
    await Promise.allSettled(targets.map((s) => s.emit(event, payload)));
  }

  handle<K extends MethodName>(
    method: K,
    handler: (
      ctx: CallContext,
      params: MethodRequest<K>
    ) => MethodResponse<K> | Promise<MethodResponse<K>>
  ): () => void;
  handle(method: string, handler: UntypedMethodHandler): () => void;
  handle(
    method: string,
    handler: (ctx: CallContext, params: unknown) => unknown | Promise<unknown>
  ): () => void {
    return this.handlers.setMethod(method, handler as UntypedMethodHandler);
  }

  on<K extends EventName>(event: K, handler: (payload: EventPayload<K>) => void): () => void;
  on(event: string, handler: UntypedEventHandler): () => void;
  on(event: string, handler: UntypedEventHandler): () => void {
    return this.handlers.addEventListener(event, handler);
  }

  getSessions(): AxtpSession[] {
    return [...this.sessions.values()];
  }

  getSession(id: number): AxtpSession | undefined {
    return this.sessions.get(id);
  }

  async close(): Promise<void> {
    this.closed = true;
    for (const session of this.sessions.values()) session.close();
    this.sessions.clear();
    await this.transport.close();
    this.onCloseStream.emit(undefined);
    this.onConnectStream.close();
  }

  get isClosed(): boolean {
    return this.closed;
  }
}
