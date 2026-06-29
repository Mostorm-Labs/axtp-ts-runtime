// AxtpServer：多 client 管理 + 广播/单播（SDK 层，不知 Connection）。
// 每 client 一个 AxtpSession（内含 Connection），Session 是上层一等对象。
// handle/on 全局生效：注册到 HandlerRegistry，所有 session 委托查询。
// call(sessionId, ...) 单播；emit(...) 广播。
//
// 事件语义：
// - onConnect: 新 session 创建（握手前，物理连接到达即触发）
// - onSessionReady: session 握手成功（ready 后触发）
// - onSessionClose: 单个 session 断开
// - onClose: Server 整体关闭

import { HandlerRegistry } from "../session/handler/handlerRegistry.js";
import {
  AxtpSession,
  type CallContext,
  type CallOptions,
  type CommonOptions,
  type UntypedEventHandler,
  type UntypedMethodHandler
} from "../session/session.js";
import type { IServerTransport, ITransport } from "../transport/transport.js";
import { AxtpError, ErrorCode } from "../types/error.js";
import { EventStream } from "../types/events.js";
import type {
  EventName,
  EventPayload,
  MethodName,
  MethodRequest,
  MethodResponse
} from "../types/registry.js";

/** Server 选项（当前与 CommonOptions 相同，预留扩展点）。 */
export type ServerOptions = CommonOptions;

export class AxtpServer {
  private readonly sessions = new Map<number, AxtpSession>();
  private readonly handlers = new HandlerRegistry();
  private readonly onConnectStream = new EventStream<AxtpSession>();
  private readonly onSessionReadyStream = new EventStream<AxtpSession>();
  private readonly onSessionCloseStream = new EventStream<AxtpSession>();
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
    const session = new AxtpSession(t, {
      physicalRole: "server",
      logicalRole: this.options.logicalRole ?? "client",
      defaultTimeoutMs: this.options.defaultTimeoutMs ?? 10000,
      handshakeTimeoutMs: this.options.handshakeTimeoutMs,
      heartbeatIntervalMs: this.options.heartbeatIntervalMs,
      heartbeatTimeoutMs: this.options.heartbeatTimeoutMs,
      maxFrameSize: this.options.maxFrameSize,
      globalHandlers: this.handlers
    });
    this.sessions.set(session.id, session);

    // session ready 后通知上层（与 onConnect 区分）
    session.onReady.subscribe(() => this.onSessionReadyStream.emit(session));

    session.onClose.subscribe(() => {
      this.sessions.delete(session.id);
      this.onSessionCloseStream.emit(session);
    });

    // onConnect：物理连接到达即触发（session 尚未 ready）
    this.onConnectStream.emit(session);
  }

  /** 新 session 创建（握手前）。 */
  get onConnect(): EventStream<AxtpSession> {
    return this.onConnectStream;
  }

  /** session 握手成功（ready 后）。 */
  get onSessionReady(): EventStream<AxtpSession> {
    return this.onSessionReadyStream;
  }

  /** 单个 client session 断开时触发。 */
  get onSessionClose(): EventStream<AxtpSession> {
    return this.onSessionCloseStream;
  }

  /** Server 整体关闭时触发。 */
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

  /** 仅返回 ready 的 session。 */
  getReadySessions(): AxtpSession[] {
    return [...this.sessions.values()].filter((s) => s.isReady);
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
    this.onSessionReadyStream.close();
  }

  get isClosed(): boolean {
    return this.closed;
  }
}
