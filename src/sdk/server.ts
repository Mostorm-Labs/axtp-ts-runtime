// AxtpServer：多 client 管理 + 广播/单播。
// 每 client 一个 AxtpSession（内含 Connection），Session 是上层一等对象，Connection 不直接暴露。
// handle/on 全局生效：注册到 HandlerRegistry，所有 session 委托查询（不复制）。
// call(sessionId, ...) 单播；emit(...) 广播（仅 APP_READY + eventMasks 命中）。
// onConnect 暴露 Session。

import type { ConnectionOptions } from "../protocol/connection.js";
import { Connection } from "../protocol/connection.js";
import type {
  EventName,
  EventPayload,
  MethodName,
  MethodRequest,
  MethodResponse
} from "../protocol/generated/registry.js";
import { HandlerRegistry } from "../session/handlerRegistry.js";
import type {
  EventHandler,
  MethodHandler,
  UntypedEventHandler,
  UntypedMethodHandler
} from "../session/session.js";
import { AxtpSession, type CallOptions } from "../session/session.js";
import type { IServerTransport, ITransport, LogicalRole, PhysicalRole } from "../transport/transport.js";
import { EventStream } from "../types/events.js";

let nextSessionId = 1;

export interface ServerOptions extends ConnectionOptions {
  /** call 默认超时。 */
  defaultTimeoutMs?: number;
  /** 连接握手默认超时。 */
  handshakeTimeoutMs?: number;
  /** Logical 角色：默认 "client"（收 Hello、发 Identify，Cloud Reverse 主场景：接受连接方=能力消费方）。
   *  经典场景（接受连接方=能力提供方）设为 "server"。 */
  logicalRole?: LogicalRole;
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

  /** 开始监听，接受多连接。 */
  async listen(): Promise<void> {
    this.transport.onConnection.subscribe((t) => this.adoptConnection(t));
    await this.transport.listen();
  }

  /** 新连接到达：建 Connection + Session，委托全局 HandlerRegistry。 */
  private adoptConnection(t: ITransport): void {
    const physicalRole: PhysicalRole = "server"; // server 固定接受传输连接
    const logicalRole: LogicalRole = this.options.logicalRole ?? "client"; // 默认 Logical Client（Cloud Reverse）
    const conn = new Connection(physicalRole, t, this.options);
    const session = new AxtpSession(logicalRole, conn, {
      defaultTimeoutMs: this.options.defaultTimeoutMs ?? 10000,
      globalHandlers: this.handlers // 委托全局 handler
    });
    const id = nextSessionId++;
    (session as { __id?: number }).__id = id;
    this.sessions.set(id, session);
    session.onClose.subscribe(() => this.sessions.delete(id));
    this.onConnectStream.emit(session);
  }

  // ===== 多 client 操作 =====

  /** 新连接事件（暴露 Session）。 */
  get onConnect(): EventStream<AxtpSession> {
    return this.onConnectStream;
  }

  get onClose(): EventStream<void> {
    return this.onCloseStream;
  }

  /** 单播：指定 session 调用。 */
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
    if (session === undefined) return Promise.reject(new Error(`session ${sessionId} not found`));
    return session.call(method as never, params as never, options);
  }

  /** 广播事件给所有 APP_READY + eventMasks 命中的 session（默认尊重订阅）。 */
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
    await Promise.all(targets.map((s) => s.emit(event as never, payload as never)));
  }

  /** 全局 handle：注册到 HandlerRegistry，所有 session 委托查询。 */
  handle<K extends MethodName>(method: K, handler: MethodHandler<K>): () => void;
  handle(method: string, handler: UntypedMethodHandler): () => void;
  handle(method: string, handler: UntypedMethodHandler): () => void {
    return this.handlers.setMethod(method, handler);
  }

  /** 全局 on：聚合所有 session 的事件。 */
  on<K extends EventName>(event: K, handler: EventHandler<K>): () => void;
  on(event: string, handler: UntypedEventHandler): () => void;
  on(event: string, handler: UntypedEventHandler): () => void {
    return this.handlers.addEventListener(event, handler);
  }

  /** 获取全局 HandlerRegistry（供 session 构造时注入委托）。 */
  getHandlerRegistry(): HandlerRegistry {
    return this.handlers;
  }

  getSessions(): AxtpSession[] {
    return [...this.sessions.values()];
  }

  getSession(id: number): AxtpSession | undefined {
    return this.sessions.get(id);
  }

  /** 关闭 server：断开所有 session。 */
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
