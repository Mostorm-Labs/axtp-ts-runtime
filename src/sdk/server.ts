// AxtpServer：多 client 管理 + 广播/单播（SDK 层，不知 Connection）。
// 每 client 一个 AxtpSession（内含 Connection），Session 是上层一等对象。
// handle/on 全局生效：注册到 HandlerRouter，所有 session 委托查询。
// call(localId, ...) 单播；emit(...) 广播。
//
// 事件语义：
// - onConnect: session 握手成功（ready 后触发）
// - onDisconnect: 单个 session 断开
// - onClose: Server 整体关闭

import { HandlerRouter } from "../session/handler/handlerRouter.js";
import {
  AxtpSession,
  type CallContext,
  type CallOptions,
  type CommonOptions,
  type UntypedEventHandler,
  type UntypedMethodHandler
} from "../session/session.js";
import type { IServerTransport, ITransport } from "../transport/contract.js";
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
 * Server 选项。当前与 CommonOptions 相同；未来增加 server 专属字段（如 maxConnections、auth）
 * 时改为 interface 形式声明。现以 type 别名表达，避免空 interface（@typescript-eslint/no-empty-object-type）。
 */
export type ServerOptions = CommonOptions;

export class AxtpServer {
  private readonly sessions = new Map<number, AxtpSession>();
  private readonly handlers = new HandlerRouter();
  readonly onConnect = new EventStream<AxtpSession>();
  readonly onDisconnect = new EventStream<AxtpSession>();
  readonly onError = new EventStream<AxtpError>();
  readonly onClose = new EventStream<void>();
  private closed = false;

  constructor(
    private readonly transport: IServerTransport,
    private readonly options: ServerOptions = {}
  ) {}

  async listen(): Promise<void> {
    this.transport.onConnection.subscribe((t) => this.adoptConnection(t));
    this.transport.onError.subscribe((err) => this.onError.emit(err));
    await this.transport.listen();
  }

  /** 新连接到达：建 Session（内含 Connection），握手成功后 onConnect 触发。 */
  private adoptConnection(t: ITransport): void {
    // server 端 transport 已由 IServerTransport accept 建立，包成一次性 factory 供 Connection 首次 attach。
    const session = new AxtpSession(() => Promise.resolve(t), {
      physicalRole: "server",
      // 默认 logical client（发 Identify）：对应 AXTP-WS-CLOUD-REVERSE（Cloud=physical server=logical client）。
      // 标准 AXTP-TCP（Device=physical server=logical server，发 Hello）需显式传 logicalRole:"server"。
      logicalRole: this.options.logicalRole ?? "client",
      defaultTimeoutMs: this.options.defaultTimeoutMs ?? 10000,
      handshakeTimeoutMs: this.options.handshakeTimeoutMs,
      heartbeatIntervalMs: this.options.heartbeatIntervalMs,
      heartbeatTimeoutMs: this.options.heartbeatTimeoutMs,
      maxFrameSize: this.options.maxFrameSize,
      globalHandlers: this.handlers
    });

    session.onClose.subscribe(() => {
      // 仅当该 session 确曾 onConnect（已在表中）才发 onDisconnect，避免对握手失败、
      // 从未 onConnect 的 session 触发“幽灵” onDisconnect。
      if (this.sessions.delete(session.localId)) {
        this.onDisconnect.emit(session);
      }
    });

    session.onError.subscribe((err) => {
      this.onError.emit(err);
    });

    // 握手成功后才注册到 sessions 表 + 触发 onConnect
    session.onReady.subscribe(() => {
      this.sessions.set(session.localId, session);
      this.onConnect.emit(session);
    });
  }

  call<K extends MethodName>(
    localId: number,
    method: K,
    params: MethodRequest<K>,
    options?: CallOptions
  ): Promise<MethodResponse<K>>;
  call(localId: number, method: string, params: unknown, options?: CallOptions): Promise<unknown>;
  call(
    localId: number,
    method: string,
    params: unknown,
    options?: CallOptions
  ): Promise<unknown> {
    const session = this.sessions.get(localId);
    if (session === undefined)
      return Promise.reject(new AxtpError(ErrorCode.NotFound, `session ${localId} not found`));
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

  /** 按运行时 localId（自增整数）查询 session。 */
  getSession(localId: number): AxtpSession | undefined {
    return this.sessions.get(localId);
  }

  /** 按协议 sid（8 位 hex）查询 session；sid 未分配（握手前）时返回 undefined。 */
  getSessionBySid(sid: string): AxtpSession | undefined {
    for (const session of this.sessions.values()) {
      if (session.sid === sid) return session;
    }
    return undefined;
  }

  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    for (const session of this.sessions.values()) session.close();
    this.sessions.clear();
    await this.transport.close();
    this.onClose.emit(undefined);
    this.onConnect.close();
    this.onDisconnect.close();
    this.onError.close();
  }

  get isClosed(): boolean {
    return this.closed;
  }
}
