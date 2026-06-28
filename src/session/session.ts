// AxtpSession：RPC Session 门面（会话语义，暴露给上层）。
// 组合子组件（HandshakeOrchestrator/RpcExchange/StreamManager/HandlerRouter），
// 创建+持有 Connection，订阅其事件。
//
// 单一职责：Session 只做"组合编排 + 生命周期 + 会话重建"。
// - HandshakeOrchestrator：握手状态机
// - RpcExchange：RPC 收发
// - StreamManager：STREAM 管理
// - HandlerRouter：handler 表 + 全局委托
//
// 重连会话重建：Connection 管传输重连（onReconnect），Session 监听后重建握手（HandshakeOrchestrator.reset）。
// handler 表在 Session 里，重连不换 Session 实例，表自然保留——无需快照迁移。

import {
  Connection,
  type ConnectionOptions,
  type TransportFactory
} from "../protocol/connection.js";
import { RpcOp } from "../protocol/generated/axtp_ids_generated.js";
import type {
  EventName,
  EventPayload,
  MethodName,
  MethodRequest,
  MethodResponse
} from "../protocol/generated/registry.js";
import type { RpcPayload, StreamPayload } from "../protocol/model.js";
import { rpcPayload } from "../protocol/model.js";
import type { Stream } from "../sdk/stream.js";
import type { ITransport, LogicalRole, PhysicalRole } from "../transport/transport.js";
import { AxtpError, ErrorCode } from "../types/error.js";
import { EventStream } from "../types/events.js";
import { registry } from "../types/registry.js";
import {
  HandlerRouter,
  type GlobalHandlerSource,
  type UntypedEventHandler,
  type UntypedMethodHandler
} from "./handlerRouter.js";
import { HandshakeOrchestrator, type SessionIO } from "./handshakeOrchestrator.js";
import { RpcExchange } from "./rpcExchange.js";
import { StreamManager } from "./streamManager.js";

// 重新导出类型（兼容旧引用，实际定义在 handlerRouter）
export type { GlobalHandlerSource, UntypedEventHandler, UntypedMethodHandler };

/** call 选项。 */
export interface CallOptions {
  timeoutMs?: number;
}

/** handler 上下文。 */
export interface CallContext {
  readonly requestId: number;
  readonly sid: string;
  reply: <K extends EventName>(event: K, payload: EventPayload<K>) => Promise<void>;
}

export type MethodHandler<K extends MethodName> = (
  ctx: CallContext,
  params: MethodRequest<K>
) => MethodResponse<K> | Promise<MethodResponse<K>>;

export type EventHandler<K extends EventName> = (payload: EventPayload<K>) => void;

export interface SessionOptions extends ConnectionOptions {
  /** Physical 角色（client 发起连接 / server 接受连接）。默认由 transport 类型推导。 */
  physicalRole?: PhysicalRole;
  /** Logical 角色：默认 "server"（Cloud Reverse 主场景：发起连接方=能力提供方）。 */
  logicalRole?: LogicalRole;
  /** call 默认超时。 */
  defaultTimeoutMs?: number;
  /** server 端：全局 handler registry 委托。 */
  globalHandlers?: GlobalHandlerSource;
  /** 传输重连工厂（client 场景 = () => clientTransport.connect()）。 */
  transportFactory?: TransportFactory;
  /** Handshake 本地种子。 */
  handshakeSeed?: number;
}

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

  constructor(transport: ITransport, options: SessionOptions = {}) {
    this.options = options;
    this.logicalRole = options.logicalRole ?? "server";
    this.physicalRole = options.physicalRole ?? "client";
    this.defaultTimeoutMs = options.defaultTimeoutMs ?? 10000;

    // 创建 Connection（Session 持有，SDK 不知 Connection）
    this.conn = new Connection(this.physicalRole, transport, options, options.transportFactory);

    // SessionIO：子组件通过此发送（转发到 Connection）
    this.io = {
      sendRpc: (p) => this.conn.sendRpc(p),
      sendStream: (p) => this.conn.sendStream(p)
    };

    // 子组件
    this.router = new HandlerRouter(options.globalHandlers);
    this.handshakeOrch = new HandshakeOrchestrator(
      this.logicalRole,
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

  get state(): string {
    return this.handshakeOrch.state;
  }

  close(): void {
    if (this.closed) return;
    this.rpc.rejectAll(new AxtpError(ErrorCode.TransportDisconnected, "session closed"));
    this.streamMgr.abortAll("session closed");
    this.conn.close();
    this.handleClose("local close", false);
  }

  // ===== typed 四件套（重载，转发子组件）=====

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

  removeHandler(method: string, handler: UntypedMethodHandler): void {
    this.router.removeMethod(method, handler);
  }

  emit<K extends EventName>(event: K, payload: EventPayload<K>): Promise<void>;
  emit(event: string, payload: unknown): Promise<void>;
  async emit(event: string, payload: unknown): Promise<void> {
    this.requireReady();
    const eventId = registry.eventId(event as EventName) ?? 0;
    const rpc = rpcPayload({
      op: RpcOp.Event,
      methodOrEventId: eventId,
      jsonSid: this.sid,
      body: new TextEncoder().encode(JSON.stringify(payload ?? {})),
      meta: { jsonMethodOrEventName: event }
    });
    this.conn.sendRpc(rpc);
  }

  on<K extends EventName>(event: K, handler: EventHandler<K>): () => void;
  on(event: string, handler: UntypedEventHandler): () => void;
  on(event: string, handler: UntypedEventHandler): () => void {
    return this.router.addEventListener(event, handler);
  }

  // ===== STREAM =====

  async openStream<K extends MethodName>(
    method: K,
    params: MethodRequest<K>,
    options?: CallOptions
  ): Promise<{ streamId: number; response: MethodResponse<K>; stream: Stream }> {
    this.requireReady();
    const { streamId, response, stream } = await this.streamMgr.openStream(
      (m, p) => this.rpc.call(m, p, options?.timeoutMs ?? this.defaultTimeoutMs),
      method,
      params
    );
    return { streamId, response: response as MethodResponse<K>, stream };
  }

  onStream<K extends MethodName>(
    method: K,
    handler: (
      ctx: CallContext,
      params: MethodRequest<K>
    ) => MethodResponse<K> | Promise<MethodResponse<K>>
  ): () => void {
    return this.handle(method, ((ctx: unknown, params: unknown) =>
      this.streamMgr.wrapStreamHandler(
        async (p) => handler(ctx as CallContext, p as MethodRequest<K>),
        () => {} // Stream 创建回调（handler 在返回 result 后才建 Stream，无法传给 handler）
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
        this.dispatchEvent(payload);
        break;
    }
  }

  private dispatchEvent(payload: RpcPayload): void {
    const eventName = payload.meta.jsonMethodOrEventName ?? "";
    const handlers = this.router.getEventHandlers(eventName);
    if (handlers.size === 0) return;
    let data: unknown;
    try {
      data = payload.body.length === 0 ? {} : JSON.parse(new TextDecoder().decode(payload.body));
    } catch {
      return;
    }
    for (const handler of handlers) {
      try {
        handler(data);
      } catch {
        // 单个 handler 抛错不影响其它
      }
    }
  }

  private onLinkReady(): void {
    this.handshakeOrch.onLinkReady();
  }

  /** Connection 传输重连成功后：重建会话（重置握手，重新走 Hello/Identify）。 */
  private handleReconnect(_attempt: number): void {
    // 重置握手状态，等 onLinkReady 重新触发握手流程
    this.ready = false;
    this.handshakeOrch.reset();
    this.onReconnectStream.emit({ attempt: _attempt, totalDowntimeMs: 0 });
    // onLinkReady 会由 Connection 的 fireLinkReady 重新触发 → handshakeOrch.onLinkReady → 发 Hello 或等 Hello
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
