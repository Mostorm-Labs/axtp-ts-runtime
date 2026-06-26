// AxtpSession：RPC Session（会话语义，暴露给上层）。
// 持有 Handshake（Hello/Identify/Identified 状态机）+ RpcDispatcher（pending call）+ StreamRegistry。
// 使用 Connection 的收发能力（onPayload 上交解码后的 RpcPayload，sendRpc 发出）。
// handler 表按 name 索引——规避 method id 与 event id 共享同一数字空间。
//
// typed 重载：内置 spec 方法（K extends MethodName）强类型；vendor/自定义（string）同 JSON 便利但 unknown。
// 会话状态机规范 4 态：LINK_CONNECTED -> FRAMING_READY -> APP_READY -> CLOSING。
// 未 ready 发请求 -> CONTROL_OPEN_REQUIRED（conformance 对齐）。

import type { Bytes } from "../io/bytes.js";
import type { Connection } from "../protocol/connection.js";
import { Handshake } from "../protocol/engine/handshake.js";
import { RpcDispatcher } from "../protocol/engine/rpcDispatcher.js";
import { RpcOp } from "../protocol/generated/axtp_ids_generated.js";
import type {
  EventName,
  EventPayload,
  MethodName,
  MethodRequest,
  MethodResponse
} from "../protocol/generated/registry.js";
import type { RpcPayload } from "../protocol/model.js";
import { rpcPayload } from "../protocol/model.js";
import { Stream } from "../sdk/stream.js";
import type { LogicalRole } from "../transport/transport.js";
import { AxtpError, ErrorCode } from "../types/error.js";
import { EventStream } from "../types/events.js";
import { registry } from "../types/registry.js";
import { StreamRegistry, type StreamContext } from "./streamRegistry.js";

/** call 选项。 */
export interface CallOptions {
  /** 超时 ms（默认 10000）。 */
  timeoutMs?: number;
}

/** handler 上下文。 */
export interface CallContext {
  readonly requestId: number;
  readonly sid: string;
  /** 便捷：向该对端推事件。 */
  reply: <K extends EventName>(event: K, payload: EventPayload<K>) => Promise<void>;
}

/** 方法 handler（typed）。 */
export type MethodHandler<K extends MethodName> = (
  ctx: CallContext,
  params: MethodRequest<K>
) => MethodResponse<K> | Promise<MethodResponse<K>>;

/** 事件 handler（typed）。 */
export type EventHandler<K extends EventName> = (payload: EventPayload<K>) => void;

/** vendor/untyped 方法 handler。 */
export type UntypedMethodHandler = (
  ctx: CallContext,
  params: unknown
) => unknown | Promise<unknown>;

/** vendor/untyped 事件 handler。 */
export type UntypedEventHandler = (payload: unknown) => void;

export interface SessionOptions {
  /** client 在 Identify 携带的 eventMasks（订阅意图）。 */
  eventMasks?: string;
  /** Handshake 本地种子。 */
  handshakeSeed?: number;
  /** call 默认超时。 */
  defaultTimeoutMs?: number;
  /** server 端：全局 handler registry 委托（dispatchRequest/dispatchEvent miss 时查此）。 */
  globalHandlers?: {
    getMethod: (name: string) => UntypedMethodHandler | undefined;
    getEventListeners: (name: string) => Set<UntypedEventHandler> | undefined;
  };
}

export class AxtpSession {
  private readonly conn: Connection;
  readonly handshake: Handshake;
  readonly dispatcher = new RpcDispatcher();
  readonly streams = new StreamRegistry();

  /** 按 name 索引的 method handler 表。 */
  private readonly methodHandlers = new Map<string, UntypedMethodHandler>();
  /** 按 name 索引的 event handler 表。typed/vendor 共享同一 entry。 */
  private readonly eventHandlers = new Map<string, Set<UntypedEventHandler>>();
  /** server 端全局 handler 委托（dispatchRequest 本地 miss 时查此）。 */
  private readonly globalHandlers?: {
    getMethod: (name: string) => UntypedMethodHandler | undefined;
    getEventListeners: (name: string) => Set<UntypedEventHandler> | undefined;
  };

  private readonly onReadyStream = new EventStream<void>();
  private readonly onCloseStream = new EventStream<{ reason: string; remote: boolean }>();

  private ready = false;
  private closed = false;
  private readonly defaultTimeoutMs: number;

  constructor(
    private readonly logicalRole: LogicalRole,
    conn: Connection,
    options: SessionOptions = {}
  ) {
    this.conn = conn;
    this.defaultTimeoutMs = options.defaultTimeoutMs ?? 10000;
    this.handshake = new Handshake(logicalRole, options.handshakeSeed);
    this.globalHandlers = options.globalHandlers;
    if (options.eventMasks) this.handshake.setEventMasks(options.eventMasks);

    // 订阅 Connection 事件（回调已就绪后 start）
    this.conn.onPayload.subscribe((p) => this.ingest(p));
    this.conn.onStream.subscribe((s) => this.streams.onData(s));
    this.conn.onLinkReady.subscribe(() => this.onLinkReady());
    this.conn.onClose.subscribe((r) => this.handleClose(r.reason, r.remote));

    // 启动 Connection 接收
    this.conn.start();
  }

  // ===== 生命周期 =====

  /** APP_READY 后 resolve。 */
  readonly onReady: Promise<void> = new Promise((resolve) => {
    this.onReadyStream.subscribe(() => resolve());
  });

  get onReadyEvent(): EventStream<void> {
    return this.onReadyStream;
  }

  get onClose(): EventStream<{ reason: string; remote: boolean }> {
    return this.onCloseStream;
  }

  get isReady(): boolean {
    return this.ready;
  }

  get isClosed(): boolean {
    return this.closed;
  }

  get sid(): string {
    return this.handshake.sid;
  }

  get state(): string {
    return this.handshake.state;
  }

  // ===== STREAM（framed-binary 双向数据流；WS 不支持）=====

  /**
   * 发起建流：调用 openStream 类 RPC（如 video.openStream），server 返回 streamId，
   * 本地建 send 方 Stream。返回的 Stream 可 send()/onChunk()。
   * streamId 由 server 分配（spec: VideoOpenStreamResult.streamId）。
   */
  async openStream<K extends MethodName>(
    method: K,
    params: MethodRequest<K>,
    options?: CallOptions
  ): Promise<{ streamId: number; response: MethodResponse<K>; stream: Stream }> {
    this.requireReady();
    const result = await this.call(method, params, options);
    const streamId = (result as { streamId?: number }).streamId;
    if (typeof streamId !== "number" || streamId === 0) {
      throw new AxtpError(ErrorCode.StreamIdInvalid, "openStream response missing streamId");
    }
    // 本地 send 方：用 server 分配的 streamId 建 context（adopt 语义）
    const sendCtx = this.streams.adopt(streamId);
    sendCtx.direction = "send";
    const stream = this.makeStream(sendCtx);
    return { streamId, response: result, stream };
  }

  /**
   * 注册建流 handler（server 端）：当 client 调 openStream RPC 时触发。
   * handler 内用返回的 response.streamId，并获得 Stream 对象用于 send/onChunk。
   * server 端 Stream 是 receive 方（接收 client 的数据），也可 send（双向）。
   */
  onStream<K extends MethodName>(
    method: K,
    handler: (
      ctx: CallContext,
      params: MethodRequest<K>,
      stream: Stream
    ) => MethodResponse<K> | Promise<MethodResponse<K>>
  ): () => void {
    return this.handle(method, async (callCtx, params) => {
      const result = await handler(callCtx, params, undefined as never);
      const streamId = (result as { streamId?: number }).streamId;
      if (typeof streamId !== "number" || streamId === 0) {
        throw new AxtpError(ErrorCode.StreamIdInvalid, "onStream handler must return streamId");
      }
      // server 用自己分配的 streamId 建 receive context
      const recvCtx = this.streams.adopt(streamId);
      recvCtx.direction = "receive";
      this.makeStream(recvCtx);
      return result;
    });
  }

  /** 构造 Stream 对象，绑定发送/关闭回调。 */
  private makeStream(ctx: StreamContext): Stream {
    return new Stream(
      ctx,
      (streamId, data, seqId) => this.sendStreamData(streamId, data, seqId),
      (streamId) => this.closeStream(streamId)
    );
  }

  /** 发送 STREAM 数据帧（framed only）。 */
  private sendStreamData(streamId: number, data: Bytes, seqId: number): void {
    this.conn.sendStream({ streamId, seqId, cursor: 0n, data });
  }

  /** 关闭单个流（本地状态清理）。完整关流走 video.closeStream RPC。 */
  private closeStream(streamId: number): void {
    this.streams.close(streamId, "local close");
  }

  close(): void {
    if (this.closed) return;
    this.dispatcher.rejectAll(new AxtpError(ErrorCode.TransportDisconnected, "session closed"));
    this.streams.abortAll("session closed");
    this.conn.close();
    this.handleClose("local close", false);
  }

  // ===== typed 四件套（重载）=====

  /** call：内置 typed / vendor untyped。 */
  call<K extends MethodName>(
    method: K,
    params: MethodRequest<K>,
    options?: CallOptions
  ): Promise<MethodResponse<K>>;
  call(method: string, params: unknown, options?: CallOptions): Promise<unknown>;
  call(method: string, params: unknown, options?: CallOptions): Promise<unknown> {
    return this.doCall(method, params, options?.timeoutMs ?? this.defaultTimeoutMs);
  }

  /** handle：内置 typed / vendor untyped。返回 unsubscribe。 */
  handle<K extends MethodName>(method: K, handler: MethodHandler<K>): () => void;
  handle(method: string, handler: UntypedMethodHandler): () => void;
  handle(method: string, handler: UntypedMethodHandler): () => void {
    this.methodHandlers.set(method, handler);
    return () => {
      if (this.methodHandlers.get(method) === handler) this.methodHandlers.delete(method);
    };
  }

  /** 移除 handler（供 unsubscribe 闭包使用，避免捕获旧 session）。 */
  removeHandler(method: string, handler: UntypedMethodHandler): void {
    if (this.methodHandlers.get(method) === handler) this.methodHandlers.delete(method);
  }

  /** emit：内置 typed / vendor untyped。 */
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

  /** on：内置 typed / vendor untyped。返回 unsubscribe。 */
  on<K extends EventName>(event: K, handler: EventHandler<K>): () => void;
  on(event: string, handler: UntypedEventHandler): () => void;
  on(event: string, handler: UntypedEventHandler): () => void {
    const set = this.eventHandlers.get(event) ?? new Set<UntypedEventHandler>();
    if (!this.eventHandlers.has(event)) this.eventHandlers.set(event, set);
    set.add(handler);
    return () => set.delete(handler);
  }

  // ===== call 实现 =====

  private doCall(method: string, params: unknown, timeoutMs: number): Promise<unknown> {
    this.requireReady();
    const methodId = registry.methodId(method as MethodName) ?? 0;
    const { requestId, promise } = this.dispatcher.request((id) => {
      const rpc = rpcPayload({
        op: RpcOp.Request,
        requestId: id,
        methodOrEventId: methodId,
        jsonSid: this.sid,
        body: new TextEncoder().encode(JSON.stringify(params ?? {})),
        meta: { jsonMethodOrEventName: method }
      });
      this.conn.sendRpc(rpc);
    }, timeoutMs);

    return promise.then((payload) => {
      if (payload.statusCode !== ErrorCode.Success) {
        throw new AxtpError(
          payload.statusCode as ErrorCode,
          `call ${method} failed`,
          undefined,
          requestId
        );
      }
      if (payload.body.length === 0) return undefined;
      try {
        return JSON.parse(new TextDecoder().decode(payload.body));
      } catch {
        throw new AxtpError(
          ErrorCode.RpcPayloadInvalid,
          `call ${method} response parse failed`,
          undefined,
          requestId
        );
      }
    });
  }

  // ===== 入站总入口 =====

  private ingest(payload: RpcPayload): void {
    if (this.closed) return;

    // 会话握手（Handshake 消费，归 Session）
    if (!this.ready && this.isHandshakeOp(payload.op)) {
      const result = this.handshake.handle(payload);
      if (result.outbound) this.conn.sendRpc(result.outbound);
      if (result.becameReady) {
        this.ready = true;
        this.onReadyStream.emit(undefined);
      }
      return;
    }

    // 未 ready 的业务请求 -> CONTROL_OPEN_REQUIRED（conformance 对齐）
    if (!this.ready) {
      if (payload.op === RpcOp.Request) {
        this.conn.sendRpc(
          rpcPayload({
            op: RpcOp.RequestResponse,
            requestId: payload.requestId,
            statusCode: ErrorCode.ControlOpenRequired,
            jsonSid: this.sid
          })
        );
      }
      return;
    }

    // APP_READY 业务分发
    switch (payload.op) {
      case RpcOp.Request:
        this.dispatchRequest(payload);
        break;
      case RpcOp.RequestResponse:
        this.dispatcher.resolve(payload);
        break;
      case RpcOp.Event:
        this.dispatchEvent(payload);
        break;
    }
  }

  /** 链路 ready 后：Logical Server 发 Hello（spec: Hello 永远由 Logical Server 发，与 Physical 角色正交）。 */
  private onLinkReady(): void {
    this.handshake.onLinkReady();
    if (this.logicalRole === "server") {
      // Logical Server 发 Hello
      this.conn.sendRpc(this.handshake.startHello());
    }
    // Logical Client 等待 Hello 到达，ingest 里处理
  }

  private dispatchRequest(payload: RpcPayload): void {
    const methodName = payload.meta.jsonMethodOrEventName ?? "";
    // 先查本地 handler，miss 委托全局 registry（server 模式）
    const handler =
      this.methodHandlers.get(methodName) ?? this.globalHandlers?.getMethod(methodName);
    const ctx: CallContext = {
      requestId: payload.requestId,
      sid: this.sid,
      reply: (event, eventPayload) => this.emit(event, eventPayload)
    };

    if (handler === undefined) {
      // 未注册 -> RPC_METHOD_NOT_FOUND
      this.conn.sendRpc(
        rpcPayload({
          op: RpcOp.RequestResponse,
          requestId: payload.requestId,
          statusCode: ErrorCode.RpcMethodNotFound,
          jsonSid: this.sid
        })
      );
      return;
    }

    // 解析 params
    let params: unknown;
    try {
      params = payload.body.length === 0 ? {} : JSON.parse(new TextDecoder().decode(payload.body));
    } catch {
      this.conn.sendRpc(
        rpcPayload({
          op: RpcOp.RequestResponse,
          requestId: payload.requestId,
          statusCode: ErrorCode.RpcPayloadInvalid,
          jsonSid: this.sid
        })
      );
      return;
    }

    // 执行 handler（异步）
    Promise.resolve()
      .then(() => handler(ctx, params))
      .then(
        (result) => {
          this.conn.sendRpc(
            rpcPayload({
              op: RpcOp.RequestResponse,
              requestId: payload.requestId,
              statusCode: ErrorCode.Success,
              jsonSid: this.sid,
              body: new TextEncoder().encode(JSON.stringify(result ?? {}))
            })
          );
        },
        (err) => {
          const code = err instanceof AxtpError ? err.code : ErrorCode.RpcExecutionFailed;
          this.conn.sendRpc(
            rpcPayload({
              op: RpcOp.RequestResponse,
              requestId: payload.requestId,
              statusCode: code,
              jsonSid: this.sid
            })
          );
        }
      );
  }

  private dispatchEvent(payload: RpcPayload): void {
    const eventName = payload.meta.jsonMethodOrEventName ?? "";
    // 先查本地 event handler，再委托全局 registry（server 模式，server.on 注册到此）
    const localSet = this.eventHandlers.get(eventName);
    const globalSet = this.globalHandlers?.getEventListeners(eventName);
    const handlers = new Set<UntypedEventHandler>();
    if (localSet !== undefined) for (const h of localSet) handlers.add(h);
    if (globalSet !== undefined) for (const h of globalSet) handlers.add(h);
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

  private isHandshakeOp(op: RpcOp): boolean {
    return op === RpcOp.Hello || op === RpcOp.Identify || op === RpcOp.Identified;
  }

  private requireReady(): void {
    if (this.closed) throw new AxtpError(ErrorCode.TransportDisconnected, "session closed");
    if (!this.ready) throw new AxtpError(ErrorCode.InvalidState, "session not ready");
  }

  private handleClose(reason: string, remote: boolean): void {
    if (this.closed) return;
    this.closed = true;
    this.ready = false;
    this.dispatcher.rejectAll(
      new AxtpError(ErrorCode.TransportDisconnected, `connection closed: ${reason}`)
    );
    this.streams.abortAll(`connection closed: ${reason}`);
    this.onCloseStream.emit({ reason, remote });
    this.onReadyStream.close();
  }
}
