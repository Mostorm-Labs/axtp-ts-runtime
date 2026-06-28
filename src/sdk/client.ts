// AxtpClient：单 Session 封装（SDK 层，不知 Connection）。
// connect() 建立 transport + Session。自动重连由 Session+Connection 协作（SDK 只转发事件）。
// handler/event 注册到 Session，重连不换 Session 实例，表自然保留——无需快照迁移。

import type { ReconnectPolicy } from "../connection/reconnect.js";
import {
  AxtpSession,
  type CallContext,
  type CallOptions,
  type SessionOptions,
  type UntypedEventHandler,
  type UntypedMethodHandler
} from "../session/session.js";
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

export interface ClientOptions extends SessionOptions {
  /** 传输重连策略（透传给 Session→Connection）。 */
  reconnect?: ReconnectPolicy;
}

export class AxtpClient {
  private session: AxtpSession | undefined;
  private connected = false;

  private readonly onConnectStream = new EventStream<void>();
  private readonly onDisconnectStream = new EventStream<{ reason: string; remote: boolean }>();
  private readonly onReconnectStream = new EventStream<{
    attempt: number;
    totalDowntimeMs: number;
  }>();
  private readonly onReconnectFailedStream = new EventStream<void>();

  constructor(
    private readonly transport: IClientTransport,
    private readonly options: ClientOptions = {}
  ) {}

  // ===== 生命周期事件 =====

  get onConnect(): EventStream<void> {
    return this.onConnectStream;
  }
  get onDisconnect(): EventStream<{ reason: string; remote: boolean }> {
    return this.onDisconnectStream;
  }
  get onReconnect(): EventStream<{ attempt: number; totalDowntimeMs: number }> {
    return this.onReconnectStream;
  }
  get onReconnectFailed(): EventStream<void> {
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
    if (this.connected) throw new Error("client already connected");
    const transport = await this.transport.connect();
    // Session 创建 Connection（SDK 不知 Connection）。transportFactory 供重连用。
    this.session = new AxtpSession(transport, {
      ...this.options,
      physicalRole: "client",
      transportFactory: () => this.transport.connect()
    });
    this.session.onClose.subscribe((info) => {
      this.connected = false;
      this.onDisconnectStream.emit(info);
    });
    this.session.onReconnect.subscribe((info) => {
      this.onReconnectStream.emit(info);
    });
    await this.session.onReady;
    this.connected = true;
    this.onConnectStream.emit(undefined);
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
  handle(
    method: string,
    handler: (ctx: CallContext, params: unknown) => unknown | Promise<unknown>
  ): () => void {
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
    return session.emit(event as never, payload as never);
  }

  on<K extends EventName>(event: K, handler: (payload: EventPayload<K>) => void): () => void;
  on(event: string, handler: UntypedEventHandler): () => void;
  on(event: string, handler: UntypedEventHandler): () => void {
    const session = this.session;
    if (session === undefined) throw new AxtpError(ErrorCode.InvalidState, "client not connected");
    return session.on(event, handler);
  }

  /** 主动关闭，不再重连。 */
  async close(): Promise<void> {
    this.session?.close();
    this.connected = false;
  }

  private requireUsable(): void {
    if (!this.connected || !this.session?.isReady) {
      throw new AxtpError(ErrorCode.InvalidState, "client not ready");
    }
  }
}
