// AxtpClient：单 Session 封装（SDK 层，不知 Connection）。
// connect() 建立 transport + Session。自动重连由 Session+Connection 协作（SDK 只转发事件）。
// handler/event 注册到 Session，重连不换 Session 实例，表自然保留——无需快照迁移。

import type { ReconnectPolicy } from "../connection/reconnect.js";
import {
  AxtpSession,
  type CallContext,
  type CallOptions,
  type SessionCloseInfo,
  type UntypedEventHandler,
  type UntypedMethodHandler
} from "../session/session.js";
import type { IClientTransport, LogicalRole } from "../transport/transport.js";
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
 * Client 选项（独立定义，不继承 SessionOptions，避免暴露内部参数）。
 * 只包含用户该配置的参数；physicalRole/transportFactory/globalHandlers 等由 SDK 内部注入。
 */
export interface ClientOptions {
  /** Logical 角色：默认 "server"（Cloud Reverse 主场景：发起连接方=能力提供方）。 */
  logicalRole?: LogicalRole;
  /** call 默认超时 ms。 */
  defaultTimeoutMs?: number;
  /** 握手超时 ms（超时后 connect reject）。 */
  handshakeTimeoutMs?: number;
  /** 传输重连策略。 */
  reconnect?: ReconnectPolicy;
  /** 心跳间隔 ms。 */
  heartbeatIntervalMs?: number;
  /** 心跳超时 ms。 */
  heartbeatTimeoutMs?: number;
  /** 最大帧大小。 */
  maxFrameSize?: number;
}

export class AxtpClient {
  private session: AxtpSession | undefined;
  private connected = false;

  private readonly onConnectStream = new EventStream<void>();
  private readonly onDisconnectStream = new EventStream<SessionCloseInfo>();
  private readonly onReconnectStream = new EventStream<{ attempt: number }>();
  private readonly onReconnectFailedStream = new EventStream<void>();
  private readonly onErrorStream = new EventStream<AxtpError>();

  constructor(
    private readonly transport: IClientTransport,
    private readonly options: ClientOptions = {}
  ) {}

  // ===== 生命周期事件 =====

  get onConnect(): EventStream<void> {
    return this.onConnectStream;
  }
  get onDisconnect(): EventStream<SessionCloseInfo> {
    return this.onDisconnectStream;
  }
  get onReconnect(): EventStream<{ attempt: number }> {
    return this.onReconnectStream;
  }
  get onReconnectFailed(): EventStream<void> {
    return this.onReconnectFailedStream;
  }
  /** D5: 转发 session 的异步错误（如 transport 不支持心跳等） */
  get onError(): EventStream<AxtpError> {
    return this.onErrorStream;
  }

  get isReady(): boolean {
    return this.session?.isReady ?? false;
  }

  get sid(): string {
    return this.session?.sid ?? "";
  }

  /** 首次连接。 */
  async connect(): Promise<void> {
    if (this.connected) throw new AxtpError(ErrorCode.InvalidState, "client already connected");
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
    this.session.onClose.subscribe((info) => {
      this.connected = false;
      this.onDisconnectStream.emit(info);
    });
    this.session.onError.subscribe((err) => this.onErrorStream.emit(err));
    this.session.onReconnect.subscribe((info) => {
      this.onReconnectStream.emit(info);
    });
    this.session.onReconnectFailed.subscribe(() => {
      this.onReconnectFailedStream.emit(undefined);
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
    return session.call(method, params, options);
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
    return session.emit(event, payload);
  }

  on<K extends EventName>(event: K, handler: (payload: EventPayload<K>) => void): () => void;
  on(event: string, handler: UntypedEventHandler): () => void;
  on(event: string, handler: UntypedEventHandler): () => void {
    const session = this.session;
    if (session === undefined) throw new AxtpError(ErrorCode.InvalidState, "client not connected");
    return session.on(event, handler);
  }

  /** 主动关闭，不再重连。 */
  close(): void {
    this.session?.close();
    this.connected = false;
  }

  private requireUsable(): void {
    if (!this.connected || !this.session?.isReady) {
      throw new AxtpError(ErrorCode.InvalidState, "client not ready");
    }
  }
}
