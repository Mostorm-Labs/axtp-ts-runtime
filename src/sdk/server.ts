// AxtpServer：多连接 SDK 门面（基于 AxtpEndpoint）。
// listen → 每条接受的 StreamTransport 建一个 Endpoint（logicalRole 决定 Hello 方向）。
// handle/on 注册到共享 router，作为每个 Endpoint 的 globalHandlers（全局生效）。
// call(id) 单播；emit 广播（可 filter）。

import type { UntypedEventHandler, UntypedMethodHandler } from "../broker/context.js";
import { HandlerRouter } from "../broker/router.js";
import { AxtpEndpoint } from "../endpoint/endpoint.js";
import type { LogicalRole, StreamServerTransport, StreamTransport } from "../transport/contract.js";
import { AxtpError, ErrorCode } from "../types/error.js";
import { EventStream } from "../types/events.js";
import type {
  EventName,
  EventPayload,
  MethodName,
  MethodRequest,
  MethodResponse
} from "../types/registry.js";
import type { CallContext, CallOptions } from "./types.js";

export interface ServerOptions {
  logicalRole?: LogicalRole;
  defaultTimeoutMs?: number;
  heartbeatIntervalMs?: number;
  maxFrameSize?: number;
}

const DEFAULT_TIMEOUT_MS = 10_000;
const DEFAULT_MAX_FRAME_SIZE = 4096;
const DEFAULT_HEARTBEAT_MS = 5_000;

export class AxtpServer {
  private readonly router = new HandlerRouter();
  private readonly entries = new Map<number, AxtpEndpoint>();
  private nextId = 1;
  private closed = false;

  readonly onConnect = new EventStream<AxtpEndpoint>();
  readonly onDisconnect = new EventStream<AxtpEndpoint>();
  readonly onError = new EventStream<AxtpError>();
  readonly onClose = new EventStream<void>();

  constructor(
    private readonly transport: StreamServerTransport,
    private readonly options: ServerOptions = {}
  ) {}

  async listen(): Promise<void> {
    this.transport.onConnection.subscribe((t) => this.adopt(t));
    await this.transport.listen();
  }

  /** 新连接到达：建 Endpoint，握手成功后注册 + onConnect。 */
  private adopt(t: StreamTransport): void {
    const id = this.nextId++;
    const endpoint = new AxtpEndpoint({
      transport: t,
      physicalRole: "server",
      logicalRole: this.options.logicalRole ?? "client",
      maxFrameSize: this.options.maxFrameSize ?? DEFAULT_MAX_FRAME_SIZE,
      heartbeatIntervalMs: this.options.heartbeatIntervalMs ?? DEFAULT_HEARTBEAT_MS,
      defaultTimeoutMs: this.options.defaultTimeoutMs ?? DEFAULT_TIMEOUT_MS,
      globalHandlers: this.router
    });
    endpoint.onReady.subscribe(() => {
      if (this.closed) return;
      this.entries.set(id, endpoint);
      this.onConnect.emit(endpoint);
    });
    endpoint.onClose.subscribe(() => {
      if (this.entries.delete(id)) {
        this.onDisconnect.emit(endpoint);
      }
    });
    endpoint.onError.subscribe((e) => this.onError.emit(e));
    endpoint.start();
  }

  /** id（运行时自增整数，区别于协议 sid）。 */
  getId(endpoint: AxtpEndpoint): number | undefined {
    for (const [id, ep] of this.entries) {
      if (ep === endpoint) return id;
    }
    return undefined;
  }

  getEndpoint(id: number): AxtpEndpoint | undefined {
    return this.entries.get(id);
  }

  /** 按协议 sid 查询。 */
  getEndpointBySid(sid: string): AxtpEndpoint | undefined {
    for (const ep of this.entries.values()) {
      if (ep.sid === sid) return ep;
    }
    return undefined;
  }

  getEndpoints(): AxtpEndpoint[] {
    return [...this.entries.values()];
  }

  call<K extends MethodName>(
    id: number,
    method: K,
    params: MethodRequest<K>,
    options?: CallOptions
  ): Promise<MethodResponse<K>> {
    return this.callRaw(id, method, params, options) as Promise<MethodResponse<K>>;
  }

  /** 弱类型 call：method 为任意 string、params 为 unknown。动态/自定义方法名走这里。 */
  callRaw(id: number, method: string, params: unknown, options?: CallOptions): Promise<unknown> {
    const ep = this.entries.get(id);
    if (ep === undefined)
      return Promise.reject(new AxtpError(ErrorCode.NotFound, `endpoint ${id} not found`));
    return ep.call(method, params, options?.timeoutMs);
  }

  emit<K extends EventName>(event: K, payload: EventPayload<K>): Promise<void>;
  emit<K extends EventName>(
    event: K,
    payload: EventPayload<K>,
    filter: (endpoint: AxtpEndpoint) => boolean
  ): Promise<void>;
  emit<K extends EventName>(
    event: K,
    payload: EventPayload<K>,
    filter?: (endpoint: AxtpEndpoint) => boolean
  ): Promise<void> {
    return this.emitRaw(event, payload, filter);
  }

  /** 弱类型 emit（广播，可选 filter）。 */
  async emitRaw(
    event: string,
    payload: unknown,
    filter?: (endpoint: AxtpEndpoint) => boolean
  ): Promise<void> {
    const targets = [...this.entries.values()].filter(
      (ep) => ep.isReady && (filter === undefined || filter(ep))
    );
    await Promise.allSettled(targets.map((ep) => ep.emit(event, payload)));
  }

  handle<K extends MethodName>(
    method: K,
    handler: (
      ctx: CallContext,
      params: MethodRequest<K>
    ) => MethodResponse<K> | Promise<MethodResponse<K>>
  ): () => void {
    return this.handleRaw(method, handler as UntypedMethodHandler);
  }

  /** 弱类型 handle。 */
  handleRaw(method: string, handler: UntypedMethodHandler): () => void {
    return this.router.setMethod(method, handler);
  }

  on<K extends EventName>(event: K, handler: (payload: EventPayload<K>) => void): () => void {
    return this.onRaw(event, handler as UntypedEventHandler);
  }

  /** 弱类型 on。 */
  onRaw(event: string, handler: UntypedEventHandler): () => void {
    return this.router.addEventListener(event, handler);
  }

  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    for (const endpoint of this.entries.values()) endpoint.close();
    this.entries.clear();
    await this.transport.close();
    this.onClose.emit(undefined);
    this.onConnect.close();
    this.onDisconnect.close();
    this.onError.close();
    this.onClose.close();
  }

  get isClosed(): boolean {
    return this.closed;
  }
}
