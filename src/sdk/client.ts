// AxtpClient：单连接 SDK 门面（基于 AxtpEndpoint）。
// 持一个 router（method/event handler，跨重连复用——作为每个 Endpoint 的 globalHandlers）。
// connect()：transport.connect → Endpoint → 等握手 ready（超时保护）。reconnect 启用时首次失败也走重连。
// 显式状态机：idle → connecting → ready → reconnecting → connecting → ready / → closed。

import { AxtpEndpoint } from "../endpoint/endpoint.js";
import { HandlerRouter } from "../broker/router.js";
import type { UntypedEventHandler, UntypedMethodHandler } from "../broker/context.js";
import {
  ReconnectCoordinator,
  resolvePolicy,
  type ReconnectPolicy
} from "../endpoint/reconnect.js";
import type { StreamClientTransport, StreamTransport } from "../transport/contract.js";
import type { LogicalRole } from "../transport/contract.js";
import { AxtpError, ErrorCode } from "../types/error.js";
import { EventStream } from "../types/events.js";
import type {
  EventName,
  EventPayload,
  MethodName,
  MethodRequest,
  MethodResponse
} from "../types/registry.js";
import { computeEventMasks } from "../types/registry.js";
import type { CallContext, CallOptions, Stream } from "./types.js";

export interface ClientOptions {
  logicalRole?: LogicalRole;
  defaultTimeoutMs?: number;
  handshakeTimeoutMs?: number;
  heartbeatIntervalMs?: number;
  maxFrameSize?: number;
  reconnect?: ReconnectPolicy;
}

export type ClientState = "idle" | "connecting" | "ready" | "reconnecting" | "closed";

const DEFAULT_CONNECT_TIMEOUT_MS = 30000;
const DEFAULT_TIMEOUT_MS = 10000;
const DEFAULT_MAX_FRAME_SIZE = 4096;
const DEFAULT_HEARTBEAT_MS = 1000;

export class AxtpClient {
  /** handler 路由：跨连接复用（作为每个 Endpoint 的 globalHandlers）。 */
  private readonly router = new HandlerRouter();
  private readonly subscribedEvents = new Set<string>();
  private endpoint: AxtpEndpoint | undefined;
  private state: ClientState = "idle";
  private firstReady = true;
  private coordinator: ReconnectCoordinator<StreamTransport> | undefined;
  /** connect() 等待首次 ready 的 resolver（首次 ready resolve；close/重连耗尽 reject）。 */
  private readyWait: { resolve: () => void; reject: (e: AxtpError) => void } | undefined;

  readonly onStateChange = new EventStream<ClientState>();
  readonly onConnect = new EventStream<void>();
  readonly onDisconnect = new EventStream<{ remote: boolean }>();
  readonly onReconnect = new EventStream<{ attempt: number }>();
  readonly onReconnectFailed = new EventStream<void>();
  readonly onError = new EventStream<AxtpError>();

  constructor(
    private readonly transport: StreamClientTransport,
    private readonly options: ClientOptions = {}
  ) {}

  get sid(): string {
    return this.endpoint?.sid ?? "";
  }
  get isReady(): boolean {
    return this.state === "ready";
  }
  get isClosed(): boolean {
    return this.state === "closed";
  }

  private setState(s: ClientState): void {
    if (this.state === s || this.state === "closed") return;
    this.state = s;
    this.onStateChange.emit(s);
  }

  /** 首次连接。reconnect 启用：首次失败也走重连（无硬超时，持续到 ready 或耗尽）。 */
  async connect(timeoutMs: number = DEFAULT_CONNECT_TIMEOUT_MS): Promise<void> {
    if (this.state !== "idle")
      throw new AxtpError(ErrorCode.InvalidState, `cannot connect from state ${this.state}`);
    this.firstReady = true;
    this.setState("connecting");

    const policy = resolvePolicy(this.options.reconnect);
    if (policy.enabled) {
      this.coordinator = new ReconnectCoordinator<StreamTransport>(
        policy,
        () => this.transport.connect(),
        (t) => this.spawnEndpoint(t),
        () => this.handleReconnectFailed(),
        (e) => this.onError.emit(e)
      );
    }

    void this.tryOpen(policy.enabled);
    await this.awaitReady(policy.enabled ? Number.POSITIVE_INFINITY : timeoutMs);
    this.setState("ready");
    this.onConnect.emit(undefined);
  }

  private async tryOpen(reconnectEnabled: boolean): Promise<void> {
    try {
      const t = await this.transport.connect();
      if (this.state === "closed") {
        t.close();
        return;
      }
      this.spawnEndpoint(t);
    } catch (err) {
      if (this.state === "closed") return;
      this.onError.emit(
        err instanceof AxtpError
          ? err
          : new AxtpError(ErrorCode.TransportDisconnected, "connect failed", err)
      );
      if (reconnectEnabled && this.coordinator !== undefined) {
        this.setState("reconnecting");
        this.coordinator.start();
      } else {
        this.failReady(new AxtpError(ErrorCode.TransportDisconnected, "connect failed", err));
      }
    }
  }

  /** 建立/重建一个 Endpoint（首次 + 每次重连）。handler 经 router(globalHandlers) 跨连接复用。 */
  private spawnEndpoint(t: StreamTransport): void {
    if (this.state === "closed") {
      t.close();
      return;
    }
    const ep = new AxtpEndpoint({
      transport: t,
      physicalRole: "client",
      logicalRole: this.options.logicalRole ?? "server",
      maxFrameSize: this.options.maxFrameSize ?? DEFAULT_MAX_FRAME_SIZE,
      heartbeatIntervalMs: this.options.heartbeatIntervalMs ?? DEFAULT_HEARTBEAT_MS,
      defaultTimeoutMs: this.options.defaultTimeoutMs ?? DEFAULT_TIMEOUT_MS,
      globalHandlers: this.router,
      eventMasks: this.computeEventMasks()
    });
    this.endpoint = ep;
    if (this.state !== "ready") this.setState("connecting");
    ep.onReady.subscribe(() => this.onEndpointReady());
    ep.onClose.subscribe(({ remote }) => this.onEndpointClose(remote));
    ep.onError.subscribe((e) => this.onError.emit(e));
    ep.start();
  }

  private onEndpointReady(): void {
    this.coordinator?.onSuccess();
    if (this.firstReady) {
      this.firstReady = false;
      this.resolveReady();
    } else {
      this.setState("ready");
      this.onReconnect.emit({ attempt: this.coordinator?.attemptCount ?? 0 });
    }
  }

  private onEndpointClose(remote: boolean): void {
    if (this.state === "closed") return;
    this.endpoint = undefined;
    if (this.firstReady) {
      // 首次 ready 前断开
      if (this.coordinator !== undefined) {
        this.setState("reconnecting");
        this.onDisconnect.emit({ remote });
        this.coordinator.start();
      } else {
        this.failReady(
          new AxtpError(
            ErrorCode.TransportDisconnected,
            `closed before ready${remote ? " by peer" : ""}`
          )
        );
      }
    } else {
      this.onDisconnect.emit({ remote });
      if (this.coordinator !== undefined) {
        this.setState("reconnecting");
        this.coordinator.start();
      } else {
        this.setState("closed");
      }
    }
  }

  private handleReconnectFailed(): void {
    this.failReady(new AxtpError(ErrorCode.TransportDisconnected, "reconnect attempts exhausted"));
    this.setState("closed");
    this.onReconnectFailed.emit(undefined);
  }

  private awaitReady(timeoutMs: number): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      const rw: { resolve: () => void; reject: (e: AxtpError) => void } = {
        resolve: () => {},
        reject: () => {}
      };
      this.readyWait = rw;
      const armed = timeoutMs !== Number.POSITIVE_INFINITY;
      const timer = armed
        ? setTimeout(() => {
            if (this.readyWait === rw) {
              this.readyWait = undefined;
              rw.reject(new AxtpError(ErrorCode.Timeout, `connect timed out after ${timeoutMs}ms`));
            }
          }, timeoutMs)
        : undefined;
      const done = (fn: () => void) => {
        if (timer !== undefined) clearTimeout(timer);
        fn();
      };
      // 用一个内部 promise 桥接 rw.resolve/reject（在 resolveReady/failReady 调用时触发）
      new Promise<void>((res, rej) => {
        rw.resolve = () => done(() => res());
        rw.reject = (e) => done(() => rej(e));
      }).then(resolve, reject);
    });
  }

  private resolveReady(): void {
    if (this.readyWait !== undefined) {
      const rw = this.readyWait;
      this.readyWait = undefined;
      rw.resolve();
    }
  }

  private failReady(err: AxtpError): void {
    if (this.readyWait !== undefined) {
      const rw = this.readyWait;
      this.readyWait = undefined;
      rw.reject(err);
    }
  }

  // ===== 四件套 =====

  call<K extends MethodName>(
    method: K,
    params: MethodRequest<K>,
    options?: CallOptions
  ): Promise<MethodResponse<K>>;
  call(method: string, params: unknown, options?: CallOptions): Promise<unknown>;
  call(method: string, params: unknown, options?: CallOptions): Promise<unknown> {
    this.requireUsable();
    return this.endpoint!.call(method, params, options?.timeoutMs);
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
    return this.router.setMethod(method, handler);
  }

  emit<K extends EventName>(event: K, payload: EventPayload<K>): Promise<void>;
  emit(event: string, payload: unknown): Promise<void>;
  emit(event: string, payload: unknown): Promise<void> {
    this.requireUsable();
    this.endpoint!.emit(event, payload);
    return Promise.resolve();
  }

  on<K extends EventName>(event: K, handler: (payload: EventPayload<K>) => void): () => void;
  on(event: string, handler: UntypedEventHandler): () => void;
  on(event: string, handler: UntypedEventHandler): () => void {
    this.subscribedEvents.add(event);
    return this.router.addEventListener(event, handler);
  }

  openStream(
    method: string,
    params: unknown,
    options?: CallOptions
  ): Promise<{ streamId: number; response: unknown; stream: Stream }> {
    this.requireUsable();
    return this.endpoint!.openStream(method, params, options?.timeoutMs) as Promise<{
      streamId: number;
      response: unknown;
      stream: Stream;
    }>;
  }

  onStream(
    method: string,
    handler: (params: unknown, stream: Stream) => unknown | Promise<unknown>
  ): () => void {
    return this.endpoint!.onStream(method, handler);
  }

  /** 主动关闭，不再重连。 */
  async close(): Promise<void> {
    this.coordinator?.stop();
    this.setState("closed");
    this.endpoint?.close();
    this.failReady(new AxtpError(ErrorCode.TransportDisconnected, "closed"));
  }

  private computeEventMasks(): string | undefined {
    return this.subscribedEvents.size > 0
      ? computeEventMasks([...this.subscribedEvents])
      : undefined;
  }

  private requireUsable(): void {
    if (this.state === "closed")
      throw new AxtpError(ErrorCode.TransportDisconnected, "client closed");
    if (this.state === "reconnecting")
      throw new AxtpError(ErrorCode.TransportDisconnected, "client reconnecting");
    if (this.state !== "ready" || this.endpoint === undefined)
      throw new AxtpError(ErrorCode.InvalidState, "client not ready");
  }
}
