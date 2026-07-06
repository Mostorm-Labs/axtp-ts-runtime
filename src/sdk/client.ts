// AxtpClient：单连接 SDK 门面（基于 AxtpEndpoint）。
// 持一个 router（method/event handler，跨重连复用——作为每个 Endpoint 的 globalHandlers）。
// connect()：transport.connect → Endpoint → 等握手 ready（超时保护）。reconnect 启用时首次失败也走重连。
// 显式状态机：idle → connecting → ready → reconnecting → connecting → ready / → closed。

import type { UntypedEventHandler, UntypedMethodHandler } from "../broker/context.js";
import { HandlerRouter } from "../broker/router.js";
import { diagnosticData, emitDiagnostic, type AxtpDiagnostics } from "../diagnostics.js";
import { AxtpEndpoint } from "../endpoint/endpoint.js";
import {
  ReconnectCoordinator,
  resolvePolicy,
  type ReconnectPolicy
} from "../endpoint/reconnect.js";
import type { LogicalRole, StreamClientTransport, StreamTransport } from "../transport/contract.js";
import { AxtpError, ErrorCode } from "../types/error.js";
import { EventStream } from "../types/events.js";
import type {
  EventName,
  EventPayload,
  MethodName,
  MethodRequest,
  MethodResponse
} from "../types/registry.js";
import { computeEventMasks, EVENT_REGISTRY } from "../types/registry.js";
import type {
  CallContext,
  CallOfflinePolicy,
  CallOptions,
  QueuedCallCoalescePrevious,
  Stream
} from "./types.js";

export interface ClientDeliveryQueueOptions {
  maxSize?: number;
  overflow?: "drop-oldest" | "drop-newest" | "reject";
}

export interface EventDeliveryPolicy {
  offline?: "fail-fast" | "queue";
  queue?: ClientDeliveryQueueOptions;
}

export interface CallDeliveryDefaults {
  timeoutMs?: number;
  offlinePolicy?: CallOfflinePolicy;
}

export interface CallMethodDeliveryPolicy extends CallDeliveryDefaults {
  coalesceKey?: string;
  coalescePrevious?: QueuedCallCoalescePrevious;
}

export interface CallDeliveryPolicy {
  default?: CallDeliveryDefaults;
  queue?: ClientDeliveryQueueOptions;
  methods?: Record<string, CallMethodDeliveryPolicy>;
}

export interface ClientDeliveryOptions {
  events?: EventDeliveryPolicy;
  calls?: CallDeliveryPolicy;
}

export interface ClientOptions {
  logicalRole?: LogicalRole;
  defaultTimeoutMs?: number;
  handshakeTimeoutMs?: number;
  heartbeatIntervalMs?: number;
  maxFrameSize?: number;
  reconnect?: ReconnectPolicy;
  delivery?: ClientDeliveryOptions;
  diagnostics?: AxtpDiagnostics;
}

export type ClientState = "idle" | "connecting" | "ready" | "reconnecting" | "closed";

const DEFAULT_CONNECT_TIMEOUT_MS = 30_000;
const DEFAULT_TIMEOUT_MS = 10_000;
const DEFAULT_MAX_FRAME_SIZE = 4096;
const DEFAULT_HEARTBEAT_MS = 5_000;

type QueuedEvent = {
  event: string;
  payload: unknown;
  resolve: () => void;
  reject: (err: AxtpError) => void;
};

type QueuedCallWaiter = {
  resolve: (value: unknown) => void;
  reject: (err: AxtpError) => void;
};

type QueuedCall = {
  method: string;
  params: unknown;
  timeoutMs: number | undefined;
  coalesceKey: string | undefined;
  waiters: QueuedCallWaiter[];
};

type ResolvedEventDeliveryPolicy = {
  offline: "fail-fast" | "queue";
  queue: Required<ClientDeliveryQueueOptions>;
};

export class AxtpClient {
  /** handler 路由：跨连接复用（作为每个 Endpoint 的 globalHandlers）。 */
  private readonly router = new HandlerRouter();
  private readonly subscribedEvents = new Set<string>();
  private endpoint: AxtpEndpoint | undefined;
  private state: ClientState = "idle";
  private firstReady = true;
  private coordinator: ReconnectCoordinator | undefined;
  private readonly eventOutbox: QueuedEvent[] = [];
  private readonly callQueue: QueuedCall[] = [];
  private flushingCallQueue = false;
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
      this.coordinator = new ReconnectCoordinator(
        policy,
        () => this.transport.connect(),
        (t) => this.spawnEndpoint(t),
        () => this.handleReconnectFailed(),
        (e) => this.onError.emit(e)
      );
    }

    void this.tryOpen(policy.enabled);
    await this.awaitReady(timeoutMs);
    this.setState("ready");
    this.flushOutbox();
    this.flushCallQueue();
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
      eventMasks: this.computeEventMasks(),
      diagnostics: this.options.diagnostics
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
    this.flushOutbox();
    this.flushCallQueue();
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
    const err = new AxtpError(ErrorCode.TransportDisconnected, "reconnect attempts exhausted");
    this.failReady(err);
    this.rejectCallQueue(err);
    this.setState("closed");
    this.rejectOutbox(err);
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

  private resolveCallOptions(method: string, options?: CallOptions): CallOptions {
    return {
      ...this.options.delivery?.calls?.default,
      ...this.options.delivery?.calls?.methods?.[method],
      ...options
    };
  }

  private resolveCallQueueOptions(): Required<ClientDeliveryQueueOptions> {
    const queue = this.options.delivery?.calls?.queue;
    return {
      maxSize: queue?.maxSize ?? 1000,
      overflow: queue?.overflow ?? "reject"
    };
  }

  private resolveEventDeliveryPolicy(): ResolvedEventDeliveryPolicy {
    const eventPolicy = this.options.delivery?.events;
    return {
      offline: eventPolicy?.offline ?? "fail-fast",
      queue: {
        maxSize: eventPolicy?.queue?.maxSize ?? 1000,
        overflow: eventPolicy?.queue?.overflow ?? "reject"
      }
    };
  }

  // ===== 四件套 =====

  call<K extends MethodName>(
    method: K,
    params: MethodRequest<K>,
    options?: CallOptions
  ): Promise<MethodResponse<K>> {
    return this.callRaw(method, params, options) as Promise<MethodResponse<K>>;
  }

  /** 弱类型 call：method 为任意 string、params 为 unknown。动态/自定义方法名走这里。 */
  async callRaw(method: string, params: unknown, options?: CallOptions): Promise<unknown> {
    const resolvedOptions = this.resolveCallOptions(method, options);
    if (resolvedOptions.offlinePolicy === "wait-ready") {
      const ep = await this.waitForUsable();
      return ep.call(method, params, resolvedOptions.timeoutMs);
    }
    if (resolvedOptions.offlinePolicy === "queue") {
      const ep = this.endpoint;
      if (this.state === "ready" && ep !== undefined)
        return ep.call(method, params, resolvedOptions.timeoutMs);
      return this.enqueueCall(method, params, resolvedOptions);
    }
    const ep = this.requireUsable();
    return ep.call(method, params, resolvedOptions.timeoutMs);
  }

  private enqueueCall(method: string, params: unknown, options: CallOptions): Promise<unknown> {
    if (this.state === "closed")
      return Promise.reject(new AxtpError(ErrorCode.TransportDisconnected, "client closed"));

    return new Promise<unknown>((resolve, reject) => {
      const waiter: QueuedCallWaiter = { resolve, reject };
      if (options.coalesceKey !== undefined) {
        const existing = this.callQueue.find((item) => item.coalesceKey === options.coalesceKey);
        if (existing !== undefined) {
          const previousWaiters = existing.waiters.splice(0);
          if ((options.coalescePrevious ?? "reject") === "resolve-with-next") {
            existing.waiters.push(...previousWaiters, waiter);
          } else {
            const replacement =
              options.coalescePrevious === "drop"
                ? undefined
                : new AxtpError(ErrorCode.InvalidState, "queued call superseded");
            for (const previous of previousWaiters) {
              if (replacement === undefined) previous.resolve(undefined);
              else previous.reject(replacement);
            }
            existing.waiters.push(waiter);
          }
          existing.method = method;
          existing.params = params;
          existing.timeoutMs = options.timeoutMs;
          this.flushCallQueue();
          return;
        }
      }

      const queue = this.resolveCallQueueOptions();
      const maxSize = queue.maxSize;
      const overflow = queue.overflow;
      if (this.callQueue.length >= maxSize) {
        if (overflow === "reject") {
          reject(new AxtpError(ErrorCode.InvalidState, "client call queue full"));
          return;
        }
        if (overflow === "drop-newest") {
          resolve(undefined);
          return;
        }
        const dropped = this.callQueue.shift();
        for (const previous of dropped?.waiters ?? []) previous.resolve(undefined);
      }

      this.callQueue.push({
        method,
        params,
        timeoutMs: options.timeoutMs,
        coalesceKey: options.coalesceKey,
        waiters: [waiter]
      });
      this.flushCallQueue();
    });
  }

  private async flushCallQueue(): Promise<void> {
    if (this.flushingCallQueue) return;
    this.flushingCallQueue = true;
    try {
      while (this.state === "ready" && this.endpoint !== undefined && this.callQueue.length > 0) {
        const ep = this.endpoint;
        const item = this.callQueue.shift() as QueuedCall;
        try {
          const value = await ep.call(item.method, item.params, item.timeoutMs);
          for (const waiter of item.waiters) waiter.resolve(value);
        } catch (err) {
          const axtpErr =
            err instanceof AxtpError
              ? err
              : new AxtpError(ErrorCode.TransportDisconnected, "failed to flush call queue", err);
          for (const waiter of item.waiters) waiter.reject(axtpErr);
        }
      }
    } finally {
      this.flushingCallQueue = false;
      if (this.state === "ready" && this.endpoint !== undefined && this.callQueue.length > 0) {
        void this.flushCallQueue();
      }
    }
  }

  private rejectCallQueue(err: AxtpError): void {
    const queued = this.callQueue.splice(0);
    for (const item of queued) {
      for (const waiter of item.waiters) waiter.reject(err);
    }
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

  emit<K extends EventName>(event: K, payload: EventPayload<K>): Promise<void> {
    return this.emitRaw(event, payload);
  }

  /** 弱类型 emit。 */
  emitRaw(event: string, payload: unknown): Promise<void> {
    const ep = this.endpoint;
    if (this.state === "ready" && ep !== undefined) {
      ep.emit(event, payload);
      return Promise.resolve();
    }
    return this.enqueueEvent(event, payload);
  }

  private enqueueEvent(event: string, payload: unknown): Promise<void> {
    if (this.state === "closed")
      return Promise.reject(new AxtpError(ErrorCode.TransportDisconnected, "client closed"));
    const policy = this.resolveEventDeliveryPolicy();
    if (policy.offline !== "queue") {
      try {
        this.requireUsable();
      } catch (err) {
        return Promise.reject(err);
      }
    }
    const maxSize = policy.queue.maxSize;
    const overflow = policy.queue.overflow;
    if (this.eventOutbox.length >= maxSize) {
      if (overflow === "reject") {
        return Promise.reject(new AxtpError(ErrorCode.InvalidState, "client event outbox full"));
      }
      if (overflow === "drop-newest") return Promise.resolve();
      const dropped = this.eventOutbox.shift();
      dropped?.resolve();
    }
    return new Promise<void>((resolve, reject) => {
      this.eventOutbox.push({ event, payload, resolve, reject });
      this.flushOutbox();
    });
  }

  private flushOutbox(): void {
    while (this.state === "ready" && this.endpoint !== undefined && this.eventOutbox.length > 0) {
      const item = this.eventOutbox.shift() as QueuedEvent;
      try {
        this.endpoint.emit(item.event, item.payload);
        item.resolve();
      } catch (err) {
        item.reject(
          err instanceof AxtpError
            ? err
            : new AxtpError(ErrorCode.TransportDisconnected, "failed to flush event outbox", err)
        );
      }
    }
  }

  private rejectOutbox(err: AxtpError): void {
    const queued = this.eventOutbox.splice(0);
    for (const item of queued) item.reject(err);
  }

  on<K extends EventName>(event: K, handler: (payload: EventPayload<K>) => void): () => void {
    return this.onRaw(event, handler as UntypedEventHandler);
  }

  /** 弱类型 on。非 registry 事件名不进入 eventMasks（computeEventMasks 对未知名自动跳过）。 */
  onRaw(event: string, handler: UntypedEventHandler): () => void {
    this.subscribedEvents.add(event);
    emitDiagnostic(this.options.diagnostics, {
      level: "debug",
      scope: "client",
      event: "listener.add",
      name: event,
      known: event in EVENT_REGISTRY
    });
    return this.router.addEventListener(event, handler);
  }

  openStream(
    method: string,
    params: unknown,
    options?: CallOptions
  ): Promise<{ streamId: number; response: unknown; stream: Stream }> {
    const ep = this.requireUsable();
    return ep.openStream(method, params, options?.timeoutMs) as Promise<{
      streamId: number;
      response: unknown;
      stream: Stream;
    }>;
  }

  onStream(
    method: string,
    handler: (params: unknown, stream: Stream) => unknown | Promise<unknown>
  ): () => void {
    const ep = this.requireUsable();
    return ep.onStream(method, handler);
  }

  /** 主动关闭，不再重连。 */
  async close(): Promise<void> {
    this.coordinator?.stop();
    this.setState("closed");
    this.endpoint?.close();
    const err = new AxtpError(ErrorCode.TransportDisconnected, "closed");
    this.failReady(err);
    this.rejectCallQueue(err);
    this.rejectOutbox(err);
  }

  private computeEventMasks(): string | undefined {
    if (this.subscribedEvents.size === 0) return undefined;
    const events = [...this.subscribedEvents];
    const masks = computeEventMasks(events);
    emitDiagnostic(this.options.diagnostics, {
      level: "debug",
      scope: "client",
      event: "identify.eventMasks",
      data: diagnosticData(this.options.diagnostics, { events, masks })
    });
    return masks;
  }

  private requireUsable(): AxtpEndpoint {
    if (this.state === "closed")
      throw new AxtpError(ErrorCode.TransportDisconnected, "client closed");
    if (this.state === "reconnecting")
      throw new AxtpError(ErrorCode.TransportDisconnected, "client reconnecting");
    const ep = this.endpoint;
    if (this.state !== "ready" || ep === undefined)
      throw new AxtpError(ErrorCode.InvalidState, "client not ready");
    return ep;
  }

  private waitForUsable(): Promise<AxtpEndpoint> {
    try {
      return Promise.resolve(this.requireUsable());
    } catch (err) {
      if (this.state === "closed") return Promise.reject(err);
      return new Promise<AxtpEndpoint>((resolve, reject) => {
        const cleanup = () => {
          unsubscribeConnect();
          unsubscribeFailed();
          unsubscribeState();
        };
        const resolveIfReady = () => {
          try {
            const ep = this.requireUsable();
            cleanup();
            resolve(ep);
          } catch {
            /* still not ready */
          }
        };
        const unsubscribeConnect = this.onConnect.subscribe(resolveIfReady);
        const unsubscribeFailed = this.onReconnectFailed.subscribe(() => {
          cleanup();
          reject(new AxtpError(ErrorCode.TransportDisconnected, "reconnect attempts exhausted"));
        });
        const unsubscribeState = this.onStateChange.subscribe((state) => {
          if (state === "closed") {
            cleanup();
            reject(new AxtpError(ErrorCode.TransportDisconnected, "client closed"));
          } else if (state === "ready") {
            resolveIfReady();
          }
        });
        resolveIfReady();
      });
    }
  }
}
