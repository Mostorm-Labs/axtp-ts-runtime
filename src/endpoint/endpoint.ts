// AxtpEndpoint：粘合 + 时序 + 流驱动（取代 Connection + Session）—— Endpoint 层。
//
// 持有 1 transport + 1 core + 1 broker。wire 时：
//   transport.readable ──pipeThrough(core.inbound,{signal})──▶ reader 消费 CoreEvent（路由 broker/pending/stream）
//   core.outbound ──pipeTo(transport.writable,{signal})──▶ 字节发对端
// AbortController：close/重连 abort() → signal 级联取消整条 pipe 链。
//
// 出站四件套（call/emit/openStream）组合 core；入站握手/控制由 core 自动处理。

import { AxtpCore } from "../core/core.js";
import type { CoreEvent } from "../core/events.js";
import { BasicBroker } from "../broker/broker.js";
import type {
  GlobalHandlerSource,
  UntypedEventHandler,
  UntypedMethodHandler
} from "../broker/context.js";
import type {
  KeepaliveStreamTransport,
  LogicalRole,
  PhysicalRole,
  StreamTransport,
  TransportProfile
} from "../transport/contract.js";
import { keepaliveMode, supportsControl, supportsStream } from "../transport/profile.js";
import { AxtpError, ErrorCode } from "../types/error.js";
import { EventStream } from "../types/events.js";
import { Heartbeat } from "./timers.js";
import { StreamManager } from "./streamManager.js";
import type { Stream } from "./stream.js";

export type { KeepaliveStreamTransport, StreamTransport } from "../transport/contract.js";

export type EndpointLifecycle = "idle" | "connecting" | "ready" | "closed";

export interface EndpointOptions {
  readonly transport: StreamTransport;
  readonly physicalRole: PhysicalRole;
  readonly logicalRole: LogicalRole;
  readonly maxFrameSize: number;
  readonly heartbeatIntervalMs: number;
  readonly defaultTimeoutMs?: number;
  readonly globalHandlers?: GlobalHandlerSource;
  readonly handshakeSeed?: number;
  readonly eventMasks?: string;
  /** Server 管理时的 endpoint localId（传入 broker → CallContext.id 供 handler 定向操作）。 */
  readonly id?: number;
}

export class AxtpEndpoint {
  readonly core: AxtpCore;
  readonly broker: BasicBroker;
  readonly onReady = new EventStream<void>();
  readonly onClose = new EventStream<{ remote: boolean }>();
  readonly onError = new EventStream<AxtpError>();

  private readonly transport: StreamTransport;
  private readonly physicalRole: PhysicalRole;
  private readonly profile: TransportProfile;
  private readonly defaultTimeoutMs: number;
  private ac: AbortController | undefined;
  private lifecycle: EndpointLifecycle = "idle";
  private heartbeat: Heartbeat | undefined;
  private keepaliveAckUnsub: (() => void) | undefined;
  private readonly streamMgr: StreamManager;

  constructor(opts: EndpointOptions) {
    this.transport = opts.transport;
    this.physicalRole = opts.physicalRole;
    this.profile = opts.transport.profile;
    this.defaultTimeoutMs = opts.defaultTimeoutMs ?? 10000;
    this.core = new AxtpCore({
      profile: this.profile,
      physicalRole: opts.physicalRole,
      logicalRole: opts.logicalRole,
      maxFrameSize: opts.maxFrameSize,
      heartbeatIntervalMs: opts.heartbeatIntervalMs,
      handshakeSeed: opts.handshakeSeed,
      eventMasks: opts.eventMasks
    });
    this.broker = new BasicBroker(opts.globalHandlers);
    this.broker.setSink({
      onResult: (msg) => this.core.sendRpc(msg),
      onError: (err) => this.onError.emit(err)
    });
    this.broker.emit = (event, payload) => this.core.emit(event, payload);
    this.broker.id = opts.id;
    this.streamMgr = new StreamManager((sp) => this.core.sendStream(sp));
  }

  get state(): EndpointLifecycle {
    return this.lifecycle;
  }

  get isReady(): boolean {
    return this.lifecycle === "ready";
  }

  get sid(): string {
    return this.core.sid;
  }

  /** 启动：wire transport + 发起链路握手。 */
  start(): void {
    if (this.lifecycle !== "idle") return;
    this.lifecycle = "connecting";
    this.wire(this.transport);
  }

  private wire(t: StreamTransport): void {
    this.ac = new AbortController();
    const piped = t.readable.pipeThrough(this.core.inbound, { signal: this.ac.signal });
    void this.consume(piped.getReader());
    this.core.outbound.readable.pipeTo(t.writable, { signal: this.ac.signal }).catch(() => {
      /* abort 或 transport 错误：由 consume/close 路径处理 */
    });
    // 链路发起：framed client 发 OPEN；unframed 立即 markLinkReady；framed server 等 OPEN（core inbound 处理）。
    if (supportsControl(this.profile)) {
      if (this.physicalRole === "client") this.core.sendControlOpen();
    } else {
      this.core.markLinkReady();
    }
  }

  private async consume(reader: ReadableStreamDefaultReader<CoreEvent>): Promise<void> {
    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) {
          if (this.lifecycle !== "closed") this.close(true);
          return;
        }
        this.onCoreEvent(value);
      }
    } catch {
      // reader reject = transport 错误（非对端干净关闭）；lifecycle 已 closed（abort）→ no-op
      if (this.lifecycle !== "closed") this.close(false);
    }
  }

  private onCoreEvent(ev: CoreEvent): void {
    if (this.lifecycle === "closed") return;
    switch (ev.kind) {
      case "linkReady":
        this.startHeartbeat(ev.heartbeatIntervalMs);
        break;
      case "handshakeReady":
        if (this.lifecycle !== "ready") {
          this.lifecycle = "ready";
          this.onReady.emit(undefined);
        }
        break;
      case "rpcRequest":
        this.broker.dispatchRequest(ev.msg);
        break;
      case "rpcEvent":
        this.broker.dispatchEvent(ev.msg);
        break;
      case "streamData":
        this.streamMgr.onData(ev.msg);
        break;
      case "linkOpenRejected":
      case "linkClosing":
        this.close(true);
        break;
      case "handshakeError":
      case "error":
        this.onError.emit(ev.err);
        break;
      case "heartbeatAck":
        this.heartbeat?.reset();
        break;
    }
  }

  /** linkReady 后启动心跳：framed=CONTROL Heartbeat（core.sendHeartbeat）；unframed=native（transport.sendKeepalive）。 */
  private startHeartbeat(intervalMs: number): void {
    const mode = keepaliveMode(this.profile);
    if (mode === "none" || this.heartbeat !== undefined) return;
    const timeoutMs = Math.max(intervalMs * 2, 10000);
    this.heartbeat = new Heartbeat({
      intervalMs,
      timeoutMs,
      onTick: () => {
        if (mode === "control-heartbeat") this.core.sendHeartbeat();
        else if (isKeepaliveTransport(this.transport)) this.transport.sendKeepalive();
      },
      onTimeout: () => this.close(false, true)
    });
    if (mode === "native-keepalive" && isKeepaliveTransport(this.transport)) {
      this.keepaliveAckUnsub = this.transport.onKeepaliveAck(() => this.heartbeat?.reset());
    }
    this.heartbeat.start();
  }

  // ===== 出站四件套 =====

  call(method: string, params: unknown, timeoutMs?: number): Promise<unknown> {
    this.requireReady();
    return this.core.call(method, params, timeoutMs ?? this.defaultTimeoutMs);
  }

  emit(event: string, payload: unknown): void {
    this.requireReady();
    this.core.emit(event, payload);
  }

  /** 注册方法 handler（委托 broker；conformance/直接 Endpoint 用法）。 */
  handle(method: string, handler: UntypedMethodHandler): () => void {
    return this.broker.setMethod(method, handler);
  }

  /** 注册事件 handler（委托 broker）。 */
  on(event: string, handler: UntypedEventHandler): () => void {
    return this.broker.addEventListener(event, handler);
  }

  /** 发起 STREAM（framed only）：openStream RPC 拿 streamId，返回双向 Stream。 */
  openStream(
    method: string,
    params: unknown,
    timeoutMs?: number
  ): Promise<{ streamId: number; response: unknown; stream: Stream }> {
    this.requireReady();
    if (!supportsStream(this.profile)) {
      throw new AxtpError(ErrorCode.NotSupported, "STREAM not supported on this transport profile");
    }
    return this.streamMgr.openStream(
      (m, p) => this.core.call(m, p, timeoutMs ?? this.defaultTimeoutMs),
      method,
      params
    );
  }

  /** 注册建流 handler（receive 方）：handler 收到 Stream，返回含 streamId 的 result。 */
  onStream(
    method: string,
    handler: (params: unknown, stream: Stream) => unknown | Promise<unknown>
  ): () => void {
    return this.broker.setMethod(method, this.streamMgr.wrapStreamHandler(handler));
  }

  /** 主动关闭：abort pipe 链 + 关 transport。remote=true 对端发起；terminate=true 死连接强制断（不等 CLOSE 握手）。 */
  close(remote = false, terminate = false): void {
    if (this.lifecycle === "closed") return;
    this.heartbeat?.stop();
    this.heartbeat = undefined;
    this.keepaliveAckUnsub?.();
    this.keepaliveAckUnsub = undefined;
    this.ac?.abort();
    if (terminate && this.transport.terminate) this.transport.terminate();
    else this.transport.close();
    this.lifecycle = "closed";
    this.onClose.emit({ remote });
    this.core.rejectAllPending(
      new AxtpError(ErrorCode.TransportDisconnected, `endpoint closed${remote ? " by peer" : ""}`)
    );
    this.streamMgr.abortAll(`endpoint closed${remote ? " by peer" : ""}`);
    this.onReady.close();
    this.onClose.close();
    this.onError.close();
  }

  private requireReady(): void {
    if (this.lifecycle === "closed")
      throw new AxtpError(ErrorCode.TransportDisconnected, "endpoint closed");
    if (this.lifecycle !== "ready")
      throw new AxtpError(ErrorCode.InvalidState, "endpoint not ready");
  }
}

/** Type guard：transport 是否支持 native keepalive（WS ping/pong）。 */
function isKeepaliveTransport(t: StreamTransport): t is KeepaliveStreamTransport {
  return "sendKeepalive" in t;
}
