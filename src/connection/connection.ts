// Connection：传输连接 + 链路层编排 + 传输重连（连接语义，不导出）。
// 持有 transport + CodecPipeline(framed) + Heartbeat + ReconnectCoordinator。
// 解码后把 RpcPayload/StreamPayload 上交给 Session。
//
// 重连：transport.onClose → ReconnectCoordinator → 新 transport → attachTransport(统一重置) → 链路启动。
// 心跳：framed 用 CONTROL Heartbeat/Ack；WS 用原生 keepalive。

import type { Bytes } from "../io/bytes.js";
import { decodeJsonRpc, encodeJsonRpc } from "../protocol/codec/jsonRpc.js";
import type { RpcPayload, StreamPayload } from "../protocol/model.js";
import type {
  CloseReason,
  ITransport,
  PhysicalRole,
  TransportCapabilities,
  TransportFactory
} from "../transport/transport.js";
import { CloseCode } from "../transport/transport.js";
import { AxtpError, ErrorCode } from "../types/error.js";
import { EventStream } from "../types/events.js";
import { CodecPipeline } from "./codec/codecPipeline.js";
import { type NegotiatedLink } from "./codec/controlSession.js";
import { Heartbeat } from "./heartbeat.js";
import { resolvePolicy, type ReconnectInfo, type ReconnectPolicy } from "./reconnect/reconnect.js";
import { ReconnectCoordinator } from "./reconnect/reconnectCoordinator.js";

export interface ConnectionOptions {
  heartbeatIntervalMs?: number;
  heartbeatTimeoutMs?: number;
  maxFrameSize?: number;
  reconnect?: ReconnectPolicy;
}

/** Connection 生命周期状态机。 */
export type ConnectionState =
  | "idle" // 构造后未 start
  | "link_connecting" // start() 后，等链路 ready
  | "link_ready" // 链路 ready
  | "reconnecting" // 传输断开，重连退避/尝试中
  | "closed"; // 终态

const DEFAULT_HEARTBEAT_INTERVAL_MS = 1000;
const DEFAULT_MAX_FRAME_SIZE = 4096;

export class Connection {
  readonly onClose = new EventStream<CloseReason>();
  readonly onDisconnect = new EventStream<CloseReason>();
  readonly onError = new EventStream<AxtpError>();
  readonly onPayload = new EventStream<RpcPayload>();
  readonly onStream = new EventStream<StreamPayload>();
  readonly onLinkReady = new EventStream<void>();
  readonly onReconnect = new EventStream<ReconnectInfo>();
  readonly onReconnectFailed = new EventStream<void>();

  private transport: ITransport;
  private capabilities: TransportCapabilities;
  private readonly physicalRole: PhysicalRole;
  private readonly options: ConnectionOptions;

  private pipeline: CodecPipeline | undefined;
  private heartbeat: Heartbeat | undefined;
  private reconnectCoordinator: ReconnectCoordinator | undefined;
  private keepaliveUnsub: (() => void) | undefined;
  private transportUnsubs: Array<() => void> = [];

  private connState: ConnectionState = "idle";
  private started = false;
  private pendingBytes: Bytes[] = [];
  private nextHeartbeatControlId = 0x100;

  constructor(
    physicalRole: PhysicalRole,
    transport: ITransport,
    options: ConnectionOptions = {},
    transportFactory?: TransportFactory
  ) {
    this.physicalRole = physicalRole;
    this.options = options;

    const policy = resolvePolicy(options.reconnect);
    if (policy.enabled && transportFactory !== undefined) {
      this.reconnectCoordinator = ReconnectCoordinator.fromPolicy(
        options.reconnect,
        transportFactory,
        {
          onReconnected: (t) => this.handleReconnected(t),
          onFailed: () => this.handleReconnectFailed(),
          onError: (err) => this.onError.emit(err)
        }
      );
    }
    // reconnect enabled but no transportFactory: 延迟到 start() 检查（此时订阅者已就绪）

    this.capabilities = transport.capabilities;
    this.transport = transport;
    this.attachTransport(transport);
  }

  /** 统一状态转换入口。 */
  private setState(newState: ConnectionState): void {
    if (this.connState === newState) return;
    if (this.connState === "closed") return;
    this.connState = newState;
  }

  get state(): ConnectionState {
    return this.connState;
  }

  get isClosed(): boolean {
    return this.connState === "closed";
  }

  get isReconnecting(): boolean {
    return this.connState === "reconnecting";
  }

  /**
   * 绑定一条 transport：统一重置所有 transport 相关可变状态 + 订阅事件。
   * 构造和重连都调此方法。确保重连时 pipeline/heartbeat/controlId 完全重置。
   */
  private attachTransport(transport: ITransport): void {
    // 1. detach 旧 transport 订阅
    for (const unsub of this.transportUnsubs) unsub();
    this.transportUnsubs = [];

    // 2. 清理旧 keepalive 监听
    this.keepaliveUnsub?.();
    this.keepaliveUnsub = undefined;

    // 3. 停旧心跳
    this.heartbeat?.stop();
    this.heartbeat = undefined;

    // 4. 重置链路状态（linkReadyFired 改为状态机驱动，无需 boolean）
    this.nextHeartbeatControlId = 0x100;

    // 5. 刷新 capabilities
    this.capabilities = transport.capabilities;

    // 6. 重建 pipeline（framed only）——完全新实例，无旧状态泄漏
    this.pipeline = undefined;
    if (this.capabilities.supportsControl) {
      this.pipeline = new CodecPipeline(
        this.physicalRole,
        transport,
        {
          maxFrameSize: this.options.maxFrameSize ?? DEFAULT_MAX_FRAME_SIZE,
          heartbeatIntervalMs: this.options.heartbeatIntervalMs ?? DEFAULT_HEARTBEAT_INTERVAL_MS
        },
        {
          onRpc: (p) => this.onPayload.emit(p),
          onStream: (s) => this.onStream.emit(s),
          onControlHeartbeat: (cid) => this.pipeline?.sendHeartbeatAck(cid),
          onControlHeartbeatAck: () => this.heartbeat?.reset(),
          onControlClosing: () => this.close(CloseCode.Normal, "remote close"),
          onControlRejected: (sc) =>
            this.close(
              CloseCode.HandshakeFailed,
              `link rejected: 0x${sc.toString(16).padStart(4, "0")}`
            ),
          onLinkReady: (neg) => this.onNegotiatedLinkReady(neg)
        }
      );
    }

    // 7. 订阅 transport 事件
    this.transportUnsubs.push(
      transport.onMessage.subscribe((bytes) => {
        if (this.connState === "closed") return;
        if (!this.started) {
          this.pendingBytes.push(bytes);
          return;
        }
        this.onTransportBytes(bytes);
      })
    );
    this.transportUnsubs.push(
      transport.onClose.subscribe((reason) => this.handleTransportClose(reason))
    );
    this.transportUnsubs.push(transport.onError.subscribe((err) => this.onError.emit(err)));

    transport.attach?.();
  }

  start(): void {
    if (this.started) return;
    this.started = true;

    // 延迟检查：reconnect enabled 但无 transportFactory（构造期无法 emit，此时订阅者已就绪）
    const policy = resolvePolicy(this.options.reconnect);
    if (policy.enabled && this.reconnectCoordinator === undefined) {
      this.onError.emit(
        new AxtpError(
          ErrorCode.InvalidState,
          "reconnect enabled but no transportFactory provided; reconnect disabled"
        )
      );
    }

    this.setState("link_connecting");
    this.flushPendingAndStartLink();
  }

  private flushPendingAndStartLink(): void {
    const buffered = this.pendingBytes.splice(0);
    for (const bytes of buffered) this.onTransportBytes(bytes);

    this.startLinkHandshake();
  }

  /** 链路握手启动：framed 发 OPEN（client）/ 等 OPEN（server）；WS 直接 linkReady + 心跳。 */
  private startLinkHandshake(): void {
    if (this.capabilities.supportsControl) {
      if (this.physicalRole === "client") this.pipeline?.sendOpen();
    } else {
      this.fireLinkReady();
      this.startHeartbeat(this.options.heartbeatIntervalMs ?? DEFAULT_HEARTBEAT_INTERVAL_MS);
    }
  }

  sendRpc(payload: RpcPayload): void {
    if (this.connState === "closed") return;
    if (this.connState === "reconnecting")
      throw new AxtpError(ErrorCode.TransportDisconnected, "connection reconnecting");
    const jsonBytes = encodeJsonRpc(payload);
    if (this.capabilities.supportsControl) {
      this.pipeline?.sendRpc(jsonBytes);
    } else {
      this.transport.send(jsonBytes);
    }
  }

  sendStream(payload: StreamPayload): void {
    if (this.connState === "closed") return;
    if (this.connState === "reconnecting")
      throw new AxtpError(ErrorCode.TransportDisconnected, "connection reconnecting");
    if (!this.capabilities.supportsControl) {
      throw new AxtpError(ErrorCode.NotSupported, "STREAM not supported on this transport");
    }
    this.pipeline?.sendStreamPayload(payload);
  }

  close(code: CloseCode = CloseCode.Normal, reason = "local close"): void {
    if (this.connState === "closed") return;
    this.reconnectCoordinator?.stop();
    this.heartbeat?.stop();
    if (this.capabilities.supportsControl && this.pipeline?.controlSessionIsOpen) {
      this.pipeline.sendClose();
    }
    this.transport.close();
    this.terminate({ code, reason, remote: false });
  }

  /** 统一收尾：setState(closed) + emit onClose (+可选 onReconnectFailed) + cleanupStreams。 */
  private terminate(reason: CloseReason, emitReconnectFailed = false): void {
    this.setState("closed");
    if (emitReconnectFailed) this.onReconnectFailed.emit(undefined);
    this.onClose.emit(reason);
    this.cleanupStreams();
  }

  // ===== 重连 =====

  private handleTransportClose(reason: CloseReason): void {
    if (this.connState === "closed") return;
    this.heartbeat?.stop();
    this.heartbeat = undefined;

    // 任何断连都通知上层（Session 据此 reject pending calls + abort streams）
    this.onDisconnect.emit(reason);

    if (this.reconnectCoordinator !== undefined) {
      // 有重连策略：进入重连（start 幂等，内部有 active 守卫防重复）
      this.setState("reconnecting");
      this.reconnectCoordinator.start();
      return;
    }

    // 无重连策略：直接关闭
    this.terminate(reason);
  }

  private handleReconnected(newTransport: ITransport): void {
    // 先立即 detach 旧 transport 订阅，防止旧 transport 的异步投递字节进入新 pipeline
    for (const unsub of this.transportUnsubs) unsub();
    this.transportUnsubs = [];

    this.transport = newTransport;
    this.pendingBytes = [];

    const attempt = this.reconnectCoordinator?.attemptCount ?? 0;
    this.onReconnect.emit({ attempt });

    this.attachTransport(newTransport);
    this.setState("link_connecting");

    this.startLinkHandshake();
  }

  private handleReconnectFailed(): void {
    this.transport.close();
    this.terminate(
      { code: CloseCode.Reconnect, reason: "reconnect failed", remote: false },
      true
    );
  }

  // ===== 内部 =====

  private onTransportBytes(bytes: Bytes): void {
    if (this.connState === "closed") return;
    if (this.capabilities.supportsControl) {
      this.pipeline?.onBytes(bytes);
    } else {
      const payload = decodeJsonRpc(bytes);
      if (payload !== undefined) this.onPayload.emit(payload);
    }
  }

  private onNegotiatedLinkReady(neg: NegotiatedLink): void {
    if (!neg.accepted) return;
    this.pipeline?.setMaxFrameSize(neg.maxFrameSize);
    this.fireLinkReady();
    this.startHeartbeat(neg.heartbeatIntervalMs);
    this.reconnectCoordinator?.onSuccess();
  }

  private fireLinkReady(): void {
    if (this.connState === "link_ready") return;
    this.setState("link_ready");
    this.onLinkReady.emit(undefined);
    if (!this.capabilities.supportsControl) {
      this.reconnectCoordinator?.onSuccess();
    }
  }

  private startHeartbeat(negotiatedIntervalMs: number): void {
    this.keepaliveUnsub?.();
    this.keepaliveUnsub = undefined;
    this.heartbeat?.stop();

    const interval =
      negotiatedIntervalMs || this.options.heartbeatIntervalMs || DEFAULT_HEARTBEAT_INTERVAL_MS;
    const timeout = this.options.heartbeatTimeoutMs ?? Math.max(interval * 2, 10000);

    if (this.capabilities.supportsControl) {
      this.heartbeat = new Heartbeat({
        intervalMs: interval,
        timeoutMs: timeout,
        onTick: () => {
          const cid = this.nextHeartbeatControlId;
          this.nextHeartbeatControlId = (this.nextHeartbeatControlId + 1) & 0xffff;
          this.pipeline?.sendHeartbeat(cid);
        },
        onTimeout: () => this.close(CloseCode.HeartbeatTimeout, "heartbeat timeout")
      });
    } else if (this.capabilities.supportsKeepalive) {
      const t = this.transport;
      this.heartbeat = new Heartbeat({
        intervalMs: interval,
        timeoutMs: timeout,
        onTick: () => t.sendKeepalive?.(),
        onTimeout: () => this.close(CloseCode.HeartbeatTimeout, "heartbeat timeout")
      });
      this.keepaliveUnsub = t.onKeepaliveAck?.(() => this.heartbeat?.reset());
    } else {
      this.onError.emit(
        new AxtpError(
          ErrorCode.NotSupported,
          "Transport supports neither CONTROL heartbeat nor native keepalive"
        )
      );
    }
    this.heartbeat?.start();
  }

  private cleanupStreams(): void {
    this.onPayload.close();
    this.onStream.close();
    this.onLinkReady.close();
    this.onError.close();
    this.onReconnect.close();
    this.onReconnectFailed.close();
    this.onDisconnect.close();
    this.onClose.close();
  }
}
