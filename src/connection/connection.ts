// Connection：传输连接 + 链路层编排 + 传输重连（连接语义，不导出）。
// 持有 transport + CodecPipeline(framed) + Heartbeat + ReconnectCoordinator。
// 解码后把 RpcPayload/StreamPayload 上交给 Session。
//
// 重连：transport.onClose → ReconnectCoordinator → 新 transport → attachTransport(统一重置) → 链路启动。
// 心跳：framed 用 CONTROL Heartbeat/Ack；WS 用原生 keepalive。

import type { Bytes } from "../io/bytes.js";
import type { NegotiationParams } from "../protocol/codec/control.js";
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
import { CodecPipeline } from "./codecPipeline.js";
import { type NegotiatedLink } from "./controlSession.js";
import { Heartbeat } from "./heartbeat.js";
import { resolvePolicy, type ReconnectInfo, type ReconnectPolicy } from "./reconnect.js";
import { ReconnectCoordinator } from "./reconnectCoordinator.js";

export interface ConnectionOptions {
  heartbeatIntervalMs?: number;
  heartbeatTimeoutMs?: number;
  /** framed 链路协商参数（framed-only，不暴露给用户 SessionOptions）。 */
  negotiationParams?: NegotiationParams;
  maxFrameSize?: number;
  reconnect?: ReconnectPolicy;
}

const DEFAULT_HEARTBEAT_INTERVAL_MS = 30000;
const DEFAULT_MAX_FRAME_SIZE = 4096;

export class Connection {
  readonly onClose = new EventStream<CloseReason>();
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
  private readonly transportFactory: TransportFactory | undefined;

  private pipeline: CodecPipeline | undefined;
  private heartbeat: Heartbeat | undefined;
  private reconnectCoordinator: ReconnectCoordinator | undefined;
  private keepaliveUnsub: (() => void) | undefined;
  private transportUnsubs: Array<() => void> = [];

  private started = false;
  private closed = false;
  private linkReadyFired = false;
  private reconnecting = false;
  private nextHeartbeatControlId = 0x100;
  private pendingBytes: Bytes[] = [];

  constructor(
    physicalRole: PhysicalRole,
    transport: ITransport,
    options: ConnectionOptions = {},
    transportFactory?: TransportFactory
  ) {
    this.physicalRole = physicalRole;
    this.options = options;
    this.transportFactory = transportFactory;

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

    this.capabilities = transport.capabilities;
    this.transport = transport;
    this.attachTransport(transport);
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

    // 4. 重置链路状态
    this.linkReadyFired = false;
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
          heartbeatIntervalMs: this.options.heartbeatIntervalMs ?? DEFAULT_HEARTBEAT_INTERVAL_MS,
          negotiationParams: this.options.negotiationParams
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
        if (this.closed) return;
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
    this.flushPendingAndStartLink();
  }

  private flushPendingAndStartLink(): void {
    const buffered = this.pendingBytes.splice(0);
    for (const bytes of buffered) this.onTransportBytes(bytes);

    if (this.capabilities.supportsControl) {
      if (this.physicalRole === "client") this.pipeline?.sendOpen();
    } else {
      this.fireLinkReady();
      this.startHeartbeat(this.options.heartbeatIntervalMs ?? DEFAULT_HEARTBEAT_INTERVAL_MS);
    }
  }

  sendRpc(payload: RpcPayload): void {
    if (this.closed) return;
    if (this.reconnecting)
      throw new AxtpError(ErrorCode.TransportDisconnected, "connection reconnecting");
    const jsonBytes = encodeJsonRpc(payload);
    if (this.capabilities.supportsControl) {
      this.pipeline?.sendRpc(jsonBytes);
    } else {
      this.transport.send(jsonBytes);
    }
  }

  sendStream(payload: StreamPayload): void {
    if (this.closed) return;
    if (this.reconnecting)
      throw new AxtpError(ErrorCode.TransportDisconnected, "connection reconnecting");
    if (!this.capabilities.supportsControl) {
      throw new AxtpError(ErrorCode.NotSupported, "STREAM not supported on this transport");
    }
    this.pipeline?.sendStreamPayload(payload);
  }

  close(code: CloseCode = CloseCode.Normal, reason = "local close"): void {
    if (this.closed) return;
    this.closed = true;
    this.reconnectCoordinator?.stop();
    this.heartbeat?.stop();
    if (this.capabilities.supportsControl && this.pipeline?.controlSessionIsOpen) {
      this.pipeline.sendClose();
    }
    this.transport.close();
    this.onClose.emit({ code, reason, remote: false });
    this.cleanupStreams();
  }

  get isClosed(): boolean {
    return this.closed;
  }

  get isReconnecting(): boolean {
    return this.reconnecting;
  }

  // ===== 重连 =====

  private handleTransportClose(reason: CloseReason): void {
    if (this.closed) return;
    this.heartbeat?.stop();
    this.heartbeat = undefined;
    this.linkReadyFired = false;

    if (this.reconnectCoordinator !== undefined) {
      if (!this.reconnecting) {
        this.reconnecting = true;
        this.reconnectCoordinator.start();
      } else {
        this.reconnectCoordinator.reset();
        this.reconnectCoordinator.start();
      }
      return;
    }

    this.closed = true;
    this.onClose.emit(reason);
    this.cleanupStreams();
  }

  private handleReconnected(newTransport: ITransport): void {
    this.transport = newTransport;
    this.pendingBytes = [];

    const attempt = this.reconnectCoordinator?.attemptCount ?? 0;
    this.onReconnect.emit({ attempt });

    this.attachTransport(newTransport);

    if (this.capabilities.supportsControl) {
      if (this.physicalRole === "client") this.pipeline?.sendOpen();
    } else {
      this.fireLinkReady();
      this.startHeartbeat(this.options.heartbeatIntervalMs ?? DEFAULT_HEARTBEAT_INTERVAL_MS);
    }

    this.reconnecting = false;
  }

  private handleReconnectSuccess(): void {
    this.reconnectCoordinator?.notifySuccess();
    this.reconnectCoordinator?.reset();
  }

  private handleReconnectFailed(): void {
    this.reconnecting = false;
    this.closed = true;
    this.transport.close();
    this.onReconnectFailed.emit(undefined);
    this.onClose.emit({ code: CloseCode.Reconnect, reason: "reconnect failed", remote: false });
    this.cleanupStreams();
  }

  // ===== 内部 =====

  private onTransportBytes(bytes: Bytes): void {
    if (this.closed) return;
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
    this.handleReconnectSuccess();
  }

  private fireLinkReady(): void {
    if (this.linkReadyFired) return;
    this.linkReadyFired = true;
    this.onLinkReady.emit(undefined);
    if (!this.capabilities.supportsControl) {
      this.handleReconnectSuccess();
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
    this.onClose.close();
  }
}
