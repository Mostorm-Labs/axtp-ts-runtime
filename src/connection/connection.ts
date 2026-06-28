// Connection：传输连接 + 链路层编排 + 传输重连（连接语义，不导出）。
// 持有 transport + codec + ControlSession(framed) + Heartbeat + ReconnectCoordinator。
// 解码后把 RpcPayload/StreamPayload 上交给 Session（onPayload/onStream/onLinkReady/onClose/onReconnect）。
// 不做应用语义（sid/handler/pending call 都归 Session）。
//
// 重连（传输层）：transport.onClose → ReconnectCoordinator 退避 → transportFactory() 重建传输
//   → 重建 codec pipeline + 链路 + 心跳 → onReconnect 通知 Session（Session 负责会话重建）。
//   Connection 只管传输层重连，不碰 Session/handler。
//
// 心跳：framed 在链路 ready（FRAMING_READY/ACCEPT）启动，用 CONTROL Heartbeat/Ack；
//       WS 用原生 keepalive（ITransport.sendKeepalive/onKeepaliveAck，capabilities.supportsKeepalive 声明）。

import type { Bytes } from "../io/bytes.js";
import {
  defaultOpenParams,
  encodeHeartbeat,
  encodeHeartbeatAck,
  type NegotiationParams
} from "../protocol/codec/control.js";
import {
  FrameDecoder,
  FrameEncoder,
  MessageFragmenter,
  MessageReassembler
} from "../protocol/codec/frame.js";
import { decodeJsonRpc, encodeJsonRpc } from "../protocol/codec/jsonRpc.js";
import { PayloadDecoder } from "../protocol/codec/payload.js";
import { encodeStream } from "../protocol/codec/stream.js";
import { PayloadType, RpcEncoding } from "../protocol/generated/axtp_ids_generated.js";
import type { Message, RpcPayload, StreamPayload } from "../protocol/model.js";
import type {
  CloseReason,
  ITransport,
  PhysicalRole,
  TransportCapabilities,
  TransportFactory
} from "../transport/transport.js";
import { CloseCode } from "../transport/transport.js";
import type { AxtpError } from "../types/error.js";
import { EventStream } from "../types/events.js";
import { ControlSession, type NegotiatedLink } from "./controlSession.js";
import { Heartbeat } from "./heartbeat.js";
import { nextDelay, resolvePolicy, type ReconnectInfo, type ReconnectPolicy } from "./reconnect.js";

export interface ConnectionOptions {
  heartbeatIntervalMs?: number;
  heartbeatTimeoutMs?: number;
  negotiationParams?: NegotiationParams;
  maxFrameSize?: number;
  /** 传输重连策略（仅 client 场景，需 transportFactory）。 */
  reconnect?: ReconnectPolicy;
}

/**
 * ReconnectCoordinator：传输重连编排（退避 + transportFactory + 链路重建触发）。
 * 只管传输层：触发 onAttempt（Connection 在此重建 pipeline+链路），成功/失败回调。
 */
class ReconnectCoordinator {
  private attempts = 0;
  private timer: ReturnType<typeof setTimeout> | undefined;
  private active = false;

  constructor(
    private readonly policy: ReturnType<typeof resolvePolicy>,
    private readonly transportFactory: TransportFactory,
    private readonly onReconnected: (transport: ITransport) => void,
    private readonly onSuccess: () => void,
    private readonly onFailed: () => void
  ) {}

  start(): void {
    if (this.active) return;
    this.active = true;
    this.schedule();
  }

  stop(): void {
    this.active = false;
    if (this.timer !== undefined) {
      clearTimeout(this.timer);
      this.timer = undefined;
    }
  }

  get attemptCount(): number {
    return this.attempts;
  }

  private schedule(): void {
    if (!this.active) return;
    if (this.attempts >= this.policy.maxAttempts) {
      this.active = false;
      this.onFailed();
      return;
    }
    const delay = nextDelay(this.policy, this.attempts);
    this.attempts += 1;
    this.timer = setTimeout(() => {
      this.attempt().catch(() => {
        if (this.active) this.schedule();
      });
    }, delay);
  }

  private async attempt(): Promise<void> {
    const transport = await this.transportFactory();
    if (!this.active) return;
    this.onReconnected(transport);
  }

  /** 重连成功后调用（Connection 重建 pipeline 后调此重置退避）。 */
  notifySuccess(): void {
    if (this.policy.resetBackoffOnSuccess) {
      this.attempts = 0;
    }
  }
}

export class Connection {
  readonly onClose = new EventStream<CloseReason>();
  readonly onError = new EventStream<AxtpError>();
  readonly onPayload = new EventStream<RpcPayload>();
  readonly onStream = new EventStream<StreamPayload>();
  readonly onLinkReady = new EventStream<void>();
  readonly onReconnect = new EventStream<ReconnectInfo>();
  readonly onReconnectFailed = new EventStream<void>();

  private transport: ITransport;
  private readonly capabilities: TransportCapabilities;
  private readonly physicalRole: PhysicalRole;
  private readonly options: ConnectionOptions;
  private readonly transportFactory: TransportFactory | undefined;

  private controlSession: ControlSession | undefined;
  private heartbeat: Heartbeat | undefined;
  private reconnectCoordinator: ReconnectCoordinator | undefined;

  // framed-binary 编解码流水线（WS 模式不使用）。重连时重建。
  private frameDecoder!: FrameDecoder;
  private fragmenter: MessageFragmenter;
  private readonly frameEncoder = new FrameEncoder();

  private started = false;
  private closed = false;
  private linkReadyFired = false;
  private reconnecting = false;
  /** start 前的消息缓冲（防止 transport 在 Connection 构造到 start 间投递的消息丢失）。 */
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
    this.fragmenter = new MessageFragmenter(options.maxFrameSize ?? 4096);

    // 重连协调器（仅当有 transportFactory 且 policy.enabled）
    const policy = resolvePolicy(options.reconnect);
    if (policy.enabled && transportFactory !== undefined) {
      this.reconnectCoordinator = new ReconnectCoordinator(
        policy,
        transportFactory,
        (newTransport) => this.handleReconnected(newTransport),
        () => this.handleReconnectSuccess(),
        () => this.handleReconnectFailed()
      );
    }

    this.capabilities = transport.capabilities;
    this.transport = transport;
    this.attachTransport(transport);
  }

  /**
   * 绑定一条 transport：建立 codec pipeline（framed）+ 订阅事件 + attach 缓冲。
   * 构造和重连都调此方法。
   */
  private attachTransport(transport: ITransport): void {
    if (this.capabilities.supportsControl) {
      this.controlSession = new ControlSession(
        this.physicalRole,
        {
          onSendBytes: (body) => this.sendFramedMessage(PayloadType.Control, body),
          onLinkReady: (neg) => this.onNegotiatedLinkReady(neg),
          onHeartbeat: (controlId) =>
            this.sendFramedMessage(PayloadType.Control, encodeHeartbeatAck(controlId)),
          onHeartbeatAck: () => this.heartbeat?.reset(),
          onClosing: () => this.close(CloseCode.Normal, "remote close")
        },
        this.options.negotiationParams ??
          defaultOpenParams(
            this.options.maxFrameSize ?? 4096,
            this.options.heartbeatIntervalMs ?? 1000
          )
      );

      const payloadDecoder = new PayloadDecoder({
        onControl: (body) => {
          const cs = this.controlSession;
          if (cs !== undefined) cs.handleControlBody(body);
        },
        onRpc: (p) => this.onPayload.emit(p),
        onStream: (s) => this.onStream.emit(s)
      });
      const reassembler = new MessageReassembler({
        onMessage: (m) => payloadDecoder.onMessage(m.payloadType, m.body)
      });
      this.frameDecoder = new FrameDecoder(reassembler, this.options.maxFrameSize ?? 4096);
    }

    // 订阅 transport 事件（start 前缓冲）。
    transport.onMessage.subscribe((bytes) => {
      if (this.closed) return;
      if (!this.started) {
        this.pendingBytes.push(bytes);
        return;
      }
      this.onTransportBytes(bytes);
    });
    transport.onClose.subscribe((reason) => this.handleTransportClose(reason, transport));
    transport.onError.subscribe((err) => this.onError.emit(err));

    transport.attach?.();
  }

  /** 启动处理（Session 在回调赋值后调用）。framed client 同时发 OPEN。flush 缓冲消息。 */
  start(): void {
    if (this.started) return;
    this.started = true;
    this.flushPendingAndStartLink();
  }

  /** flush 缓冲 + 启动链路（构造 start 和重连都用）。 */
  private flushPendingAndStartLink(): void {
    const buffered = this.pendingBytes.splice(0);
    for (const bytes of buffered) this.onTransportBytes(bytes);

    if (this.capabilities.supportsControl) {
      const cs = this.controlSession;
      if (this.physicalRole === "client" && cs !== undefined) cs.sendOpen();
    } else {
      this.fireLinkReady();
      this.startHeartbeat(this.options.heartbeatIntervalMs ?? 30000);
    }
  }

  /** 发送 RpcPayload（应用层，Session 调用）。 */
  sendRpc(payload: RpcPayload): void {
    if (this.closed || this.reconnecting) return;
    const jsonBytes = encodeJsonRpc(payload);
    if (this.capabilities.supportsControl) {
      this.sendFramedMessage(PayloadType.Rpc, this.wrapRpcEncoding(jsonBytes));
    } else {
      this.transport.send(jsonBytes);
    }
  }

  /** 发送 StreamPayload（framed only）。 */
  sendStream(payload: StreamPayload): void {
    if (this.closed || this.reconnecting || !this.capabilities.supportsControl) return;
    this.sendFramedMessage(PayloadType.Stream, encodeStream(payload));
  }

  /** 主动关闭（用户触发，不再重连）。 */
  close(code: CloseCode = CloseCode.Normal, reason = "local close"): void {
    if (this.closed) return;
    this.closed = true;
    this.reconnectCoordinator?.stop();
    this.heartbeat?.stop();
    if (this.capabilities.supportsControl && this.controlSession?.isOpen) {
      this.controlSession.sendClose();
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

  // ===== 重连（传输层）=====

  private handleTransportClose(reason: CloseReason, _closedTransport: ITransport): void {
    if (this.closed) return;
    // 停心跳、清理当前 transport 绑定的链路状态
    this.heartbeat?.stop();
    this.heartbeat = undefined;
    this.linkReadyFired = false;

    // 若启用重连，触发重连编排（不 emit onClose，等重连结果）
    if (this.reconnectCoordinator !== undefined && !this.reconnecting) {
      this.reconnecting = true;
      this.reconnectCoordinator.start();
      return;
    }

    // 不重连：直接 emit onClose
    if (!this.reconnecting) {
      this.closed = true;
      this.onClose.emit(reason);
      this.cleanupStreams();
    }
  }

  /** ReconnectCoordinator 回调：拿到新 transport，重建 pipeline + 链路。 */
  private handleReconnected(newTransport: ITransport): void {
    this.transport = newTransport;
    // 重置 start 状态以重新走 flushPendingAndStartLink
    this.pendingBytes = [];
    this.attachTransport(newTransport);
    this.flushPendingAndStartLink();
  }

  /** 链路重建成功（fireLinkReady 触发后，Session 会收到 onLinkReady 重建会话）。 */
  private handleReconnectSuccess(): void {
    this.reconnectCoordinator?.notifySuccess();
  }

  /** 重连耗尽：emit onReconnectFailed + onClose。 */
  private handleReconnectFailed(): void {
    this.reconnecting = false;
    this.closed = true;
    this.onReconnectFailed.emit(undefined);
    this.onClose.emit({ code: CloseCode.Reconnect, reason: "reconnect failed", remote: false });
    this.cleanupStreams();
  }

  // ===== 内部 =====

  private onTransportBytes(bytes: Bytes): void {
    if (this.closed) return;
    if (this.capabilities.supportsControl) {
      this.frameDecoder.onBytes(bytes);
    } else {
      const payload = decodeJsonRpc(bytes);
      if (payload !== undefined) this.onPayload.emit(payload);
    }
  }

  private onNegotiatedLinkReady(neg: NegotiatedLink): void {
    if (!neg.accepted) return;
    this.fragmenter.setMaxFrameSize(neg.maxFrameSize);
    this.fireLinkReady();
    this.startHeartbeat(neg.heartbeatIntervalMs);
    // 重连场景：链路 ready 表示传输重连成功
    if (this.reconnecting) {
      this.reconnecting = false;
      const attempt = this.reconnectCoordinator?.attemptCount ?? 0;
      this.onReconnect.emit({ attempt, totalDowntimeMs: 0 });
      this.handleReconnectSuccess();
    }
  }

  private fireLinkReady(): void {
    if (this.linkReadyFired) return;
    this.linkReadyFired = true;
    this.onLinkReady.emit(undefined);
    // WS 模式：fireLinkReady 后也要标记重连成功（framed 走 onNegotiatedLinkReady）
    if (this.reconnecting && !this.capabilities.supportsControl) {
      this.reconnecting = false;
      const attempt = this.reconnectCoordinator?.attemptCount ?? 0;
      this.onReconnect.emit({ attempt, totalDowntimeMs: 0 });
      this.handleReconnectSuccess();
    }
  }

  private startHeartbeat(negotiatedIntervalMs: number): void {
    const interval = negotiatedIntervalMs || this.options.heartbeatIntervalMs || 30000;
    const timeout = this.options.heartbeatTimeoutMs ?? Math.max(interval * 2, 10000);

    if (this.capabilities.supportsControl) {
      // framed: CONTROL Heartbeat/Ack
      this.heartbeat = new Heartbeat({
        intervalMs: interval,
        timeoutMs: timeout,
        onTick: () => this.sendFramedMessage(PayloadType.Control, encodeHeartbeat(1)),
        onTimeout: () => this.close(CloseCode.HeartbeatTimeout, "heartbeat timeout")
      });
    } else if (this.capabilities.supportsKeepalive) {
      // WS: 原生 keepalive（ITransport.sendKeepalive/onKeepaliveAck）
      const t = this.transport;
      this.heartbeat = new Heartbeat({
        intervalMs: interval,
        timeoutMs: timeout,
        onTick: () => t.sendKeepalive?.(),
        onTimeout: () => this.close(CloseCode.HeartbeatTimeout, "heartbeat timeout")
      });
      t.onKeepaliveAck?.(() => this.heartbeat?.reset());
    }
    this.heartbeat?.start();
  }

  private sendFramedMessage(payloadType: PayloadType, body: Uint8Array): void {
    if (this.closed || this.reconnecting || !this.capabilities.supportsControl) return;
    const message: Message = { messageId: 0, payloadType, body };
    for (const frame of this.fragmenter.fragment(message)) {
      this.transport.send(this.frameEncoder.encode(frame));
    }
  }

  private wrapRpcEncoding(jsonBody: Uint8Array): Uint8Array {
    const out = new Uint8Array(1 + jsonBody.length);
    out[0] = RpcEncoding.Json;
    out.set(jsonBody, 1);
    return out;
  }

  private cleanupStreams(): void {
    this.onPayload.close();
    this.onStream.close();
    this.onLinkReady.close();
    this.onError.close();
    this.onReconnect.close();
    this.onReconnectFailed.close();
  }
}
