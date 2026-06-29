// Connection：传输连接 + 链路层编排 + 传输重连（连接语义，不导出）。
// 持有 transport + codec + ControlSession(framed) + Heartbeat + ReconnectCoordinator。
// 解码后把 RpcPayload/StreamPayload 上交给 Session（onPayload/onStream/onLinkReady/onClose/onReconnect）。
// 不做应用语义（sid/handler/pending call 都归 Session）。
//
// 重连（传输层）：transport.onClose → ReconnectCoordinator 退避 → transportFactory() 重建传输
//   → emit onReconnect（Session reset 握手）→ attachTransport + 启动链路。
//   Connection 只管传输层重连，不碰 Session/handler。
//
// 心跳：framed 在链路 ready 启动，用 CONTROL Heartbeat/Ack；
//       WS 用原生 keepalive（ITransport.sendKeepalive/onKeepaliveAck）。

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
import { AxtpError, ErrorCode } from "../types/error.js";
import { EventStream } from "../types/events.js";
import { ControlSession, type NegotiatedLink } from "./controlSession.js";
import { Heartbeat } from "./heartbeat.js";
import { resolvePolicy, type ReconnectInfo, type ReconnectPolicy } from "./reconnect.js";
import { ReconnectCoordinator } from "./reconnectCoordinator.js";

export interface ConnectionOptions {
  heartbeatIntervalMs?: number;
  heartbeatTimeoutMs?: number;
  negotiationParams?: NegotiationParams;
  maxFrameSize?: number;
  /** 传输重连策略（仅 client 场景，需 transportFactory）。 */
  reconnect?: ReconnectPolicy;
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
  private frameDecoder?: FrameDecoder;
  private fragmenter: MessageFragmenter;
  private readonly frameEncoder = new FrameEncoder();

  /** transport 事件订阅句柄（detach 时取消） */
  private transportUnsubs: Array<() => void> = [];

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
      this.reconnectCoordinator = ReconnectCoordinator.fromPolicy(
        options.reconnect,
        transportFactory,
        {
          onReconnected: (newTransport) => this.handleReconnected(newTransport),
          onSuccess: () => this.handleReconnectSuccess(),
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
   * 绑定一条 transport：建立 codec pipeline（framed）+ 订阅事件 + attach 缓冲。
   * 构造和重连都调此方法。重连时先 detach 旧 transport 订阅。
   */
  private attachTransport(transport: ITransport): void {
    // detach 旧 transport 订阅（M6：防止旧 transport 事件污染）
    for (const unsub of this.transportUnsubs) unsub();
    this.transportUnsubs = [];

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

  /** 启动处理（Session 在回调赋值后调用）。framed client 同时发 OPEN。flush 缓冲消息。 */
  start(): void {
    if (this.started) return;
    this.started = true;
    this.flushPendingAndStartLink();
  }

  /** flush 缓冲 + 启动链路（首次 start 用）。 */
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

  /**
   * 发送 RpcPayload（应用层，Session 调用）。
   * H2：重连中抛错而非静默丢弃，让调用方知道消息未发出。
   */
  sendRpc(payload: RpcPayload): void {
    if (this.closed) return;
    if (this.reconnecting) {
      throw new AxtpError(ErrorCode.TransportDisconnected, "connection reconnecting");
    }
    const jsonBytes = encodeJsonRpc(payload);
    if (this.capabilities.supportsControl) {
      this.sendFramedMessage(PayloadType.Rpc, this.wrapRpcEncoding(jsonBytes));
    } else {
      this.transport.send(jsonBytes);
    }
  }

  /**
   * 发送 StreamPayload（framed only）。
   * H2：重连中抛错而非静默丢弃。
   */
  sendStream(payload: StreamPayload): void {
    if (this.closed) return;
    if (this.reconnecting) {
      throw new AxtpError(ErrorCode.TransportDisconnected, "connection reconnecting");
    }
    if (!this.capabilities.supportsControl) return;
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

  private handleTransportClose(reason: CloseReason): void {
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

  /**
   * ReconnectCoordinator 回调：拿到新 transport，重建 pipeline + 链路。
   * H1：先 emit onReconnect（让 Session reset 握手状态），再 attachTransport + 启动链路。
   * 这样 Session.handleReconnect 在 onLinkReady 之前执行，握手状态机正确 reset。
   */
  private handleReconnected(newTransport: ITransport): void {
    this.transport = newTransport;
    this.linkReadyFired = false;
    this.pendingBytes = [];

    // 先 emit onReconnect，让 Session reset 握手（必须在 fireLinkReady/onLinkReady 之前）
    const attempt = this.reconnectCoordinator?.attemptCount ?? 0;
    this.onReconnect.emit({ attempt });

    // attachTransport 会 detach 旧 transport + 订阅新 transport + 建 codec pipeline
    this.attachTransport(newTransport);

    // 启动链路（不发 OPEN 走 flushPending，直接启动）
    if (this.capabilities.supportsControl) {
      const cs = this.controlSession;
      if (this.physicalRole === "client" && cs !== undefined) cs.sendOpen();
    } else {
      this.fireLinkReady();
      this.startHeartbeat(this.options.heartbeatIntervalMs ?? 30000);
    }

    // 标记重连完成
    this.reconnecting = false;
    this.handleReconnectSuccess();
  }

  /** 链路重建成功。 */
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
      this.frameDecoder?.onBytes(bytes);
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
  }

  private fireLinkReady(): void {
    if (this.linkReadyFired) return;
    this.linkReadyFired = true;
    this.onLinkReady.emit(undefined);
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
    } else {
      // 两种心跳能力都不支持：通过 onError 上报（库代码不应用 console）
      this.onError.emit(
        new AxtpError(
          ErrorCode.NotSupported,
          "Transport supports neither CONTROL heartbeat nor native keepalive; connection may hang silently"
        )
      );
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
    // L：onClose 最后关闭（emit 完再 close）
    this.onClose.close();
  }
}
