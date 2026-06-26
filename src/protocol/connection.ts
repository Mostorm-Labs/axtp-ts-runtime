// Connection：传输连接 + 链路层编排（连接语义，不导出）。
// 持有 transport + codec + ControlSession(framed) + Heartbeat。
// 解码后把 RpcPayload/StreamPayload 上交给 Session（onPayload/onStream/onLinkReady/onClose）。
// 不做应用语义（sid/handler/pending call 都归 Session）。
//
// start() 延迟 wire：构造时不订阅 transport，由 Session 在回调赋值完成后调 start()——
// 避免 WS 首条 Hello 在 onPayload 赋值前到达被丢弃。
//
// 心跳：framed 在链路 ready（FRAMING_READY/ACCEPT）启动，用 CONTROL Heartbeat/Ack；
//       WS 用原生 ping/pong（hasNativePing 鸭子类型探测，不污染 ITransport 接口）。

import type { Bytes } from "../io/bytes.js";
import { PayloadType, RpcEncoding } from "../protocol/generated/axtp_ids_generated.js";
import type { CloseReason, ITransport, TransportCapabilities } from "../transport/transport.js";
import { CloseCode } from "../transport/transport.js";
import { hasNativePing } from "../transport/ws/nodeWsTransport.js";
import type { AxtpError } from "../types/error.js";
import { EventStream } from "../types/events.js";
import {
  defaultOpenParams,
  encodeHeartbeat,
  encodeHeartbeatAck,
  type NegotiationParams
} from "./codec/control.js";
import {
  FrameDecoder,
  FrameEncoder,
  MessageFragmenter,
  MessageReassembler
} from "./codec/frame.js";
import { decodeJsonRpc, encodeJsonRpc } from "./codec/jsonRpc.js";
import { PayloadDecoder } from "./codec/payload.js";
import { encodeStream } from "./codec/stream.js";
import { ControlSession, type NegotiatedLink } from "./engine/controlSession.js";
import { Heartbeat } from "./engine/heartbeat.js";
import type { Message, RpcPayload, StreamPayload } from "./model.js";

export interface ConnectionOptions {
  heartbeatIntervalMs?: number;
  heartbeatTimeoutMs?: number;
  negotiationParams?: NegotiationParams;
  maxFrameSize?: number;
}

export class Connection {
  readonly onClose = new EventStream<CloseReason>();
  readonly onError = new EventStream<AxtpError>();
  readonly onPayload = new EventStream<RpcPayload>();
  readonly onStream = new EventStream<StreamPayload>();
  readonly onLinkReady = new EventStream<void>();

  private readonly transport: ITransport;
  private readonly capabilities: TransportCapabilities;
  private readonly role: "server" | "client";
  private readonly options: ConnectionOptions;

  private readonly controlSession: ControlSession | undefined;
  private heartbeat: Heartbeat | undefined;

  // framed-binary 编解码流水线（WS 模式不使用）。
  private frameDecoder!: FrameDecoder;
  private readonly fragmenter: MessageFragmenter;
  private readonly frameEncoder = new FrameEncoder();

  private started = false;
  private closed = false;
  private linkReadyFired = false;
  /** start 前的消息缓冲（防止 transport 在 Connection 构造到 start 间投递的消息丢失）。 */
  private readonly pendingBytes: Bytes[] = [];

  constructor(role: "server" | "client", transport: ITransport, options: ConnectionOptions = {}) {
    this.role = role;
    this.transport = transport;
    this.capabilities = transport.capabilities;
    this.options = options;

    const maxFrameSize = options.maxFrameSize ?? 4096;
    this.fragmenter = new MessageFragmenter(maxFrameSize);

    if (this.capabilities.wireMode === "framed-binary") {
      this.controlSession = new ControlSession(
        role,
        {
          onSendBytes: (body) => this.sendFramedMessage(PayloadType.Control, body),
          onLinkReady: (neg) => this.onNegotiatedLinkReady(neg),
          onHeartbeat: (controlId) =>
            this.sendFramedMessage(PayloadType.Control, encodeHeartbeatAck(controlId)),
          onHeartbeatAck: () => this.heartbeat?.reset(),
          onClosing: () => this.close(CloseCode.Normal, "remote close")
        },
        options.negotiationParams ??
          defaultOpenParams(maxFrameSize, options.heartbeatIntervalMs ?? 1000)
      );

      const payloadDecoder = new PayloadDecoder({
        onControl: (body) => this.controlSession!.handleControlBody(body),
        onRpc: (p) => this.onPayload.emit(p),
        onStream: (s) => this.onStream.emit(s)
      });
      const reassembler = new MessageReassembler({
        onMessage: (m) => payloadDecoder.onMessage(m.payloadType, m.body)
      });
      this.frameDecoder = new FrameDecoder(reassembler, maxFrameSize);
    }

    // 构造时立即订阅 transport.onMessage（避免在 start 前消息丢失）。
    // start 前缓冲，start 后 flush + 直接处理。
    this.transport.onMessage.subscribe((bytes) => {
      if (this.closed) return;
      if (!this.started) {
        this.pendingBytes.push(bytes);
        return;
      }
      this.onTransportBytes(bytes);
    });
    this.transport.onClose.subscribe((reason) => this.handleTransportClose(reason));
    this.transport.onError.subscribe((err) => this.onError.emit(err));

    // 真实 transport（TCP/WS）在 Connection 构造前可能已收到消息（缓冲在 transport 内）。
    // attach 让 transport flush 缓冲到 onMessage，进入上方 pendingBytes（等 start 处理）。
    this.transport.attach?.();
  }

  /** 启动处理（Session 在回调赋值后调用）。framed client 同时发 OPEN。flush 缓冲消息。 */
  start(): void {
    if (this.started) return;
    this.started = true;
    // flush start 前缓冲的消息
    const buffered = this.pendingBytes.splice(0);
    for (const bytes of buffered) this.onTransportBytes(bytes);

    if (this.capabilities.wireMode === "framed-binary") {
      if (this.role === "client") this.controlSession!.sendOpen();
      // server 等待对端 OPEN
    } else {
      // unframed-json：无 CONTROL 链路层，连接建立即 link ready。
      this.fireLinkReady();
    }
  }

  /** 发送 RpcPayload（应用层，Session 调用）。 */
  sendRpc(payload: RpcPayload): void {
    if (this.closed) return;
    const jsonBytes = encodeJsonRpc(payload);
    if (this.capabilities.wireMode === "framed-binary") {
      this.sendFramedMessage(PayloadType.Rpc, this.wrapRpcEncoding(jsonBytes));
    } else {
      this.transport.send(jsonBytes);
    }
  }

  /** 发送 StreamPayload（framed only）。 */
  sendStream(payload: StreamPayload): void {
    if (this.closed || this.capabilities.wireMode !== "framed-binary") return;
    this.sendFramedMessage(PayloadType.Stream, encodeStream(payload));
  }

  /** 主动关闭。 */
  close(code: CloseCode = CloseCode.Normal, reason = "local close"): void {
    if (this.closed) return;
    this.closed = true;
    this.heartbeat?.stop();
    if (this.capabilities.wireMode === "framed-binary" && this.controlSession?.isOpen) {
      this.controlSession.sendClose();
    }
    this.transport.close();
    this.onClose.emit({ code, reason, remote: false });
    this.cleanupStreams();
  }

  get isClosed(): boolean {
    return this.closed;
  }

  // ===== 内部 =====

  private onTransportBytes(bytes: Bytes): void {
    if (this.closed) return;
    if (this.capabilities.wireMode === "framed-binary") {
      this.frameDecoder.onBytes(bytes);
    } else {
      // unframed-json：直接 JSON 解码
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

    if (this.capabilities.wireMode === "framed-binary") {
      // framed: CONTROL Heartbeat/Ack
      this.heartbeat = new Heartbeat({
        intervalMs: interval,
        timeoutMs: timeout,
        onTick: () => this.sendFramedMessage(PayloadType.Control, encodeHeartbeat(1)),
        onTimeout: () => this.close(CloseCode.HeartbeatTimeout, "heartbeat timeout")
      });
    } else if (hasNativePing(this.transport)) {
      // WS: 原生 ping/pong（闭包注入，不污染 ITransport 接口）
      const ws = this.transport;
      this.heartbeat = new Heartbeat({
        intervalMs: interval,
        timeoutMs: timeout,
        onTick: () => ws.ping(),
        onTimeout: () => this.close(CloseCode.HeartbeatTimeout, "heartbeat timeout")
      });
      ws.onPong(() => this.heartbeat?.reset());
    }
    this.heartbeat?.start();
  }

  private sendFramedMessage(payloadType: PayloadType, body: Uint8Array): void {
    if (this.closed || this.capabilities.wireMode !== "framed-binary") return;
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

  private handleTransportClose(reason: CloseReason): void {
    if (this.closed) return;
    this.closed = true;
    this.heartbeat?.stop();
    this.onClose.emit(reason);
    this.cleanupStreams();
  }

  private cleanupStreams(): void {
    this.onPayload.close();
    this.onStream.close();
    this.onLinkReady.close();
    this.onError.close();
  }
}
