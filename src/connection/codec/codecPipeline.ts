// CodecPipeline：framed-binary 编解码流水线（单一职责：帧解码/重组/payload 分发）。
// 从 Connection 拆出，Connection 只持有它。
// 负责：FrameDecoder → MessageReassembler → PayloadDecoder 分发链。
// 重连时由 Connection 重新创建实例（确保完全重置内部状态）。

import type { Bytes } from "../../io/bytes.js";
import {
  defaultOpenParams,
  encodeHeartbeat,
  encodeHeartbeatAck,
  type NegotiationParams
} from "../../protocol/codec/control.js";
import { FrameDecoder, FrameEncoder, MessageFragmenter, MessageReassembler } from "../../protocol/codec/frame.js";
import { PayloadDecoder } from "../../protocol/codec/payload.js";
import { encodeStream } from "../../protocol/codec/stream.js";
import { PayloadType } from "../../protocol/generated/axtp_ids_generated.js";
import type { Message, RpcPayload, StreamPayload } from "../../protocol/model.js";
import type { ITransport, PhysicalRole } from "../../transport/transport.js";
import { ControlSession, type NegotiatedLink } from "./controlSession.js";

/** CodecPipeline 事件回调（上交给 Connection）。 */
export interface CodecPipelineCallbacks {
  onRpc(payload: RpcPayload): void;
  onStream(payload: StreamPayload): void;
  onControlHeartbeat(controlId: number): void;
  onControlHeartbeatAck(controlId: number): void;
  onControlClosing(): void;
  onControlRejected(statusCode: number): void;
  onLinkReady(neg: NegotiatedLink): void;
}

/**
 * framed-binary 编解码流水线。
 * 封装 ControlSession + FrameDecoder/Reassembler/PayloadDecoder + Fragmenter/Encoder。
 * WS 模式不创建此对象（Connection 直接用 transport.send/onMessage）。
 */
export class CodecPipeline {
  readonly controlSession: ControlSession;
  private readonly frameDecoder: FrameDecoder;
  private readonly fragmenter: MessageFragmenter;
  private readonly frameEncoder = new FrameEncoder;
  private readonly transport: ITransport;
  private readonly callbacks: CodecPipelineCallbacks;

  constructor(
    physicalRole: PhysicalRole,
    transport: ITransport,
    options: { maxFrameSize: number; heartbeatIntervalMs: number; negotiationParams?: NegotiationParams },
    callbacks: CodecPipelineCallbacks
  ) {
    this.transport = transport;
    this.callbacks = callbacks;

    this.controlSession = new ControlSession(
      physicalRole,
      {
        onSendBytes: (body) => this.send(PayloadType.Control, body),
        onLinkReady: (neg) => callbacks.onLinkReady(neg),
        onHeartbeat: (controlId) => callbacks.onControlHeartbeat(controlId),
        onHeartbeatAck: (controlId) => callbacks.onControlHeartbeatAck(controlId),
        onClosing: () => callbacks.onControlClosing(),
        onRejected: (statusCode) => callbacks.onControlRejected(statusCode)
      },
      options.negotiationParams ??
        defaultOpenParams(options.maxFrameSize, options.heartbeatIntervalMs)
    );

    const payloadDecoder = new PayloadDecoder({
      onControl: (body) => this.controlSession.handleControlBody(body),
      onRpc: (p) => callbacks.onRpc(p),
      onStream: (s) => callbacks.onStream(s)
    });
    const reassembler = new MessageReassembler({
      onMessage: (m) => payloadDecoder.onMessage(m.payloadType, m.body)
    });
    this.frameDecoder = new FrameDecoder(reassembler, options.maxFrameSize);
    this.fragmenter = new MessageFragmenter(options.maxFrameSize);
  }

  /** 入站字节 → frame decode → reassemble → payload decode → 分发。 */
  onBytes(bytes: Bytes): void {
    this.frameDecoder.onBytes(bytes);
  }

  /** 发送 RPC payload（framed + rpcEncoding 前缀）。 */
  sendRpc(jsonBytes: Uint8Array): void {
    const wrapped = new Uint8Array(1 + jsonBytes.length);
    wrapped[0] = 0x01; // RpcEncoding.Json
    wrapped.set(jsonBytes, 1);
    this.send(PayloadType.Rpc, wrapped);
  }

  /** 发送 STREAM payload。 */
  sendStreamPayload(payload: StreamPayload): void {
    this.send(PayloadType.Stream, encodeStream(payload));
  }

  /** 发送 CONTROL Heartbeat。 */
  sendHeartbeat(controlId: number): void {
    this.send(PayloadType.Control, encodeHeartbeat(controlId));
  }

  /** 发送 CONTROL HeartbeatAck。 */
  sendHeartbeatAck(controlId: number): void {
    this.send(PayloadType.Control, encodeHeartbeatAck(controlId));
  }

  /** 更新协商后的 maxFrameSize。 */
  setMaxFrameSize(size: number): void {
    this.fragmenter.setMaxFrameSize(size);
  }

  /** 物理客户端发 OPEN。 */
  sendOpen(): void {
    this.controlSession.sendOpen();
  }

  /** 发送 CONTROL CLOSE。 */
  sendClose(): void {
    this.controlSession.sendClose();
  }

  get controlSessionIsOpen(): boolean {
    return this.controlSession.isOpen;
  }

  /** 内部：分片 + 成帧 + transport.send。 */
  private send(payloadType: PayloadType, body: Uint8Array): void {
    const message: Message = { messageId: 0, payloadType, body };
    for (const frame of this.fragmenter.fragment(message)) {
      this.transport.send(this.frameEncoder.encode(frame));
    }
  }
}
