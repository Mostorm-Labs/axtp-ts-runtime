// FramedLink：framed-binary 链路（TCP），Link 的 framed 实现。
// 完整内聚 framed 链路的全部职责（吸收原 CodecPipeline 的胶合层）：
//   - 入站：FrameDecoder（magic resync + 8 项校验）→ MessageReassembler（分片重组）→ PayloadDecoder（分发）
//   - CONTROL 状态机：ControlSession（OPEN/ACCEPT/CLOSE/CLOSE_ACK 协商，TLV 校验）
//   - 出站：MessageFragmenter（按 maxFrameSize 分片）→ FrameEncoder（12B header + payload + 2B CRC）
//   - 心跳：Heartbeat（CONTROL Heartbeat/Ack）
// Connection 只看到 RpcMessage/StreamPayload 与链路事件，wire 细节全部封装于此。
//
// 构造无副作用（不发字节）；server 角色等 OPEN，startOpen() 为 no-op。

import type { Bytes } from "../../io/bytes.js";
import {
  defaultOpenParams,
  encodeHeartbeat,
  encodeHeartbeatAck
} from "../../protocol/codec/control.js";
import {
  FrameDecoder,
  FrameEncoder,
  MessageFragmenter,
  MessageReassembler
} from "../../protocol/codec/frame.js";
import { encodeJsonRpc } from "../../protocol/codec/jsonRpc.js";
import { decodeRpcPayload } from "../../protocol/codec/payload.js";
import { decodeStream, encodeStream } from "../../protocol/codec/stream.js";
import type { Message, RpcMessage, StreamPayload } from "../../protocol/model.js";
import { PayloadType, RpcEncoding } from "../../protocol/model.js";
import type { ITransport, PhysicalRole } from "../../transport/contract.js";
import { AxtpError, ErrorCode } from "../../types/error.js";
import { EventStream } from "../../types/events.js";
import { Heartbeat } from "../heartbeat.js";
import { ControlSession } from "./controlSession.js";
import type { Link } from "./link.js";

export interface FramedLinkOptions {
  /** 总帧大小上限（含 12B header + 2B CRC），用于 OPEN 提议与入站校验。 */
  readonly maxFrameSize: number;
  /** OPEN 提议的心跳间隔（协商后被对端值覆盖）。 */
  readonly heartbeatIntervalMs: number;
  /** 心跳超时；缺省 max(interval*2, 10000)。 */
  readonly heartbeatTimeoutMs?: number;
}

export class FramedLink implements Link {
  readonly onPayload = new EventStream<RpcMessage>();
  readonly onStream = new EventStream<StreamPayload>();
  readonly onLinkReady = new EventStream<void>();
  readonly onClosing = new EventStream<void>();
  readonly onOpenRejected = new EventStream<number>();
  readonly onHeartbeatTimeout = new EventStream<void>();
  readonly onError = new EventStream<AxtpError>();

  private heartbeat: Heartbeat | undefined;
  /** 协商后的心跳间隔（start() 使用）；link ready 前为 undefined。 */
  private negotiatedIntervalMs: number | undefined;
  /** 协商出的 RPC 编码（sendRpc 使用）；link ready 前为 undefined（回落 JSON）。 */
  private negotiatedRpcEncoding: RpcEncoding | undefined;

  private readonly transport: ITransport;
  private readonly controlSession: ControlSession;
  private readonly frameDecoder: FrameDecoder;
  private readonly fragmenter: MessageFragmenter;
  private readonly frameEncoder = new FrameEncoder();
  private readonly fallbackIntervalMs: number;
  private readonly heartbeatTimeoutMs: number | undefined;

  constructor(
    private readonly physicalRole: PhysicalRole,
    transport: ITransport,
    options: FramedLinkOptions
  ) {
    this.transport = transport;
    this.fallbackIntervalMs = options.heartbeatIntervalMs;
    this.heartbeatTimeoutMs = options.heartbeatTimeoutMs;

    this.controlSession = new ControlSession(
      physicalRole,
      {
        onSendBytes: (body) => this.send(PayloadType.Control, body),
        onLinkReady: (neg) => {
          if (!neg.accepted) return;
          this.frameDecoder.setMaxFrameSize(neg.maxFrameSize);
          this.fragmenter.setMaxFrameSize(neg.maxFrameSize);
          this.negotiatedIntervalMs = neg.heartbeatIntervalMs;
          this.negotiatedRpcEncoding = neg.selectedRpcEncoding;
          this.onLinkReady.emit(undefined);
        },
        onHeartbeat: (cid) => this.sendHeartbeatAck(cid),
        onHeartbeatAck: () => this.heartbeat?.reset(),
        onClosing: () => this.onClosing.emit(undefined),
        onOpenRejected: (sc) => this.onOpenRejected.emit(sc),
        onError: (err) => this.onError.emit(err)
      },
      defaultOpenParams(options.maxFrameSize, options.heartbeatIntervalMs)
    );

    // 入站 payload 分发（inline，去掉仅此一处使用的 PayloadDecoder 类 + PayloadSink 接口）。
    const dispatchPayload = (payloadType: PayloadType, body: Uint8Array): void => {
      switch (payloadType) {
        case PayloadType.Control:
          this.controlSession.handleControlBody(body);
          break;
        case PayloadType.Rpc:
          this.decodeRpc(body);
          break;
        case PayloadType.Stream: {
          const sp = decodeStream(body);
          if (sp !== undefined) this.onStream.emit(sp);
          break;
        }
      }
    };
    const reassembler = new MessageReassembler(
      {
        onMessage: (m) => dispatchPayload(m.payloadType, m.body),
        onError: (err) => this.onError.emit(err)
      },
      options.maxFrameSize * 256
    );
    this.frameDecoder = new FrameDecoder(
      { onFrame: (f) => reassembler.onFrame(f), onError: (err) => this.onError.emit(err) },
      options.maxFrameSize
    );
    this.fragmenter = new MessageFragmenter(options.maxFrameSize);
  }

  /** framed-binary RPC 解码：rpcEncoding(1B) + envelope。失败上报 onError。 */
  private decodeRpc(body: Uint8Array): void {
    const { payload, error } = decodeRpcPayload(body);
    if (error !== undefined) this.onError.emit(error);
    else if (payload !== undefined) this.onPayload.emit(payload);
  }

  ingest(bytes: Bytes): void {
    this.frameDecoder.onBytes(bytes);
  }

  sendRpc(payload: RpcMessage): void {
    // framed-binary RPC：rpcEncoding(1B 前缀) + envelope 字节。用协商出的 selectedRpcEncoding
    //（ControlSession 协商结果），Phase 1 恒为 JSON；非 JSON 上报 onError（为未来 JSON_BINARY 留受控路径）。
    const enc = this.negotiatedRpcEncoding ?? RpcEncoding.Json;
    if (enc !== RpcEncoding.Json) {
      this.onError.emit(
        new AxtpError(
          ErrorCode.RpcEncodingUnsupported,
          `rpcEncoding 0x${enc.toString(16)} not implemented`
        )
      );
      return;
    }
    const jsonBytes = encodeJsonRpc(payload);
    const wrapped = new Uint8Array(1 + jsonBytes.length);
    wrapped[0] = enc;
    wrapped.set(jsonBytes, 1);
    this.send(PayloadType.Rpc, wrapped);
  }

  sendStream(payload: StreamPayload): void {
    this.send(PayloadType.Stream, encodeStream(payload));
  }

  startOpen(): void {
    // 仅 Physical Client 主动发 OPEN；server 构造后等待对端 OPEN（构造无副作用）。
    if (this.physicalRole === "client") this.controlSession.sendOpen();
  }

  sendClose(): void {
    this.controlSession.sendClose();
  }

  start(): void {
    const interval = this.negotiatedIntervalMs ?? this.fallbackIntervalMs;
    const timeout = this.heartbeatTimeoutMs ?? Math.max(interval * 2, 10000);
    this.heartbeat = new Heartbeat({
      intervalMs: interval,
      timeoutMs: timeout,
      onTick: () => {
        const cid = this.controlSession.allocControlId();
        this.send(PayloadType.Control, encodeHeartbeat(cid));
      },
      onTimeout: () => this.onHeartbeatTimeout.emit(undefined)
    });
    this.heartbeat.start();
  }

  stop(): void {
    this.heartbeat?.stop();
    this.heartbeat = undefined;
  }

  get isOpen(): boolean {
    return this.controlSession.isOpen;
  }

  /** 发送 CONTROL HeartbeatAck（回显对端 controlId）。 */
  private sendHeartbeatAck(controlId: number): void {
    this.send(PayloadType.Control, encodeHeartbeatAck(controlId));
  }

  /** 内部：分片 + 成帧 + transport.send。 */
  private send(payloadType: PayloadType, body: Bytes): void {
    const message: Message = { messageId: 0, payloadType, body };
    for (const frame of this.fragmenter.fragment(message)) {
      this.transport.send(this.frameEncoder.encode(frame));
    }
  }
}
