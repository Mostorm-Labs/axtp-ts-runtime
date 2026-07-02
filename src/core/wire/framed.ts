// FramedWireAdapter：standard-framed binary 的 codec 编排（Core 的 framed wire 实现）。
// 入站：FrameDecoder（magic resync + 8 项校验）→ MessageReassembler（分片重组）→ 按 payloadType 分发
//   （Control body 直传 / RPC 解码 / STREAM 解码）→ WireSink。
// 出站：message → MessageFragmenter（按 maxFrameSize 分片）→ FrameEncoder（12B header+payload+2B CRC）→ 字节块。
// 移植自 FramedLink 的 codec 部分；心跳/事件流已剥离（心跳移 Endpoint，事件改为 CoreEvent）。

import type { Bytes } from "../../io/bytes.js";
import {
  FrameDecoder,
  FrameEncoder,
  MessageFragmenter,
  MessageReassembler
} from "../../protocol/codec/frame.js";
import { encodeJsonRpc } from "../../protocol/codec/jsonRpc.js";
import { decodeRpcPayload } from "../../protocol/codec/payload.js";
import { decodeStream, encodeStream as encodeStreamPayload } from "../../protocol/codec/stream.js";
import {
  PayloadType,
  RpcEncoding,
  type Message,
  type RpcMessage,
  type StreamPayload
} from "../../protocol/model.js";
import type { WireAdapter, WireSink } from "./adapter.js";

export class FramedWireAdapter implements WireAdapter {
  private currentSink: WireSink | undefined;
  private readonly frameDecoder: FrameDecoder;
  private readonly reassembler: MessageReassembler;
  private readonly fragmenter: MessageFragmenter;
  private readonly encoder = new FrameEncoder();

  constructor(maxFrameSize = 4096) {
    this.reassembler = new MessageReassembler(
      {
        onMessage: (m) => this.dispatchPayload(m.payloadType, m.body),
        onError: (err) => this.currentSink?.onError(err)
      },
      maxFrameSize * 256
    );
    this.frameDecoder = new FrameDecoder(
      {
        onFrame: (f) => this.reassembler.onFrame(f),
        onError: (err) => this.currentSink?.onError(err)
      },
      maxFrameSize
    );
    this.fragmenter = new MessageFragmenter(maxFrameSize);
  }

  feedBytes(bytes: Bytes, sink: WireSink): void {
    this.currentSink = sink;
    this.frameDecoder.onBytes(bytes);
  }

  encodeRpc(msg: RpcMessage): Bytes[] {
    // framed-binary RPC：rpcEncoding(1B 前缀，Phase1=JSON) + JSON envelope。
    const jsonBytes = encodeJsonRpc(msg);
    const body = new Uint8Array(1 + jsonBytes.length);
    body[0] = RpcEncoding.Json;
    body.set(jsonBytes, 1);
    return this.framePayload(PayloadType.Rpc, body);
  }

  /** framed 专有：CONTROL payload body（已由 control codec 编码）→ 成帧。 */
  encodeControlBody(body: Bytes): Bytes[] {
    return this.framePayload(PayloadType.Control, body);
  }

  /** framed 专有：STREAM payload → 编码 + 成帧。 */
  encodeStream(msg: StreamPayload): Bytes[] {
    return this.framePayload(PayloadType.Stream, encodeStreamPayload(msg));
  }

  setMaxFrameSize(size: number): void {
    this.frameDecoder.setMaxFrameSize(size);
    this.fragmenter.setMaxFrameSize(size);
  }

  private dispatchPayload(payloadType: PayloadType, body: Bytes): void {
    const sink = this.currentSink;
    if (sink === undefined) return;
    switch (payloadType) {
      case PayloadType.Control:
        sink.onControl(body);
        break;
      case PayloadType.Rpc: {
        const { payload, error } = decodeRpcPayload(body);
        if (error !== undefined) sink.onError(error);
        else if (payload !== undefined) sink.onRpc(payload);
        break;
      }
      case PayloadType.Stream: {
        const sp = decodeStream(body);
        if (sp !== undefined) sink.onStream(sp);
        break;
      }
    }
  }

  private framePayload(payloadType: PayloadType, body: Bytes): Bytes[] {
    const message: Message = { messageId: 0, payloadType, body };
    return this.fragmenter.fragment(message).map((f) => this.encoder.encode(f));
  }
}
