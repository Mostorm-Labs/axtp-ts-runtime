// Payload codec（framed-binary）：message body -> payload。
// Standard Framed RPC 在 payload 前置 rpcEncoding(1B)（spec:213），JSON=0x01。
// 本期 JSON-only：若 rpcEncoding != JSON，按 RpcEncodingUnsupported 处理（不实现 JSON_BINARY）。
// CONTROL payload 用 control.ts 的 5B header + TLV。
// STREAM payload 用 stream.ts 的 16B header。

import type { RpcPayload, StreamPayload } from "../model.js";
import { PayloadType, RpcEncoding } from "../model.js";
import { decodeJsonRpc } from "./jsonRpc.js";
import { decodeStream } from "./stream.js";

/** 入站分发目标。 */
export interface PayloadSink {
  /** CONTROL：传原始 body（含 5B header + TLV），由 ControlSession 自行 decodeControl 解析。 */
  onControl(body: Uint8Array): void;
  onRpc(payload: RpcPayload): void;
  onStream(payload: StreamPayload): void;
}

/** 把重组后的 message body 解码为对应 payload，分发给 sink。 */
export class PayloadDecoder {
  constructor(private readonly sink: PayloadSink) {}

  onMessage(payloadType: PayloadType, body: Uint8Array): void {
    switch (payloadType) {
      case PayloadType.Control:
        // CONTROL 传原始 body，由 ControlSession 解析 TLV（保留协商字段）。
        this.sink.onControl(body);
        break;
      case PayloadType.Rpc:
        this.decodeRpc(body);
        break;
      case PayloadType.Stream: {
        const sp = decodeStream(body);
        if (sp !== undefined) this.sink.onStream(sp);
        break;
      }
    }
  }

  /** framed-binary RPC：rpcEncoding(1B) + body。JSON-only，其它编码拒绝。 */
  private decodeRpc(body: Uint8Array): void {
    if (body.length === 0) return;
    const rpcEncoding = body[0];
    if (rpcEncoding !== RpcEncoding.Json) {
      // JSON_BINARY 等未实现，丢弃（上层可在 OPEN 协商时只声明 JSON 以避免此情况）。
      return;
    }
    const jsonBody = body.slice(1);
    const payload = decodeJsonRpc(jsonBody);
    if (payload !== undefined) this.sink.onRpc(payload);
  }
}
