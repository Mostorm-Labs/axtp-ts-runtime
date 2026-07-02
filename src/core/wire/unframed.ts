// UnframedWireAdapter：unframed-json（WebSocket message 边界）codec，Core 的 unframed wire 实现。
// 每条 message 即一个 AXTP JSON envelope {sid,op,d}。无成帧 / 无 CONTROL / 无 STREAM / 无 CRC。
// 仅承载 RPC（Core 在 unframed profile 下不会调用 control/stream 路径）。

import type { Bytes } from "../../io/bytes.js";
import { decodeJsonRpc, encodeJsonRpc } from "../../protocol/codec/jsonRpc.js";
import type { RpcMessage } from "../../protocol/model.js";
import { AxtpError, ErrorCode } from "../../types/error.js";
import type { WireAdapter, WireSink } from "./adapter.js";

export class UnframedWireAdapter implements WireAdapter {
  feedBytes(bytes: Bytes, sink: WireSink): void {
    const payload = decodeJsonRpc(bytes);
    if (payload !== undefined) sink.onRpc(payload);
    else sink.onError(new AxtpError(ErrorCode.RpcPayloadInvalid, "malformed JSON envelope"));
  }

  encodeRpc(msg: RpcMessage): Bytes[] {
    return [encodeJsonRpc(msg)];
  }
}
