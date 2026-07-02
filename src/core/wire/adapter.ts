// core/wire/adapter.ts — WireAdapter：framed/unframed 共有的 codec 契约。
// Core 经此与字节交互：feedBytes 入站解码（→ WireSink 分发 control/rpc/stream），
// encodeRpc 出站编码。framed 额外有 encodeControlBody/encodeStream + setMaxFrameSize
// （由 FramedWireAdapter 提供，Core 在 framed profile 下类型收窄使用）。

import type { Bytes } from "../../io/bytes.js";
import type { RpcMessage, StreamPayload } from "../../protocol/model.js";
import type { AxtpError } from "../../types/error.js";

/** 入站解码后各 payload 类型的接收回调。 */
export interface WireSink {
  /** framed：CONTROL payload body（已剥离 frame header；Core 交给 controlSession）。 */
  onControl(body: Uint8Array): void;
  onRpc(msg: RpcMessage): void;
  /** framed：STREAM payload。 */
  onStream(msg: StreamPayload): void;
  onError(err: AxtpError): void;
}

/** framed/unframed 共有的 codec 接口。 */
export interface WireAdapter {
  /** 入站：原始字节 → 解码 → sink 分发。 */
  feedBytes(bytes: Bytes, sink: WireSink): void;
  /** 出站：RPC 消息 → wire 字节块（framed 可能多帧；unframed 单块）。 */
  encodeRpc(msg: RpcMessage): Bytes[];
}
