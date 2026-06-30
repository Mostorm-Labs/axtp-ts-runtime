// Payload codec（framed-binary）：message body -> payload 的解码逻辑。
// Standard Framed RPC 在 payload 前置 rpcEncoding(1B)（spec:213），JSON=0x01。
// 本期 JSON-only：rpcEncoding != JSON -> RpcEncodingUnsupported；malformed envelope -> RpcPayloadInvalid。
// CONTROL payload 由 control.ts 解析（5B header + TLV）；STREAM payload 由 stream.ts 解析（16B header）。
// payloadType 分发由 FramedLink 内联（仅一处使用，不再需要 PayloadDecoder 类 + PayloadSink 接口）。

import type { RpcMessage } from "../model.js";
import { RpcEncoding } from "../model.js";
import { decodeJsonRpc } from "./jsonRpc.js";
import { AxtpError, ErrorCode } from "../../types/error.js";

/** decodeRpcPayload 的结果：成功返回 payload，失败返回 error，空 body 两者皆空。 */
export interface DecodedRpc {
  readonly payload?: RpcMessage;
  readonly error?: AxtpError;
}

/**
 * 解码 framed-binary RPC body（rpcEncoding(1B) + JSON envelope 字节）。
 * 空 body 返回空结果；非 JSON 编码 / malformed envelope 返回 error（由调用方上报）。
 */
export function decodeRpcPayload(body: Uint8Array): DecodedRpc {
  if (body.length === 0) return {};
  const rpcEncoding = body[0];
  if (rpcEncoding !== RpcEncoding.Json) {
    return {
      error: new AxtpError(
        ErrorCode.RpcEncodingUnsupported,
        `unsupported rpcEncoding: 0x${rpcEncoding.toString(16)}`
      )
    };
  }
  const payload = decodeJsonRpc(body.slice(1));
  if (payload === undefined) {
    return { error: new AxtpError(ErrorCode.RpcPayloadInvalid, "malformed RPC JSON envelope") };
  }
  return { payload };
}
