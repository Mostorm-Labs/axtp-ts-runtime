import { RpcBodyEncoding, RpcEncoding } from "./generated/axtp_ids_generated.js";

export const rpcEncodingJsonBinary = 0x04 as RpcEncoding;

export function isJsonBinaryRpcEncoding(encoding: RpcEncoding): boolean {
  return encoding === rpcEncodingJsonBinary;
}

export function bodyEncodingForRpcEncoding(encoding: RpcEncoding): RpcBodyEncoding {
  return isJsonBinaryRpcEncoding(encoding) ? RpcBodyEncoding.Tlv8 : RpcBodyEncoding.None;
}
