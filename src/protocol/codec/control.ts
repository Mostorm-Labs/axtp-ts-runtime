// CONTROL codec：opcode(1B) + controlId(2B BE) + statusCode(2B BE) + TLV body。
// TLV = tag(1B) + length(1B) + value(N)。
// 必需 OPEN/ACCEPT TLV（spec 20-core.md:127-134）：
//   0x04 maxFrameSize（OPEN+ACCEPT 必需，uint16，总帧含 header+CRC）
//   0x07 supportedPayloadTypes（OPEN+ACCEPT 必需，bitmap uint8）
//   0x08 supportedRpcEncodings（OPEN 必需，bitmap uint8）
//   0x0A heartbeatIntervalMs（OPEN+ACCEPT 必需，uint16，spec 示例:0a 02 03 e8）
//   0x0B ackMode（OPEN+ACCEPT 必需，uint8，Phase1=NONE=0x00）
//   0x1E selectedRpcEncoding（成功 ACCEPT 必需，uint8）
// 对 OPEN/HEARTBEAT/CLOSE 的 response MUST 回显 controlId（spec:123）。

import type { Bytes } from "../../io/bytes.js";
import { ByteReader, ByteWriter } from "../../io/io.js";
import { ControlOpcode, controlPayload, RpcEncoding, type ControlPayload } from "../model.js";

/** ACK 模式常量。Phase1 固定 NONE=0。 */
export const AckMode = { None: 0x00 } as const;

/** CONTROL TLV tag。 */
export const ControlTlvTag = {
  MaxFrameSize: 0x04,
  SupportedPayloadTypes: 0x07,
  SupportedRpcEncodings: 0x08,
  HeartbeatIntervalMs: 0x0a,
  AckMode: 0x0b,
  SelectedRpcEncoding: 0x1e
} as const;

/** OPEN/ACCEPT 协商参数。 */
export interface NegotiationParams {
  /** 总帧大小上限（含 12B header + 2B CRC）。 */
  maxFrameSize: number;
  /** 支持的 payload 类型 bitmap。Phase1 = CONTROL|RPC|STREAM = 0x07。 */
  supportedPayloadTypes: number;
  /** OPEN 声明支持的 RPC 编码。JSON-only = 0x01。 */
  supportedRpcEncodings?: number;
  /** 心跳间隔 ms（500-60000）。 */
  heartbeatIntervalMs: number;
  /** ACK 模式。Phase1 = NONE = 0。 */
  ackMode: number;
  /** 成功 ACCEPT 选定的 RPC 编码。JSON = 0x01。 */
  selectedRpcEncoding?: number;
}

export const kDefaultPayloadTypes = 0x07; // bitmap: bit0=Control, bit1=Rpc, bit2=Stream（spec:130）
export const kJsonOnlyEncoding = RpcEncoding.Json; // 0x01

/**
 * heartbeatIntervalMs 建议范围（spec 未强制硬性约束，runtime 防御性 clamp，
 * 防止对端或本地越界配置导致心跳过频/失效）。
 */
export const MIN_HEARTBEAT_INTERVAL_MS = 500;
export const MAX_HEARTBEAT_INTERVAL_MS = 60000;

/** 把 heartbeatIntervalMs 夹到合法范围；非有限值回落到下限。 */
export function clampHeartbeatInterval(ms: number): number {
  if (!Number.isFinite(ms)) return MIN_HEARTBEAT_INTERVAL_MS;
  return Math.max(MIN_HEARTBEAT_INTERVAL_MS, Math.min(MAX_HEARTBEAT_INTERVAL_MS, Math.trunc(ms)));
}

/** 默认 Phase1 OPEN 参数。 */
export function defaultOpenParams(
  maxFrameSize = 4096,
  heartbeatIntervalMs = 1000
): NegotiationParams {
  return {
    maxFrameSize,
    supportedPayloadTypes: kDefaultPayloadTypes,
    supportedRpcEncodings: kJsonOnlyEncoding,
    heartbeatIntervalMs: clampHeartbeatInterval(heartbeatIntervalMs),
    ackMode: AckMode.None
  };
}

/** TLV 编码：tag(1B) + length(1B) + value。 */
function writeTlv(writer: ByteWriter, tag: number, value: number, width: 1 | 2 | 4): void {
  writer.writeU8(tag);
  writer.writeU8(width);
  if (width === 1) writer.writeU8(value);
  else if (width === 2) writer.writeU16(value);
  else writer.writeU32(value);
}

function readTlv(reader: ByteReader): { tag: number; value: number } | undefined {
  const tag = reader.readU8();
  const len = reader.readU8();
  if (tag === undefined || len === undefined) return undefined;
  let value = 0;
  if (len === 1) value = reader.readU8Strict();
  else if (len === 2) value = reader.readU16Strict();
  else if (len === 4) value = reader.readU32Strict();
  else {
    // 异常宽度：跳过并返回 undefined（调用方会忽略此 TLV）
    reader.readBytes(len);
    return undefined;
  }
  return { tag, value };
}

/** 编码 CONTROL payload header（opcode + controlId + statusCode）+ TLV body。 */
export function encodeControl(
  opcode: ControlOpcode,
  controlId: number,
  statusCode: number,
  tlv?: Partial<NegotiationParams>
): Bytes {
  const writer = new ByteWriter();
  writer.writeU8(opcode);
  writer.writeU16(controlId);
  writer.writeU16(statusCode);
  if (tlv !== undefined) {
    if (tlv.maxFrameSize !== undefined)
      writeTlv(writer, ControlTlvTag.MaxFrameSize, tlv.maxFrameSize, 2);
    if (tlv.supportedPayloadTypes !== undefined)
      writeTlv(writer, ControlTlvTag.SupportedPayloadTypes, tlv.supportedPayloadTypes, 1);
    if (tlv.supportedRpcEncodings !== undefined)
      writeTlv(writer, ControlTlvTag.SupportedRpcEncodings, tlv.supportedRpcEncodings, 1);
    if (tlv.heartbeatIntervalMs !== undefined)
      writeTlv(writer, ControlTlvTag.HeartbeatIntervalMs, tlv.heartbeatIntervalMs, 2);
    if (tlv.ackMode !== undefined) writeTlv(writer, ControlTlvTag.AckMode, tlv.ackMode, 1);
    if (tlv.selectedRpcEncoding !== undefined)
      writeTlv(writer, ControlTlvTag.SelectedRpcEncoding, tlv.selectedRpcEncoding, 1);
  }
  return writer.takeBytes();
}

/** 解码 CONTROL payload header + TLV。 */
export function decodeControl(body: Bytes): ControlPayload & { tlv: Partial<NegotiationParams> } {
  const reader = new ByteReader(body);
  const opcode = reader.readU8Strict();
  const controlId = reader.readU16Strict();
  const statusCode = reader.readU16Strict();
  const tlvBody = reader.readBytes(reader.remaining()) ?? new Uint8Array();
  const tlv: Partial<NegotiationParams> = {};
  const tlvReader = new ByteReader(tlvBody);
  while (!tlvReader.empty()) {
    const entry = readTlv(tlvReader);
    if (entry === undefined) break;
    switch (entry.tag) {
      case ControlTlvTag.MaxFrameSize:
        tlv.maxFrameSize = entry.value;
        break;
      case ControlTlvTag.SupportedPayloadTypes:
        tlv.supportedPayloadTypes = entry.value;
        break;
      case ControlTlvTag.SupportedRpcEncodings:
        tlv.supportedRpcEncodings = entry.value;
        break;
      case ControlTlvTag.HeartbeatIntervalMs:
        tlv.heartbeatIntervalMs = entry.value;
        break;
      case ControlTlvTag.AckMode:
        tlv.ackMode = entry.value;
        break;
      case ControlTlvTag.SelectedRpcEncoding:
        tlv.selectedRpcEncoding = entry.value;
        break;
    }
  }
  return {
    ...controlPayload({ opcode, controlId, statusCode, body: tlvBody }),
    tlv
  };
}

/** 编码 OPEN（带全部必需 TLV）。 */
export function encodeOpen(controlId: number, params: NegotiationParams): Bytes {
  return encodeControl(ControlOpcode.Open, controlId, 0, params);
}

/** 编码 ACCEPT（成功带 selectedRpcEncoding）。 */
export function encodeAccept(controlId: number, params: NegotiationParams): Bytes {
  return encodeControl(ControlOpcode.Accept, controlId, 0, params);
}

/**
 * 编码拒绝 OPEN 的 ACCEPT（非零 statusCode，无 TLV）。
 * spec:121: 不存在 REJECT opcode，拒绝 OPEN = 带非零 statusCode 的 ACCEPT。
 */
export function encodeRejectedAccept(controlId: number, statusCode: number): Bytes {
  return encodeControl(ControlOpcode.Accept, controlId, statusCode);
}

/** HEARTBEAT（无 body）。 */
export function encodeHeartbeat(controlId: number): Bytes {
  return encodeControl(ControlOpcode.Heartbeat, controlId, 0);
}

/** HEARTBEAT_ACK（回显 controlId）。 */
export function encodeHeartbeatAck(controlId: number): Bytes {
  return encodeControl(ControlOpcode.HeartbeatAck, controlId, 0);
}

/** CLOSE。 */
export function encodeClose(controlId: number): Bytes {
  return encodeControl(ControlOpcode.Close, controlId, 0);
}

/** CLOSE_ACK（回显 controlId）。 */
export function encodeCloseAck(controlId: number): Bytes {
  return encodeControl(ControlOpcode.CloseAck, controlId, 0);
}
