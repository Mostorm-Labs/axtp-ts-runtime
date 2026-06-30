// Protocol model：payload 数据模型。codec 与 engine 共享。
// 与 spec 20-core.md 对齐：CONTROL/RPC/STREAM 三种 PayloadType。

import {
  ControlOpcode,
  ErrorCode,
  PayloadType,
  RpcBodyEncoding,
  RpcEncoding,
  RpcOp
} from "./generated/axtp_ids_generated.js";

export { ControlOpcode, ErrorCode, PayloadType, RpcBodyEncoding, RpcEncoding, RpcOp };

/** 标准 Frame header（12B）。 */
export interface FrameHeader {
  readonly version: number;
  readonly payloadType: PayloadType;
  readonly payloadLength: number;
  readonly sourceId: number;
  readonly destinationId: number;
  readonly messageId: number;
  readonly frameIndex: number;
  readonly frameCount: number;
}

export interface Frame {
  readonly header: FrameHeader;
  readonly payload: Uint8Array;
  readonly crc16: number;
}

/** 重组后的完整 message。 */
export interface Message {
  readonly messageId: number;
  readonly payloadType: PayloadType;
  readonly body: Uint8Array;
}

/** RPC envelope 元信息（编解码辅助）。 */
export interface PayloadMeta {
  /** 方法/事件的字符串名（JSON envelope 的 d.method / d.event）。 */
  jsonMethodOrEventName?: string;
  /** Identify 携带的 randomSeed。 */
  randomSeed?: number;
  /** eventMasks hex 串（Identify 携带）。 */
  jsonEventMasks?: string;
}

/**
 * RPC payload：会话层消息（JSON / JSON_BINARY 双轨统一模型）。
 *
 * 本期 JSON-only。methodOrEventId / bodyEncoding 是 spec 20-core.md:218
 * JSON_BINARY 15B fixed header 的合同字段：JSON 路径下 methodOrEventId 仅作
 * encode 时 method/event name 的数字 fallback，bodyEncoding 不读不写。
 * spec:213 称高吞吐 profile SHOULD 实现 JSON_BINARY (0x04)——届时启用这些字段，勿删。
 *
 * wire 的 rpcEncoding(1B) 前缀属于 framed payload（payload.ts / codecPipeline.ts
 * 直接读写 body[0]），不在 envelope 内，故 RpcPayload 不设 encoding 字段。
 */
export interface RpcPayload {
  readonly op: RpcOp;
  /** 分配前 0；Identified 后填 sid（仅 JSON 路径用 jsonSid 字符串）。 */
  readonly requestId: number;
  /** JSON_BINARY 合同 method/event id (uint16)；JSON 路径仅作 name fallback。 */
  readonly methodOrEventId: number;
  readonly statusCode: ErrorCode;
  /** JSON_BINARY 合同 body 编码（NONE/TLV8/TLV16，spec:223）；JSON 路径不读写。 */
  readonly bodyEncoding: RpcBodyEncoding;
  readonly meta: PayloadMeta;
  /** 业务 body（JSON 文本字节 / TLV 字节）。 */
  readonly body: Uint8Array;
  /** JSON envelope 外层 sid（8 位 hex 或空串）。 */
  jsonSid?: string;
}

/** CONTROL payload：链路层控制。 */
export interface ControlPayload {
  readonly opcode: ControlOpcode;
  readonly controlId: number;
  readonly statusCode: ErrorCode;
  /** TLV body。 */
  readonly body: Uint8Array;
}

/** STREAM payload：数据面（framed only）。16B header + data。 */
export interface StreamPayload {
  readonly streamId: number;
  readonly seqId: number;
  readonly cursor: bigint;
  readonly data: Uint8Array;
}

// 工厂函数（便于构造）。

export function rpcPayload(init: Partial<RpcPayload> & { op: RpcOp }): RpcPayload {
  return {
    op: init.op,
    requestId: init.requestId ?? 0,
    methodOrEventId: init.methodOrEventId ?? 0,
    statusCode: init.statusCode ?? ErrorCode.Success,
    bodyEncoding: init.bodyEncoding ?? RpcBodyEncoding.None,
    meta: init.meta ?? {},
    body: init.body ?? new Uint8Array(),
    jsonSid: init.jsonSid ?? ""
  };
}

export function controlPayload(init: {
  opcode: ControlOpcode;
  controlId: number;
  statusCode?: ErrorCode;
  body?: Uint8Array;
}): ControlPayload {
  return {
    opcode: init.opcode,
    controlId: init.controlId,
    statusCode: init.statusCode ?? ErrorCode.Success,
    body: init.body ?? new Uint8Array()
  };
}
