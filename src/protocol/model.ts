// Protocol model：payload 数据模型。codec 与 engine 共享。
// 与 spec 20-core.md 对齐：CONTROL/RPC/STREAM 三种 PayloadType。

import {
  ControlOpcode,
  ErrorCode,
  PayloadType,
  RpcBodyEncoding,
  RpcEncoding,
  RpcOp
} from "../protocol/generated/axtp_ids_generated.js";

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

/** RPC payload：会话层消息。 */
export interface RpcPayload {
  readonly encoding: RpcEncoding;
  readonly op: RpcOp;
  /** 分配前 0；Identified 后填 sid（仅 JSON 路径用 jsonSid 字符串）。 */
  readonly requestId: number;
  readonly methodOrEventId: number;
  readonly statusCode: ErrorCode;
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
    encoding: init.encoding ?? RpcEncoding.Json,
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

export function streamPayload(init: {
  streamId: number;
  seqId: number;
  cursor: bigint;
  data: Uint8Array;
}): StreamPayload {
  return {
    streamId: init.streamId,
    seqId: init.seqId,
    cursor: init.cursor,
    data: init.data
  };
}

/** 空 meta。 */
export function defaultPayloadMeta(): PayloadMeta {
  return {};
}
