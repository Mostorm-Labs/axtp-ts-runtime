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

/**
 * RpcMessage：编码无关的 RPC 语义模型（判别联合）—— 内部核心数据通路使用。
 *
 * 按 op 区分，每个 op 字段语义清晰、无死字段；业务数据（params/result/data）
 * 直接持有已 parse 的结构化 JS 值，消除 bytes 中转与重复 JSON 编解码。
 * method/eventName/randomSeed/eventMasks/axtpVersion 均为一等字段；status（而非
 * statusCode）与 wire 的 d.status 对齐。
 *
 * 编码无关：JSON 路径由 codec 直接 wire↔RpcMessage；未来 JSON_BINARY 由其 codec
 * 负责映射，本模型不携带 methodOrEventId/bodyEncoding 等编码细节（它们属 codec 内部）。
 */
export interface HelloPayload {
  readonly op: RpcOp.Hello;
  readonly sid: string;
  readonly axtpVersion: string;
}
export interface IdentifyPayload {
  readonly op: RpcOp.Identify;
  readonly sid: string;
  readonly randomSeed: number;
  readonly eventMasks?: string;
}
export interface IdentifiedPayload {
  readonly op: RpcOp.Identified;
  readonly sid: string;
}
export interface EventPayload {
  readonly op: RpcOp.Event;
  readonly sid: string;
  readonly eventName: string;
  readonly data: unknown;
}
export interface RequestPayload {
  readonly op: RpcOp.Request;
  readonly sid: string;
  readonly requestId: number;
  readonly method: string;
  readonly params: unknown;
}
export interface ResponsePayload {
  readonly op: RpcOp.RequestResponse;
  readonly sid: string;
  readonly requestId: number;
  readonly status: ErrorCode;
  readonly result: unknown;
}
export type RpcMessage =
  | HelloPayload
  | IdentifyPayload
  | IdentifiedPayload
  | EventPayload
  | RequestPayload
  | ResponsePayload;

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

// RpcMessage 构造工厂（字段默认值收敛于此；内部核心通路构造用）。

export function helloMsg(sid: string, axtpVersion: string): HelloPayload {
  return { op: RpcOp.Hello, sid, axtpVersion };
}
export function identifyMsg(sid: string, randomSeed: number, eventMasks?: string): IdentifyPayload {
  return eventMasks !== undefined
    ? { op: RpcOp.Identify, sid, randomSeed, eventMasks }
    : { op: RpcOp.Identify, sid, randomSeed };
}
export function identifiedMsg(sid: string): IdentifiedPayload {
  return { op: RpcOp.Identified, sid };
}
export function eventMsg(sid: string, eventName: string, data: unknown): EventPayload {
  return { op: RpcOp.Event, sid, eventName, data };
}
export function requestMsg(
  sid: string,
  requestId: number,
  method: string,
  params: unknown
): RequestPayload {
  return { op: RpcOp.Request, sid, requestId, method, params };
}
export function responseMsg(
  sid: string,
  requestId: number,
  status: ErrorCode,
  result?: unknown
): ResponsePayload {
  return { op: RpcOp.RequestResponse, sid, requestId, status, result };
}
