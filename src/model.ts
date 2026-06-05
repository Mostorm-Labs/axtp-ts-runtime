import type { Bytes } from "./bytes.js";
import {
  ControlOpcode,
  ErrorCode,
  PayloadType,
  RpcBodyEncoding,
  RpcEncoding,
  RpcOp
} from "./generated/axtp_ids_generated.js";

export const kAxtpStandardMagic0 = 0x41;
export const kAxtpStandardMagic1 = 0x58;
export const kAxtpVersion1 = 0x01;
export const kStandardFrameHeaderSize = 12;
export const kStandardFrameCrcSize = 2;
export const kControlPayloadHeaderSize = 5;
export const kBinaryRpcHeaderSize = 11;
export const kStreamPayloadHeaderSize = 16;

export enum SourceProtocol {
  AxtpV1 = 0x01,
  JsonRpc = 0x02
}

export interface PayloadMeta {
  sourceProtocol: SourceProtocol;
  sessionId: number;
  requestId: number;
  jsonSid: string;
  jsonMethodOrEventName: string;
}

export interface ControlPayload {
  opcode: ControlOpcode;
  controlId: number;
  statusCode: ErrorCode;
  meta: PayloadMeta;
  body: Bytes;
}

export interface RpcPayload {
  encoding: RpcEncoding;
  op: RpcOp;
  requestId: number;
  methodOrEventId: number;
  statusCode: ErrorCode;
  bodyEncoding: RpcBodyEncoding;
  meta: PayloadMeta;
  body: Bytes;
}

export interface StreamPayload {
  streamId: number;
  seqId: number;
  cursor: bigint;
  meta: PayloadMeta;
  data: Bytes;
}

export interface FrameHeader {
  version: number;
  payloadType: PayloadType;
  payloadLength: number;
  sourceId: number;
  destinationId: number;
  messageId: number;
  frameIndex: number;
  frameCount: number;
}

export interface Frame {
  header: FrameHeader;
  payload: Bytes;
  crc16: number;
}

export interface Message {
  messageId: number;
  payloadType: PayloadType;
  body: Bytes;
}

export function defaultPayloadMeta(): PayloadMeta {
  return {
    sourceProtocol: SourceProtocol.AxtpV1,
    sessionId: 0,
    requestId: 0,
    jsonSid: "",
    jsonMethodOrEventName: ""
  };
}

export function controlPayload(partial: Partial<ControlPayload> = {}): ControlPayload {
  const meta = { ...defaultPayloadMeta(), ...partial.meta };
  return {
    opcode: ControlOpcode.Open,
    controlId: 0,
    statusCode: ErrorCode.Success,
    body: new Uint8Array(),
    ...partial,
    meta
  };
}

export function rpcPayload(partial: Partial<RpcPayload> = {}): RpcPayload {
  const meta = { ...defaultPayloadMeta(), ...partial.meta };
  return {
    encoding: RpcEncoding.Json,
    op: RpcOp.Request,
    requestId: 0,
    methodOrEventId: 0,
    statusCode: ErrorCode.Success,
    bodyEncoding: RpcBodyEncoding.Tlv8,
    body: new Uint8Array(),
    ...partial,
    meta
  };
}

export function streamPayload(partial: Partial<StreamPayload> = {}): StreamPayload {
  const meta = { ...defaultPayloadMeta(), ...partial.meta };
  return {
    streamId: 0,
    seqId: 0,
    cursor: 0n,
    data: new Uint8Array(),
    ...partial,
    meta
  };
}
