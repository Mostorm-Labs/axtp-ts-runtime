// JSON-RPC codec：处理 envelope {sid, op, d}（framed RPC 的 JSON 编码 + WS Unframed JSON）。
// spec 20-core.md:170-227。编码无关判别联合 RpcMessage 的 wire 读写：
// 一次 parse 直出结构化 RpcMessage，一次 stringify 直出 wire，无 bytes 中转/重复编解码。
//
// Request:    d = { id, method, params }
// Response:   d = { id, status, result }   (status = uint errorCode; 0=SUCCESS)
// Event:      d = { event, data }
// Hello:      d = { axtpVersion }
// Identify:   d = { randomSeed, eventMasks }
// Identified: d = {}   （sid 在外层；对齐 conformance: identified.d == {}）
//
// method/event 在 JSON 恒为字符串名（数字 id 仅 JSON_BINARY，本期不实现）。

import { bytesToText, toBytes, type Bytes } from "../../io/bytes.js";
import { ErrorCode } from "../../types/error.js";
import { RpcEncoding, RpcOp, type RpcMessage } from "../model.js";

type JsonValue = null | boolean | number | string | JsonValue[] | JsonObject;
interface JsonObject {
  [key: string]: JsonValue | undefined;
}

function asObject(value: JsonValue | undefined): JsonObject {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new Error("expected object");
  }
  return value;
}

function parseOp(object: JsonObject): RpcOp {
  const op = object.op;
  if (typeof op !== "number" || !Number.isInteger(op) || op < 0 || op > 0xff) {
    throw new Error("invalid op");
  }
  return op as RpcOp;
}

function parseSid(object: JsonObject): string {
  return typeof object.sid === "string" ? object.sid : "";
}

function parseRequestIdFromEnvelope(object: JsonObject): number {
  try {
    const d = asObject(object.d);
    const id = d.id;
    if (typeof id === "number" && Number.isInteger(id) && id > 0 && id <= 0xffffffff) return id;
  } catch {
    return 0;
  }
  return 0;
}

/** 把 envelope JSON 文本解码为 RpcMessage（判别联合）。返回 undefined 表示无法解析。 */
export function decodeJsonRpc(text: Bytes | string): RpcMessage | undefined {
  let object: JsonObject;
  try {
    object = JSON.parse(typeof text === "string" ? text : bytesToText(text)) as JsonObject;
  } catch {
    return undefined;
  }
  try {
    const op = parseOp(object);
    const sid = parseSid(object);
    const d = asObject(object.d);
    switch (op) {
      case RpcOp.Hello:
        return { op, sid, axtpVersion: typeof d.axtpVersion === "string" ? d.axtpVersion : "" };
      case RpcOp.Identify: {
        const randomSeed = typeof d.randomSeed === "number" ? d.randomSeed >>> 0 : 0;
        const eventMasks = typeof d.eventMasks === "string" ? d.eventMasks : undefined;
        return eventMasks !== undefined
          ? { op, sid, randomSeed, eventMasks }
          : { op, sid, randomSeed };
      }
      case RpcOp.Identified:
        return { op, sid };
      case RpcOp.Event:
        return {
          op,
          sid,
          eventName: typeof d.event === "string" ? d.event : "",
          data: d.data ?? {}
        };
      case RpcOp.Request:
        return {
          op,
          sid,
          requestId: parseRequestIdFromEnvelope(object),
          method: typeof d.method === "string" ? d.method : "",
          params: d.params ?? {}
        };
      case RpcOp.RequestResponse: {
        const status = typeof d.status === "number" ? d.status >>> 0 : ErrorCode.Success;
        return {
          op,
          sid,
          requestId: parseRequestIdFromEnvelope(object),
          status: status as ErrorCode,
          result: d.result
        };
      }
      default:
        return undefined;
    }
  } catch {
    return undefined;
  }
}

/** 把 RpcMessage 编码为 envelope JSON 字节。 */
export function encodeJsonRpc(msg: RpcMessage): Bytes {
  switch (msg.op) {
    case RpcOp.Hello:
      return toBytes(
        JSON.stringify({ sid: msg.sid, op: msg.op, d: { axtpVersion: msg.axtpVersion } })
      );
    case RpcOp.Identify: {
      const d: JsonObject = { randomSeed: msg.randomSeed };
      if (msg.eventMasks) d.eventMasks = msg.eventMasks;
      return toBytes(JSON.stringify({ sid: msg.sid, op: msg.op, d }));
    }
    case RpcOp.Identified:
      return toBytes(JSON.stringify({ sid: msg.sid, op: msg.op, d: {} }));
    case RpcOp.Event: {
      const d: JsonObject = { event: msg.eventName };
      if (msg.data !== undefined) d.data = msg.data as JsonValue;
      return toBytes(JSON.stringify({ sid: msg.sid, op: msg.op, d }));
    }
    case RpcOp.Request: {
      const d: JsonObject = { id: msg.requestId, method: msg.method };
      if (msg.params !== undefined) d.params = msg.params as JsonValue;
      return toBytes(JSON.stringify({ sid: msg.sid, op: msg.op, d }));
    }
    case RpcOp.RequestResponse: {
      const d: JsonObject = { id: msg.requestId, status: msg.status };
      if (msg.status === ErrorCode.Success && msg.result !== undefined)
        d.result = msg.result as JsonValue;
      return toBytes(JSON.stringify({ sid: msg.sid, op: msg.op, d }));
    }
  }
}

/** 编码 JSON body（公共 helper）。 */
export function encodeJsonBody(value: unknown): Bytes {
  return toBytes(JSON.stringify(value ?? {}));
}

/** 解码 JSON body（公共 helper）。空 body 返回 {}，解析失败返回 undefined。 */
export function decodeJsonBody(body: Bytes): unknown {
  if (body.length === 0) return {};
  try {
    return JSON.parse(bytesToText(body));
  } catch {
    return undefined;
  }
}

export { RpcEncoding };
