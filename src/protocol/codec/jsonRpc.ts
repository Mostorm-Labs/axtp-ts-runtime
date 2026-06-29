// JSON-RPC codec：处理 unframed-json envelope {sid, op, d}。
// 这是 WebSocket Unframed JSON profile 的核心编解码。
// spec 20-core.md:170-227。
//
// Request:  d = { id, method, params }
// Response: d = { id, status, result }   (status = uint errorCode; 0=SUCCESS)
// Event:    d = { event, data }
// Hello:    d = { axtpVersion }
// Identify: d = { randomSeed, eventMasks, rpcVersion? }
// Identified: d = {}   （sid 在外层；对齐 conformance: identified.d == {}）
//
// 注意：method/event 在 JSON 恒为字符串名（数字 id 仅 JSON_BINARY，本期不实现）。

import { bytesToText, toBytes, type Bytes } from "../../io/bytes.js";
import { ErrorCode } from "../../types/error.js";
import { registry } from "../../types/registry.js";
import { AXTP_SPEC_VERSION } from "../generated/axtpVersion.js";
import { RpcEncoding, RpcOp, rpcPayload, type RpcPayload } from "../model.js";

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

/** 把 envelope JSON 文本解码为 RpcPayload。返回 undefined 表示无法解析。 */
export function decodeJsonRpc(text: Bytes | string): RpcPayload | undefined {
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
        return rpcPayload({
          op,
          jsonSid: sid,
          body: toBytes(JSON.stringify(d)),
          meta: {
            jsonEventMasks: typeof d.eventMasks === "string" ? d.eventMasks : undefined
          }
        });
      case RpcOp.Identify: {
        const randomSeed = typeof d.randomSeed === "number" ? d.randomSeed >>> 0 : 0;
        const eventMasks = typeof d.eventMasks === "string" ? d.eventMasks : "";
        return rpcPayload({
          op,
          jsonSid: sid,
          body: toBytes(JSON.stringify(d)),
          meta: { randomSeed, jsonEventMasks: eventMasks }
        });
      }
      case RpcOp.Identified:
        // Identified.d = {}（对齐 conformance）。sid 在外层。
        return rpcPayload({ op, jsonSid: sid, body: toBytes("{}"), meta: {} });
      case RpcOp.Event: {
        const eventName = typeof d.event === "string" ? d.event : "";
        const eventId = eventName ? (registry.eventId(eventName) ?? 0) : 0;
        return rpcPayload({
          op,
          methodOrEventId: eventId,
          jsonSid: sid,
          body: toBytes(JSON.stringify(d.data ?? {})),
          meta: { jsonMethodOrEventName: eventName }
        });
      }
      case RpcOp.Request: {
        const requestId = parseRequestIdFromEnvelope(object);
        const methodName = typeof d.method === "string" ? d.method : "";
        const methodId = methodName ? (registry.methodId(methodName) ?? 0) : 0;
        const params = d.params;
        return rpcPayload({
          op,
          requestId,
          methodOrEventId: methodId,
          jsonSid: sid,
          body: params === undefined ? toBytes("{}") : toBytes(JSON.stringify(params)),
          meta: { jsonMethodOrEventName: methodName }
        });
      }
      case RpcOp.RequestResponse: {
        const requestId = parseRequestIdFromEnvelope(object);
        const status = typeof d.status === "number" ? d.status >>> 0 : ErrorCode.Success;
        const result = d.result;
        return rpcPayload({
          op,
          requestId,
          statusCode: status as ErrorCode,
          jsonSid: sid,
          body: result === undefined ? new Uint8Array() : toBytes(JSON.stringify(result)),
          meta: {}
        });
      }
      default:
        return rpcPayload({
          op,
          jsonSid: sid,
          body: toBytes(JSON.stringify(d)),
          meta: {}
        });
    }
  } catch {
    return undefined;
  }
}

/** 把 RpcPayload 编码为 envelope JSON 字节。 */
export function encodeJsonRpc(payload: RpcPayload): Bytes {
  const sid = payload.jsonSid ?? "";
  switch (payload.op) {
    case RpcOp.Hello: {
      const body = safeParseObject(payload.body);
      if (body.axtpVersion === undefined) body.axtpVersion = AXTP_SPEC_VERSION;
      return toBytes(JSON.stringify({ sid, op: payload.op, d: body }));
    }
    case RpcOp.Identify: {
      const d = safeParseObject(payload.body);
      if (payload.meta.randomSeed !== undefined) d.randomSeed = payload.meta.randomSeed;
      if (payload.meta.jsonEventMasks) d.eventMasks = payload.meta.jsonEventMasks;
      return toBytes(JSON.stringify({ sid, op: payload.op, d }));
    }
    case RpcOp.Identified:
      // d = {}（对齐 conformance）。sid 在外层。
      return toBytes(JSON.stringify({ sid, op: payload.op, d: {} }));
    case RpcOp.Event: {
      const eventName =
        payload.meta.jsonMethodOrEventName ?? registry.eventName(payload.methodOrEventId) ?? "";
      const data = safeParse(payload.body);
      const d: JsonObject = { event: eventName };
      if (data !== undefined) d.data = data;
      return toBytes(JSON.stringify({ sid, op: payload.op, d }));
    }
    case RpcOp.Request: {
      const methodName =
        payload.meta.jsonMethodOrEventName ??
        registry.methodName(payload.methodOrEventId) ??
        String(payload.methodOrEventId);
      const params = safeParse(payload.body);
      const d: JsonObject = { id: payload.requestId, method: methodName };
      if (params !== undefined) d.params = params;
      return toBytes(JSON.stringify({ sid, op: payload.op, d }));
    }
    case RpcOp.RequestResponse: {
      const d: JsonObject = { id: payload.requestId, status: payload.statusCode };
      if (payload.statusCode === ErrorCode.Success) {
        const result = safeParse(payload.body);
        if (result !== undefined) d.result = result;
      }
      return toBytes(JSON.stringify({ sid, op: payload.op, d }));
    }
    default:
      return toBytes(JSON.stringify({ sid, op: payload.op, d: safeParseObject(payload.body) }));
  }
}

function safeParse(body: Bytes): JsonValue | undefined {
  if (body.length === 0) return undefined;
  try {
    return JSON.parse(bytesToText(body)) as JsonValue;
  } catch {
    return undefined;
  }
}

function safeParseObject(body: Bytes): JsonObject {
  const v = safeParse(body);
  if (v === null || typeof v !== "object" || Array.isArray(v)) return {};
  return v as JsonObject;
}

/** 编码 JSON body（统一入口，避免 session 层内联 TextEncoder）。 */
export function encodeJsonBody(value: unknown): Bytes {
  return toBytes(JSON.stringify(value ?? {}));
}

/** 解码 JSON body（统一入口，避免 session 层内联 TextDecoder）。解析失败返回 undefined。 */
export function decodeJsonBody(body: Bytes): unknown {
  if (body.length === 0) return {};
  try {
    return JSON.parse(bytesToText(body));
  } catch {
    return undefined;
  }
}

export { RpcEncoding };
