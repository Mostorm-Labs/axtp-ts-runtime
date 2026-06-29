// 测试专用：构造 JSON-RPC envelope 字节的便捷函数（生产代码用 encodeJsonRpc）。

import type { Bytes } from "../../src/io/bytes.js";
import { toBytes } from "../../src/io/bytes.js";
import { encodeJsonRpc } from "../../src/protocol/codec/jsonRpc.js";
import { AXTP_SPEC_VERSION } from "../../src/protocol/generated/axtpVersion.js";
import { RpcOp } from "../../src/protocol/generated/axtp_ids_generated.js";
import { rpcPayload } from "../../src/protocol/model.js";
import { ErrorCode } from "../../src/types/error.js";
import { registry } from "../../src/types/registry.js";

export function buildRequestJson(
  requestId: number,
  methodName: string,
  params: unknown,
  sid: string
): Bytes {
  return encodeJsonRpc(
    rpcPayload({
      op: RpcOp.Request,
      requestId,
      methodOrEventId: registry.methodId(methodName) ?? 0,
      jsonSid: sid,
      body: toBytes(JSON.stringify(params ?? {})),
      meta: { jsonMethodOrEventName: methodName }
    })
  );
}

export function buildResponseJson(requestId: number, result: unknown, sid: string): Bytes {
  return encodeJsonRpc(
    rpcPayload({
      op: RpcOp.RequestResponse,
      requestId,
      statusCode: ErrorCode.Success,
      jsonSid: sid,
      body: toBytes(JSON.stringify(result ?? {}))
    })
  );
}

export function buildErrorResponseJson(requestId: number, code: ErrorCode, sid: string): Bytes {
  return encodeJsonRpc(
    rpcPayload({
      op: RpcOp.RequestResponse,
      requestId,
      statusCode: code,
      jsonSid: sid
    })
  );
}

export function buildHelloJson(): Bytes {
  return toBytes(
    JSON.stringify({ sid: "", op: RpcOp.Hello, d: { axtpVersion: AXTP_SPEC_VERSION } })
  );
}

export function buildIdentifyJson(randomSeed: number, eventMasks: string): Bytes {
  const d: Record<string, unknown> = { randomSeed: randomSeed >>> 0 };
  if (eventMasks) d.eventMasks = eventMasks;
  return toBytes(JSON.stringify({ sid: "", op: RpcOp.Identify, d }));
}

export function buildIdentifiedJson(sid: string): Bytes {
  return toBytes(JSON.stringify({ sid, op: RpcOp.Identified, d: {} }));
}
