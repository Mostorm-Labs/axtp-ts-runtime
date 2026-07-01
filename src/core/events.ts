// core/events.ts — Core inbound/outbound 流的载荷类型。
//
// CoreEvent：inbound TransformStream<Bytes, CoreEvent> 的输出（Endpoint reader 消费）。
//   rpcResponse 不在此——响应在 inbound transform 内直接 pendingCalls.resolve（不外露）。
//
// OutboundMessage：outbound TransformStream<OutboundMessage, Bytes> 的输入。
//   Core 内部独占 outbound writer：Endpoint 显式 send* 与 inbound 自动响应都经此。

import type { RequestPayload, EventPayload, StreamPayload, RpcMessage } from "../protocol/model.js";
import type { AxtpError } from "../types/error.js";

export type CoreEvent =
  | { kind: "rpcRequest"; msg: RequestPayload }
  | { kind: "rpcEvent"; msg: EventPayload }
  | { kind: "streamData"; msg: StreamPayload }
  | { kind: "linkReady"; heartbeatIntervalMs: number }
  | { kind: "linkOpenRejected"; statusCode: number }
  | { kind: "linkClosing" }
  | { kind: "handshakeReady"; sid: string }
  | { kind: "handshakeError"; err: AxtpError }
  | { kind: "heartbeatAck" }
  | { kind: "error"; err: AxtpError };

export type OutboundMessage =
  | { kind: "rpc"; msg: RpcMessage }
  | { kind: "controlBody"; body: Uint8Array }
  | { kind: "stream"; msg: StreamPayload };
