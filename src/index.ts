// @axtp/runtime 主入口（重构后）。
// 用户主入口：AxtpClient / AxtpServer / Session / Stream / 类型 / 错误。
// Node 传输在 ./node 子入口。

// SDK
export { AxtpClient } from "./sdk/client.js";
export type { ClientOptions } from "./sdk/client.js";
export { AxtpServer } from "./sdk/server.js";
export type { ServerOptions } from "./sdk/server.js";
export { AxtpSession } from "./session/session.js";
export type {
  CallContext,
  CallOptions,
  EventHandler,
  MethodHandler,
  SessionOptions,
  UntypedEventHandler,
  UntypedMethodHandler
} from "./session/session.js";

// Stream
export { Stream } from "./session/stream/stream.js";
export type { StreamStats } from "./session/stream/stream.js";

// 重连
export type { ReconnectInfo, ReconnectPolicy } from "./connection/reconnect.js";

// 类型（单一事实源）
export {
  computeEventMasks,
  EVENT_REGISTRY,
  isEventSubscribed,
  METHOD_REGISTRY,
  registry,
  type EventId,
  type EventName,
  type EventPayload,
  type MethodId,
  type MethodName,
  type MethodRequest,
  type MethodResponse
} from "./types/registry.js";

// 错误
export { AxtpError, connectionClosedError, ErrorCode, notReadyError } from "./types/error.js";

// 事件流
export { EventStream } from "./types/events.js";

// 角色（Physical 驱动 CONTROL OPEN/ACCEPT，Logical 驱动 RPC Hello；二者正交，用于 Cloud Reverse 拓扑）
export type { LogicalRole, PhysicalRole } from "./transport/transport.js";

// 协议常量（供高级用户，从 model.ts 中转，不直连 generated）
export { ControlOpcode, PayloadType, RpcOp } from "./protocol/model.js";

// IO（Bytes 类型）
export { bytesToHex, bytesToText, concatBytes, hexToBytes, toBytes } from "./io/bytes.js";
export type { Bytes } from "./io/bytes.js";
