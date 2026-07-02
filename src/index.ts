// @axtp/ts-sdk 主入口：Core/Broker/Endpoint 三层 + SDK 门面（Web Streams 数据管线）。
// 用户主入口：AxtpClient / AxtpServer / AxtpEndpoint / Stream / 类型 / 错误。

// ===== SDK 门面 =====
export { AxtpClient } from "./sdk/client.js";
export type { ClientOptions, ClientState } from "./sdk/client.js";
export { AxtpServer } from "./sdk/server.js";
export type { ServerOptions } from "./sdk/server.js";

// ===== Endpoint（高级构建块）=====
export { AxtpEndpoint } from "./endpoint/endpoint.js";
export type { EndpointLifecycle } from "./endpoint/endpoint.js";

// ===== Stream =====
export { Stream } from "./endpoint/stream.js";
export type { StreamStats } from "./endpoint/stream.js";

// ===== handler / call 类型 =====
export type { CallContext, CallOptions } from "./sdk/types.js";
export type {
  UntypedEventHandler,
  UntypedMethodHandler,
  GlobalHandlerSource
} from "./broker/context.js";

// ===== 重连 =====
export type { ReconnectPolicy } from "./endpoint/reconnect.js";

// ===== 注册表（单一事实源）=====
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

// ===== 错误 / 事件流 =====
export { AxtpError, connectionClosedError, ErrorCode, notReadyError } from "./types/error.js";
export { EventStream } from "./types/events.js";

// ===== 子入口再导出（按需引入减小打包）=====
export * from "./protocol.js";
export * from "./transport.js";
export * from "./node.js";
export * from "./mock.js";
export * from "./io.js";
