// @axtp/ts-sdk 主入口：稳定 SDK API（聚合全部子入口，向后兼容）。
// 推荐按需从子入口导入以减小打包体积：./node、./protocol、./transport、./mock、./io。
// 用户主入口：AxtpClient / AxtpServer / AxtpSession / Stream / 类型 / 错误。

// ===== SDK 核心 =====
export { AxtpClient } from "./sdk/client.js";
export type { ClientOptions, ClientState } from "./sdk/client.js";
export { AxtpServer } from "./sdk/server.js";
export type { ServerOptions } from "./sdk/server.js";
export { AxtpSession } from "./session/session.js";
export type {
  CallContext,
  CallOptions,
  CommonOptions,
  EventHandler,
  GlobalHandlerSource,
  MethodHandler,
  SessionCloseInfo,
  SessionConfig,
  SessionLifecycleState,
  UntypedEventHandler,
  UntypedMethodHandler
} from "./session/session.js";

// ===== Stream =====
export { Stream } from "./session/stream/stream.js";
export type { StreamStats } from "./session/stream/stream.js";

// ===== 连接 / 重连 =====
export type { ConnectionOptions, ConnectionState } from "./connection/connection.js";
export type { ReconnectInfo, ReconnectPolicy } from "./connection/reconnect/reconnect.js";

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

// ===== 错误 =====
export { AxtpError, connectionClosedError, ErrorCode, notReadyError } from "./types/error.js";

// ===== 事件流 =====
export { EventStream } from "./types/events.js";

// ===== 高级子入口再导出（保持主入口全量向后兼容）=====
export * from "./protocol.js";
export * from "./transport.js";
export * from "./node.js";
export * from "./mock.js";
export * from "./io.js";
