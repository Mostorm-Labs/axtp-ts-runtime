// @axtp/runtime 主入口。
// 用户主入口：AxtpClient / AxtpServer / Session / Stream / 类型 / 错误 / Node 传输。

// SDK
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

// Stream
export { Stream } from "./session/stream/stream.js";
export type { StreamStats } from "./session/stream/stream.js";

// 重连 + 连接状态
export type { ConnectionState } from "./connection/connection.js";
export type { ReconnectInfo, ReconnectPolicy } from "./connection/reconnect/reconnect.js";

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

// 传输契约（自定义 transport 实现必需）
export {
  CloseCode,
  framedBinaryCapabilities,
  unframedJsonCapabilities
} from "./transport/transport.js";
export type {
  CloseReason,
  IClientTransport,
  IServerTransport,
  ITransport,
  TransportCapabilities,
  TransportFactory
} from "./transport/transport.js";

// 协议常量 + payload 模型（供高级用户，从 model.ts 中转，不直连 generated）
export {
  ControlOpcode,
  controlPayload,
  PayloadType,
  RpcBodyEncoding,
  RpcEncoding,
  RpcOp,
  rpcPayload
} from "./protocol/model.js";
export type {
  ControlPayload,
  Frame,
  FrameHeader,
  Message,
  PayloadMeta,
  RpcPayload,
  StreamPayload
} from "./protocol/model.js";

// 连接选项（高级配置）
export type { ConnectionOptions } from "./connection/connection.js";

// IO（Bytes 类型）
export { bytesToHex, bytesToText, concatBytes, hexToBytes, toBytes } from "./io/bytes.js";
export type { Bytes } from "./io/bytes.js";

// Node 传输
export {
  NodeTcpClientTransport,
  NodeTcpServerTransport
} from "./transport/tcp/nodeTcpTransport.js";
export type { TcpOptions } from "./transport/tcp/nodeTcpTransport.js";
export { NodeWsClientTransport, NodeWsServerTransport } from "./transport/ws/nodeWsTransport.js";
export type { WsClientOptions, WsServerOptions } from "./transport/ws/nodeWsTransport.js";

// Mock（测试/开发用）
export {
  createMockTransportPair,
  MockClientTransport,
  MockServerTransport,
  MockTransport
} from "./transport/mock/mockTransport.js";
