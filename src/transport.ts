// @axtp/runtime/transport：自定义 transport 契约子入口。
// 实现 ITransport / IServerTransport 所需的全部类型、能力工厂与 CloseCode。
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
  LogicalRole,
  PhysicalRole,
  TransportCapabilities,
  TransportFactory
} from "./transport/transport.js";
