// @axtp/runtime/transport：自定义 transport 契约子入口。
// ITransport / IServerTransport 全部类型、TransportProfile 能力模型与 CloseCode。
export {
  CloseCode,
  framedBinaryProfile,
  keepaliveMode,
  supportsControl,
  supportsStream,
  unframedJsonProfile
} from "./transport/contract.js";
export type {
  CloseReason,
  FrameMode,
  IClientTransport,
  IServerTransport,
  ITransport,
  KeepaliveMode,
  KeepaliveTransport,
  LogicalRole,
  PhysicalRole,
  TransportFactory,
  TransportProfile,
  TransportProfileId
} from "./transport/contract.js";
