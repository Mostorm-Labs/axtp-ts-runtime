// @axtp/ts-sdk/transport：transport 契约子入口（Stream 体系 + profile 能力模型）。
export {
  framedBinaryProfile,
  keepaliveMode,
  supportsControl,
  supportsStream,
  unframedJsonProfile
} from "./transport/contract.js";
export type {
  FrameMode,
  KeepaliveMode,
  KeepaliveStreamTransport,
  LogicalRole,
  PhysicalRole,
  StreamClientTransport,
  StreamServerTransport,
  StreamTransport,
  TransportProfile,
  TransportProfileId
} from "./transport/contract.js";
