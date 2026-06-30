// @axtp/runtime/protocol：高级协议层子入口（payload 模型 + 常量 + 工厂）。
// 供需要直接操作 frame / payload 的高级用户；普通业务用 SDK 主入口即可。
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
