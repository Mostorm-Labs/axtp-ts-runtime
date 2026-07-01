// @axtp/ts-sdk/protocol：高级协议层子入口（payload 模型 + 常量 + 工厂）。
// 供需要直接操作 frame / payload 的高级用户；普通业务用 SDK 主入口即可。
export {
  ControlOpcode,
  controlPayload,
  eventMsg,
  helloMsg,
  identifiedMsg,
  identifyMsg,
  PayloadType,
  requestMsg,
  responseMsg,
  RpcBodyEncoding,
  RpcEncoding,
  RpcOp
} from "./protocol/model.js";
export type {
  ControlPayload,
  EventPayload,
  Frame,
  FrameHeader,
  HelloPayload,
  IdentifyPayload,
  IdentifiedPayload,
  Message,
  RequestPayload,
  ResponsePayload,
  RpcMessage,
  StreamPayload
} from "./protocol/model.js";
