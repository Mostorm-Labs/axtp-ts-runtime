// runtimeGate：spec 20-core.md:87-94 Runtime Gate 的纯决策。
//
// Gate 状态（协议语义，由 Core 持有；与 Endpoint 的连接生命周期正交）：
//   LINK_CONNECTED → FRAMING_READY → APP_READY → CLOSING
// classifyInbound 把"按当前 gate state + 入站 RPC op 决定如何处置"做成纯函数，
// 供 Core inbound transform 调用，便于隔离单测。

import { RpcOp } from "../protocol/model.js";

export type GateState = "LINK_CONNECTED" | "FRAMING_READY" | "APP_READY" | "CLOSING";

export type InboundDisposition =
  | { kind: "handshake" } // 路由到 handshake 状态机（Hello/Identify/Identified）
  | { kind: "business" } // APP_READY 业务：Request→broker / Response→pending / Event→broker
  | { kind: "respond-open-required" } // pre-APP_READY 的 Request → 回 ControlOpenRequired
  | { kind: "drop" }; // 非 APP_READY 的非 Request（Event/Response）→ 丢弃

/** APP_READY 才允许业务流量（Request/Response/Event/Stream）。 */
export function allowsBusiness(state: GateState): boolean {
  return state === "APP_READY";
}

/** 按当前 gate state 决定入站 RPC op 的处置。 */
export function classifyInbound(state: GateState, op: RpcOp): InboundDisposition {
  if (op === RpcOp.Hello || op === RpcOp.Identify || op === RpcOp.Identified) {
    return { kind: "handshake" };
  }
  if (state === "APP_READY") {
    return { kind: "business" };
  }
  return op === RpcOp.Request ? { kind: "respond-open-required" } : { kind: "drop" };
}
