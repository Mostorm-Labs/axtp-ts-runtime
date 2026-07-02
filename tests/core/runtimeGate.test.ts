// runtimeGate：spec 20-core.md:87-94 Runtime Gate 的纯决策。
// 把原 Session.ingest 里的"按 gate state + op 决定如何处置入站 RPC"抽成可单测的纯函数。

import { describe, expect, it } from "vitest";
import { RpcOp } from "../../src/protocol/model.js";
import { classifyInbound, type GateState } from "../../src/core/runtimeGate.js";

const ALL_STATES: GateState[] = ["LINK_CONNECTED", "FRAMING_READY", "APP_READY", "CLOSING"];

describe("runtimeGate classifyInbound", () => {
  it("handshake op（Hello/Identify/Identified）在任意 state 都路由到 handshake", () => {
    for (const s of ALL_STATES) {
      expect(classifyInbound(s, RpcOp.Hello).kind).toBe("handshake");
      expect(classifyInbound(s, RpcOp.Identify).kind).toBe("handshake");
      expect(classifyInbound(s, RpcOp.Identified).kind).toBe("handshake");
    }
  });

  it("APP_READY：Request/RequestResponse/Event → business", () => {
    expect(classifyInbound("APP_READY", RpcOp.Request).kind).toBe("business");
    expect(classifyInbound("APP_READY", RpcOp.RequestResponse).kind).toBe("business");
    expect(classifyInbound("APP_READY", RpcOp.Event).kind).toBe("business");
  });

  it("非 APP_READY 的 Request → respond-open-required（spec: request-before-identified）", () => {
    expect(classifyInbound("LINK_CONNECTED", RpcOp.Request).kind).toBe("respond-open-required");
    expect(classifyInbound("FRAMING_READY", RpcOp.Request).kind).toBe("respond-open-required");
    expect(classifyInbound("CLOSING", RpcOp.Request).kind).toBe("respond-open-required");
  });

  it("非 APP_READY 的 Event/RequestResponse → drop", () => {
    expect(classifyInbound("FRAMING_READY", RpcOp.Event).kind).toBe("drop");
    expect(classifyInbound("FRAMING_READY", RpcOp.RequestResponse).kind).toBe("drop");
    expect(classifyInbound("LINK_CONNECTED", RpcOp.Event).kind).toBe("drop");
  });
});
