// Handshake：RPC 会话状态机（Hello/Identify/Identified），core/handshake。
// 行为对齐原 session/handshake/handshake.ts（去 Orchestrator 薄包装，由 Core transform 编排发送）。

import { describe, expect, it } from "vitest";
import { AXTP_SPEC_VERSION } from "../../src/protocol/generated/axtpVersion.js";
import {
  RpcOp,
  helloMsg,
  identifyMsg,
  identifiedMsg,
  type IdentifyPayload
} from "../../src/protocol/model.js";
import { Handshake } from "../../src/core/handshake.js";

describe("Handshake — Logical Server", () => {
  it("onLinkReady → FRAMING_READY；startHello 产出空 sid + 协议版本", () => {
    const h = new Handshake("server", 1);
    h.onLinkReady();
    expect(h.state).toBe("FRAMING_READY");
    const hello = h.startHello();
    expect(hello.op).toBe(RpcOp.Hello);
    expect(hello.sid).toBe("");
    expect(hello.axtpVersion).toBe(AXTP_SPEC_VERSION);
  });

  it("handle(Identify) → 回 Identified、becameReady、sid 为 8 位 hex 非零", () => {
    const h = new Handshake("server", 0x12345678);
    h.onLinkReady();
    const identify = identifyMsg("", 0x00000001);
    const r = h.handle(identify);
    expect(r.becameReady).toBe(true);
    expect(r.outbound?.op).toBe(RpcOp.Identified);
    expect(h.state).toBe("APP_READY");
    expect(h.sid).toMatch(/^[0-9a-fA-F]{8}$/);
    expect(h.sid).not.toBe("00000000");
  });

  it("sid 由 randomSeed ⊕ 本地熵 决定（同种子确定性）", () => {
    const seed = 0xabcdef00;
    const make = (): string => {
      const h = new Handshake("server", seed);
      h.onLinkReady();
      return h.handle(identifyMsg("", 0x11223344)).outbound?.op === RpcOp.Identified ? h.sid : "";
    };
    expect(make()).toBe(make());
  });
});

describe("Handshake — Logical Client", () => {
  it("handle(Hello 兼容版本) → 回 Identify、未 ready", () => {
    const h = new Handshake("client", 1);
    h.onLinkReady();
    const r = h.handle(helloMsg("", "1.0.0"));
    expect(r.becameReady).toBe(false);
    expect(r.outbound?.op).toBe(RpcOp.Identify);
  });

  it("handle(Hello 不兼容主版本) → error", () => {
    const h = new Handshake("client", 1);
    h.onLinkReady();
    const r = h.handle(helloMsg("", "2.0.0"));
    expect(r.error).toBeDefined();
    expect(r.outbound).toBeUndefined();
  });

  it("handle(Hello 缺版本) → error", () => {
    const h = new Handshake("client", 1);
    h.onLinkReady();
    expect(h.handle(helloMsg("", "")).error).toBeDefined();
  });

  it("handle(Identified 合法 sid) → becameReady、sid 记录", () => {
    const h = new Handshake("client", 1);
    h.onLinkReady();
    h.handle(helloMsg("", "1.0.0")); // 推进到等 Identified
    const r = h.handle(identifiedMsg("1234abcd"));
    expect(r.becameReady).toBe(true);
    expect(h.sid).toBe("1234abcd");
    expect(h.state).toBe("APP_READY");
  });

  it("handle(Identified 非法 sid) → error（zero / 非 hex / 长度错）", () => {
    const h = () => {
      const x = new Handshake("client", 1);
      x.onLinkReady();
      x.handle(helloMsg("", "1.0.0"));
      return x;
    };
    expect(h().handle(identifiedMsg("00000000")).error).toBeDefined();
    expect(h().handle(identifiedMsg("nothex!!")).error).toBeDefined();
    expect(h().handle(identifiedMsg("123")).error).toBeDefined();
  });
});

describe("Handshake — 角色隔离与重置", () => {
  it("server 收 Hello / client 收 Identify 不处理（becameReady false、无 outbound）", () => {
    const s = new Handshake("server", 1);
    expect(s.handle(helloMsg("", "1.0.0"))).toEqual({ becameReady: false });
    const c = new Handshake("client", 1);
    const ic = c.handle(identifyMsg("", 1) as IdentifyPayload);
    expect(ic.becameReady).toBe(false);
    expect(ic.outbound).toBeUndefined();
  });

  it("reset → 回 LINK_CONNECTED、sid 清空（eventMasks 保留供重连）", () => {
    const h = new Handshake("server", 1, "deadbeef");
    h.onLinkReady();
    h.handle(identifyMsg("", 1));
    expect(h.isReady).toBe(true);
    h.reset();
    expect(h.state).toBe("LINK_CONNECTED");
    expect(h.sid).toBe("");
    expect(h.isReady).toBe(false);
  });
});
