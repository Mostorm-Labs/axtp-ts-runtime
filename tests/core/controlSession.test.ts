// ControlSession：framed 链路层状态机（OPEN/ACCEPT/HEARTBEAT/CLOSE），core/controlSession。
// 行为对齐原 connection/link/controlSession.ts（纯逻辑 + 回调，由 Core transform 注入回调）。

import { describe, expect, it } from "vitest";
import {
  clampHeartbeatInterval,
  defaultOpenParams,
  decodeControl,
  encodeAccept,
  encodeClose,
  encodeHeartbeat,
  encodeHeartbeatAck,
  encodeOpen,
  encodeRejectedAccept,
  type NegotiationParams
} from "../../src/protocol/codec/control.js";
import { ControlOpcode, ErrorCode, RpcEncoding } from "../../src/protocol/model.js";
import {
  ControlSession,
  type ControlSessionCallbacks,
  type NegotiatedLink
} from "../../src/core/controlSession.js";

interface Captured {
  sends: Uint8Array[];
  ready: NegotiatedLink[];
  rejected: number[];
  heartbeats: number[];
  acks: number[];
  closing: number[];
  errors: number;
}

function newCaptured(): { cb: ControlSessionCallbacks; cap: Captured } {
  const cap: Captured = {
    sends: [],
    ready: [],
    rejected: [],
    heartbeats: [],
    acks: [],
    closing: [],
    errors: 0
  };
  const cb: ControlSessionCallbacks = {
    onSendBytes: (b) => cap.sends.push(b),
    onLinkReady: (n) => cap.ready.push(n),
    onOpenRejected: (sc) => cap.rejected.push(sc),
    onHeartbeat: (cid) => cap.heartbeats.push(cid),
    onHeartbeatAck: (cid) => cap.acks.push(cid),
    onClosing: (cid) => cap.closing.push(cid),
    onError: () => (cap.errors += 1)
  };
  return { cb, cap };
}

const goodPeerParams: NegotiationParams = {
  maxFrameSize: 4096,
  supportedPayloadTypes: 0x07,
  supportedRpcEncodings: RpcEncoding.Json,
  heartbeatIntervalMs: 1000,
  ackMode: 0
};

describe("ControlSession — Physical Server", () => {
  it("handleOpen(合法) → 回 ACCEPT、onLinkReady(accepted)、state=open", () => {
    const { cb, cap } = newCaptured();
    const cs = new ControlSession("server", cb, defaultOpenParams(8192, 1000));
    cs.handleControlBody(encodeOpen(1, goodPeerParams));
    expect(cap.ready).toHaveLength(1);
    expect(cap.ready[0].accepted).toBe(true);
    expect(cap.ready[0].selectedRpcEncoding).toBe(RpcEncoding.Json);
    // maxFrameSize 取双方较小：min(8192, 4096)=4096
    expect(cap.ready[0].maxFrameSize).toBe(4096);
    expect(cap.sends).toHaveLength(1);
    expect(decodeControl(cap.sends[0]).opcode).toBe(ControlOpcode.Accept);
    expect(cs.isOpen).toBe(true);
  });

  it("handleOpen(缺必需 TLV) → 回带非零 statusCode 的 ACCEPT（拒绝）", () => {
    const { cb, cap } = newCaptured();
    const cs = new ControlSession("server", cb);
    const partial = encodeOpen(1, { ...goodPeerParams, supportedRpcEncodings: undefined });
    cs.handleControlBody(partial);
    expect(cap.ready).toHaveLength(0);
    const acc = decodeControl(cap.sends[0]);
    expect(acc.opcode).toBe(ControlOpcode.Accept);
    expect(acc.statusCode).not.toBe(ErrorCode.Success);
  });

  it("handleOpen(对端不支持 JSON) → 拒绝", () => {
    const { cb, cap } = newCaptured();
    const cs = new ControlSession("server", cb);
    cs.handleControlBody(encodeOpen(1, { ...goodPeerParams, supportedRpcEncodings: 0x00 }));
    expect(decodeControl(cap.sends[0]).statusCode).not.toBe(ErrorCode.Success);
  });

  it("handleOpen(maxFrameSize<15) → 拒绝", () => {
    const { cb, cap } = newCaptured();
    const cs = new ControlSession("server", cb);
    cs.handleControlBody(encodeOpen(1, { ...goodPeerParams, maxFrameSize: 10 }));
    expect(decodeControl(cap.sends[0]).statusCode).not.toBe(ErrorCode.Success);
  });
});

describe("ControlSession — Physical Client", () => {
  it("sendOpen → 发 OPEN；handleAccept(成功, controlId 匹配) → onLinkReady", () => {
    const { cb, cap } = newCaptured();
    const cs = new ControlSession("client", cb, defaultOpenParams(4096, 1000));
    cs.sendOpen();
    expect(cap.sends).toHaveLength(1);
    const openControlId = decodeControl(cap.sends[0]).controlId;
    // 对端回 ACCEPT（带 selectedRpcEncoding）
    cs.handleControlBody(
      encodeAccept(openControlId, {
        ...defaultOpenParams(4096, 1000),
        selectedRpcEncoding: RpcEncoding.Json
      })
    );
    expect(cap.ready).toHaveLength(1);
    expect(cap.ready[0].accepted).toBe(true);
    expect(cs.isOpen).toBe(true);
  });

  it("handleAccept(非零 statusCode) → onOpenRejected", () => {
    const { cb, cap } = newCaptured();
    const cs = new ControlSession("client", cb);
    cs.sendOpen();
    const openControlId = decodeControl(cap.sends[0]).controlId;
    cs.handleControlBody(encodeRejectedAccept(openControlId, ErrorCode.ControlNegotiationFailed));
    expect(cap.rejected).toEqual([ErrorCode.ControlNegotiationFailed]);
    expect(cap.ready).toHaveLength(0);
  });

  it("handleAccept(controlId 不匹配) → 忽略", () => {
    const { cb, cap } = newCaptured();
    const cs = new ControlSession("client", cb);
    cs.sendOpen();
    cs.handleControlBody(encodeAccept(9999, defaultOpenParams()));
    expect(cap.ready).toHaveLength(0);
  });
});

describe("ControlSession — HEARTBEAT / CLOSE", () => {
  it("HEARTBEAT → onHeartbeat(cid)；HEARTBEAT_ACK → onHeartbeatAck(cid)", () => {
    const { cb, cap } = newCaptured();
    const cs = new ControlSession("server", cb);
    cs.handleControlBody(encodeHeartbeat(42));
    expect(cap.heartbeats).toEqual([42]);
    cs.handleControlBody(encodeHeartbeatAck(99));
    expect(cap.acks).toEqual([99]);
  });

  it("CLOSE → 回 CLOSE_ACK + onClosing", () => {
    const { cb, cap } = newCaptured();
    const cs = new ControlSession("server", cb);
    cs.handleControlBody(encodeClose(5));
    expect(decodeControl(cap.sends[0]).opcode).toBe(ControlOpcode.CloseAck);
    expect(cap.closing).toEqual([5]);
  });

  it("allocControlId 从 1 递增、&0xffff 回绕", () => {
    const { cb } = newCaptured();
    const cs = new ControlSession("server", cb);
    expect(cs.allocControlId()).toBe(1);
    expect(cs.allocControlId()).toBe(2);
  });
});

describe("ControlSession — heartbeat clamp", () => {
  it("协商 heartbeat 取 peer 值并 clamp 到合法范围", () => {
    const { cb, cap } = newCaptured();
    const cs = new ControlSession("server", cb, defaultOpenParams(4096, 1000));
    cs.handleControlBody(encodeOpen(1, { ...goodPeerParams, heartbeatIntervalMs: 10 }));
    // peer 值 10 < 500 → clamp 到 500
    expect(cap.ready[0].heartbeatIntervalMs).toBe(clampHeartbeatInterval(10));
  });
});
