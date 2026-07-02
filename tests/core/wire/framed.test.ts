// FramedWireAdapter：framed-binary 的 codec 编排（FrameDecoder+Reassembler+Fragmenter+Encoder）。
// 入站 bytes→(control body/rpc/stream)，出站 message→成帧字节块。移植自 FramedLink 的 codec 部分（去心跳/事件流）。

import { describe, expect, it } from "vitest";
import { concatBytes } from "../../../src/io/bytes.js";
import {
  eventMsg,
  requestMsg,
  type RpcMessage,
  type StreamPayload
} from "../../../src/protocol/model.js";
import { FramedWireAdapter } from "../../../src/core/wire/framed.js";
import type { WireSink } from "../../../src/core/wire/adapter.js";

function capture() {
  const rpc: RpcMessage[] = [];
  const ctrl: Uint8Array[] = [];
  const streams: StreamPayload[] = [];
  let errs = 0;
  const sink: WireSink = {
    onControl: (b) => ctrl.push(b),
    onRpc: (m) => rpc.push(m),
    onStream: (m) => streams.push(m),
    onError: () => (errs += 1)
  };
  return {
    sink,
    rpc,
    ctrl,
    streams,
    get errs() {
      return errs;
    }
  };
}

describe("FramedWireAdapter — RPC roundtrip", () => {
  it("encodeRpc → feedBytes 回环：解出原 Request", () => {
    const a = new FramedWireAdapter(4096);
    const msg = requestMsg("12345678", 5, "audio.getAlgorithmConfig", { profile: "music" });
    const c = capture();
    a.feedBytes(concatBytes(a.encodeRpc(msg)), c.sink);
    expect(c.rpc).toHaveLength(1);
    expect(c.rpc[0]).toMatchObject({ op: msg.op, method: msg.method, requestId: 5 });
  });

  it("encodeRpc → feedBytes 回环：解出原 Event", () => {
    const a = new FramedWireAdapter(4096);
    const msg = eventMsg("abcdef01", "cast.windowStateChanged", { win: 1 });
    const c = capture();
    a.feedBytes(concatBytes(a.encodeRpc(msg)), c.sink);
    expect(c.rpc).toHaveLength(1);
    expect(c.rpc[0]).toMatchObject({ op: msg.op, eventName: msg.eventName });
  });

  it("分片：body > 单帧容量 → 多帧 → 重组正确", () => {
    const a = new FramedWireAdapter(32); // capacity = 32 - 14 = 18 字节
    const big = requestMsg("12345678", 1, "m", { data: "x".repeat(200) });
    const chunks = a.encodeRpc(big);
    expect(chunks.length).toBeGreaterThan(1);
    const c = capture();
    a.feedBytes(concatBytes(chunks), c.sink);
    expect(c.rpc).toHaveLength(1);
    expect((c.rpc[0] as { method: string }).method).toBe("m");
  });

  it("分片字节分多次到达（流式）也能重组", () => {
    const a = new FramedWireAdapter(4096);
    const msg = requestMsg("12345678", 7, "big", { data: "y".repeat(100) });
    const all = concatBytes(a.encodeRpc(msg));
    const c = capture();
    // 逐字节投喂
    for (let i = 0; i < all.length; i++) a.feedBytes(all.slice(i, i + 1), c.sink);
    expect(c.rpc).toHaveLength(1);
  });
});

describe("FramedWireAdapter — CONTROL / STREAM", () => {
  it("encodeControlBody → feedBytes → onControl 收到原 body", () => {
    const a = new FramedWireAdapter(4096);
    const body = new Uint8Array([1, 2, 3, 4, 5]);
    const c = capture();
    a.feedBytes(concatBytes(a.encodeControlBody(body)), c.sink);
    expect(c.ctrl).toHaveLength(1);
    expect(Array.from(c.ctrl[0])).toEqual([1, 2, 3, 4, 5]);
  });

  it("encodeStream → feedBytes → onStream 解出字段", () => {
    const a = new FramedWireAdapter(4096);
    const sp: StreamPayload = { streamId: 7, seqId: 3, cursor: 0n, data: new Uint8Array([9, 9]) };
    const c = capture();
    a.feedBytes(concatBytes(a.encodeStream(sp)), c.sink);
    expect(c.streams).toHaveLength(1);
    expect(c.streams[0].streamId).toBe(7);
    expect(c.streams[0].seqId).toBe(3);
    expect(Array.from(c.streams[0].data)).toEqual([9, 9]);
  });
});

describe("FramedWireAdapter — 健壮性", () => {
  it("非 magic 垃圾后接合法帧：resync 静默丢弃，合法帧仍可解", () => {
    const a = new FramedWireAdapter(4096);
    const msg = requestMsg("12345678", 1, "m", {});
    const good = concatBytes(a.encodeRpc(msg));
    const garbage = new Uint8Array([0x00, 0x11, 0x22]); // 非 magic，resync 静默丢弃
    const c = capture();
    a.feedBytes(concatBytes([garbage, good]), c.sink);
    expect(c.rpc).toHaveLength(1);
  });

  it("畸形帧（magic + 非法 version）→ onError", () => {
    const a = new FramedWireAdapter(4096);
    // magic(0x41,0x58) + version=0xFF + 其余 header/crc 占位（共 14B）
    const malformed = new Uint8Array([0x41, 0x58, 0xff, 0x01, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0]);
    const c = capture();
    a.feedBytes(malformed, c.sink);
    expect(c.errs).toBeGreaterThan(0);
  });

  it("setMaxFrameSize 改变分片粒度", () => {
    const a = new FramedWireAdapter(4096);
    const msg = requestMsg("12345678", 1, "m", { data: "z".repeat(500) });
    expect(a.encodeRpc(msg).length).toBe(1);
    a.setMaxFrameSize(32);
    expect(a.encodeRpc(msg).length).toBeGreaterThan(1);
  });
});
