// AxtpCore 集成：两条 TransformStream 的端到端验证。
// framed server：OPEN→ACCEPT+linkReady+Hello、Identify→Identified+handshakeReady、APP_READY Request→rpcRequest、
// pre-ready Request→ControlOpenRequired；unframed client：markLinkReady→linkReady、Hello→Identify。
//
// 测试模式：持有 inbound writer 做 fire-and-forget 写（不 await——避免 TransformStream 默认 HWM=1 下
// "await write + 无并发 reader" 的 backpressure 死锁；真实使用 pipeThrough 并发读写无此问题），
// reader.read() 自然等待 transform 产出的 chunk。

import { describe, expect, it } from "vitest";
import { concatBytes, type Bytes } from "../../src/io/bytes.js";
import { decodeControl, defaultOpenParams, encodeOpen } from "../../src/protocol/codec/control.js";
import { encodeJsonRpc } from "../../src/protocol/codec/jsonRpc.js";
import { AxtpCore } from "../../src/core/core.js";
import { FramedWireAdapter } from "../../src/core/wire/framed.js";
import type { CoreEvent } from "../../src/core/events.js";
import type { WireSink } from "../../src/core/wire/adapter.js";
import {
  ControlOpcode,
  RpcOp,
  helloMsg,
  identifiedMsg,
  identifyMsg,
  requestMsg,
  type RpcMessage,
  type StreamPayload
} from "../../src/protocol/model.js";
import { framedBinaryProfile, unframedJsonProfile } from "../../src/transport/profile.js";

async function readEvents(core: AxtpCore, n: number): Promise<CoreEvent[]> {
  const r = core.inbound.readable.getReader();
  const out: CoreEvent[] = [];
  for (let i = 0; i < n; i++) out.push((await r.read()).value as CoreEvent);
  r.releaseLock();
  return out;
}

async function readOut(core: AxtpCore, n: number): Promise<Bytes[]> {
  const r = core.outbound.readable.getReader();
  const out: Bytes[] = [];
  for (let i = 0; i < n; i++) out.push((await r.read()).value as Bytes);
  r.releaseLock();
  return out;
}

function decodeFrame(bytes: Bytes): {
  control?: Uint8Array;
  rpc?: RpcMessage;
  stream?: StreamPayload;
} {
  const got: { control?: Uint8Array; rpc?: RpcMessage; stream?: StreamPayload } = {};
  const sink: WireSink = {
    onControl: (b) => (got.control = b),
    onRpc: (m) => (got.rpc = m),
    onStream: (m) => (got.stream = m),
    onError: () => {}
  };
  new FramedWireAdapter(4096).feedBytes(bytes, sink);
  return got;
}

function framedServer(
  overrides: Partial<ConstructorParameters<typeof AxtpCore>[0]> = {}
): AxtpCore {
  return new AxtpCore({
    profile: framedBinaryProfile("AXTP-TCP"),
    physicalRole: "server",
    logicalRole: "server",
    maxFrameSize: 4096,
    heartbeatIntervalMs: 1000,
    handshakeSeed: 0x12345678,
    ...overrides
  });
}

describe("AxtpCore — framed server 端到端", () => {
  it("收 OPEN → 发 ACCEPT + linkReady + 自动发 Hello", async () => {
    const core = framedServer();
    const w = core.inbound.writable.getWriter();
    void w.write(
      concatBytes(
        new FramedWireAdapter(4096).encodeControlBody(encodeOpen(1, defaultOpenParams(4096, 1000)))
      )
    );

    expect((await readEvents(core, 1))[0].kind).toBe("linkReady");
    const outs = await readOut(core, 2);
    expect(decodeControl(decodeFrame(outs[0]).control!).opcode).toBe(ControlOpcode.Accept);
    expect(decodeFrame(outs[1]).rpc?.op).toBe(RpcOp.Hello);
  });

  it("OPEN→Identify → handshakeReady(sid) + 发 Identified + isAppReady", async () => {
    const core = framedServer({ handshakeSeed: 0xabcdef00 });
    const peer = new FramedWireAdapter(4096);
    const w = core.inbound.writable.getWriter();
    void w.write(concatBytes(peer.encodeControlBody(encodeOpen(1, defaultOpenParams(4096, 1000)))));
    await readEvents(core, 1); // linkReady
    await readOut(core, 2); // ACCEPT + Hello

    void w.write(concatBytes(peer.encodeRpc(identifyMsg("", 0x11223344))));
    const ev = await readEvents(core, 1);
    expect(ev[0].kind).toBe("handshakeReady");
    const sid = ev[0].kind === "handshakeReady" ? ev[0].sid : "";
    expect(sid).toMatch(/^[0-9a-fA-F]{8}$/);
    expect(decodeFrame((await readOut(core, 1))[0]).rpc?.op).toBe(RpcOp.Identified);
    expect(core.isAppReady).toBe(true);
  });

  it("APP_READY：收 Request → rpcRequest 事件", async () => {
    const core = framedServer({ handshakeSeed: 1 });
    const peer = new FramedWireAdapter(4096);
    const w = core.inbound.writable.getWriter();
    void w.write(concatBytes(peer.encodeControlBody(encodeOpen(1, defaultOpenParams()))));
    await readEvents(core, 1);
    await readOut(core, 2);
    void w.write(concatBytes(peer.encodeRpc(identifyMsg("", 7))));
    const ready = await readEvents(core, 1);
    const sid = ready[0].kind === "handshakeReady" ? ready[0].sid : "";
    await readOut(core, 1); // Identified

    void w.write(concatBytes(peer.encodeRpc(requestMsg(sid, 99, "audio.get", { x: 1 }))));
    expect((await readEvents(core, 1))[0]).toMatchObject({ kind: "rpcRequest" });
  });

  it("pre-ready（FRAMING_READY）Request → 自动回 ControlOpenRequired", async () => {
    const core = framedServer({ handshakeSeed: 1 });
    const peer = new FramedWireAdapter(4096);
    const w = core.inbound.writable.getWriter();
    void w.write(concatBytes(peer.encodeControlBody(encodeOpen(1, defaultOpenParams()))));
    await readEvents(core, 1); // linkReady（FRAMING_READY，未 APP_READY）
    await readOut(core, 2); // ACCEPT + Hello

    void w.write(concatBytes(peer.encodeRpc(requestMsg("", 42, "audio.get", {}))));
    expect(decodeFrame((await readOut(core, 1))[0]).rpc?.op).toBe(RpcOp.RequestResponse);
  });
});

describe("AxtpCore — unframed client 端到端", () => {
  it("markLinkReady → linkReady（client 不发 Hello）；收 Hello → 发 Identify", async () => {
    const core = new AxtpCore({
      profile: unframedJsonProfile(),
      physicalRole: "client",
      logicalRole: "client",
      maxFrameSize: 4096,
      heartbeatIntervalMs: 1000
    });
    core.markLinkReady();
    expect((await readEvents(core, 1))[0].kind).toBe("linkReady");

    const w = core.inbound.writable.getWriter();
    void w.write(encodeJsonRpc(helloMsg("", "1.0.0")));
    const outs = await readOut(core, 1);
    expect(JSON.parse(new TextDecoder().decode(outs[0])).op).toBe(RpcOp.Identify);
  });

  it("收 Identified → handshakeReady", async () => {
    const core = new AxtpCore({
      profile: unframedJsonProfile(),
      physicalRole: "client",
      logicalRole: "client",
      maxFrameSize: 4096,
      heartbeatIntervalMs: 1000
    });
    core.markLinkReady();
    await readEvents(core, 1); // linkReady
    const w = core.inbound.writable.getWriter();
    void w.write(encodeJsonRpc(helloMsg("", "1.0.0")));
    await readOut(core, 1); // Identify
    void w.write(encodeJsonRpc(identifiedMsg("1234abcd")));
    const ev = await readEvents(core, 1);
    expect(ev[0]).toMatchObject({ kind: "handshakeReady", sid: "1234abcd" });
    expect(core.isAppReady).toBe(true);
  });
});
