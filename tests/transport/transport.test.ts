import { describe, expect, it } from "vitest";
import type { Bytes } from "../../src/io/bytes.js";
import {
  createMockTransportPair,
  MockClientTransport,
  MockServerTransport
} from "../../src/transport/mock/mockTransport.js";
import { CloseCode } from "../../src/transport/transport.js";
import { EventStream } from "../../src/types/events.js";
import { computeEventMasks, isEventSubscribed } from "../../src/types/registry.js";

describe("EventStream", () => {
  it("subscribe / emit / unsubscribe", () => {
    const stream = new EventStream<number>();
    const received: number[] = [];
    const unsub = stream.subscribe((v) => received.push(v));
    stream.emit(1);
    stream.emit(2);
    unsub();
    stream.emit(3);
    expect(received).toEqual([1, 2]);
  });

  it("一个监听器抛错不影响其它监听器", () => {
    const stream = new EventStream<number>();
    const ok: number[] = [];
    stream.subscribe(() => {
      throw new Error("boom");
    });
    stream.subscribe((v) => ok.push(v));
    stream.emit(42);
    expect(ok).toEqual([42]);
  });

  it("emit 期间 unsubscribe 安全（延迟到结束）", () => {
    const stream = new EventStream<number>();
    const seen: number[] = [];
    const second: { current: (() => void) | undefined } = { current: undefined };
    stream.subscribe((v) => {
      seen.push(v);
      second.current?.();
    });
    second.current = stream.subscribe((v) => seen.push(v * 10));
    stream.emit(1);
    expect(seen).toEqual([1, 10]);
  });
});

describe("MockTransport", () => {
  it("pair 双向互通", async () => {
    const { left, right } = createMockTransportPair();
    const received: Bytes[] = [];
    right.onMessage.subscribe((b) => received.push(b));
    left.send(new Uint8Array([1, 2, 3]));
    left.send(new Uint8Array([4]));
    await new Promise((r) => setTimeout(r, 0)); // deliver 异步
    expect(received.length).toBe(2);
    expect([...received[0]]).toEqual([1, 2, 3]);
  });

  it("close 触发对端 onClose（remote=true）", async () => {
    const { left, right } = createMockTransportPair();
    let closed = false;
    let remote = false;
    right.onClose.subscribe((r) => {
      closed = true;
      remote = r.remote;
    });
    left.close(CloseCode.Normal, "bye");
    // 对端关闭是异步传播的（模拟真实网络），需 await microtask
    await new Promise((r) => setTimeout(r, 0));
    expect(closed).toBe(true);
    expect(remote).toBe(true);
    expect(left.isConnected()).toBe(false);
    expect(right.isConnected()).toBe(false);
  });

  it("pause/resume 控制投递时序", async () => {
    const { left, right } = createMockTransportPair();
    const received: number[] = [];
    right.onMessage.subscribe((b) => received.push(b.length));
    right.pause();
    left.send(new Uint8Array([1]));
    left.send(new Uint8Array([1, 2]));
    expect(received).toEqual([]);
    right.resume();
    // resume 现在异步投递（与 deliver 语义一致），需等 microtask
    await new Promise((r) => setTimeout(r, 0));
    expect(received).toEqual([1, 2]);
  });
});

describe("MockServerTransport 多连接", () => {
  it("accept 多个 client，每个产出独立 ITransport", async () => {
    const server = new MockServerTransport();
    const connections: number[] = [];
    server.onConnection.subscribe((t) => connections.push(t.isConnected() ? 1 : 0));
    await server.listen();
    const client1 = new MockClientTransport(server.capabilities, server);
    const client2 = new MockClientTransport(server.capabilities, server);
    const [c1, c2] = await Promise.all([client1.connect(), client2.connect()]);
    // connect 异步 accept，需等 macrotask
    await new Promise((r) => setTimeout(r, 10));
    expect(connections.length).toBe(2);
    expect(c1.isConnected()).toBe(true);
    expect(c2.isConnected()).toBe(true);
    // 两条连接独立：c1 发不影响 c2
    let c2Received = 0;
    c2.onMessage.subscribe(() => c2Received++);
    c1.send(new Uint8Array([1]));
    await new Promise((r) => setTimeout(r, 0));
    expect(c2Received).toBe(0);
  });
});

describe("eventMasks 编解码", () => {
  it("computeEventMasks: audio.algorithmConfigChanged (domain 0x09, bitOffset 0) => 090101", () => {
    const masks = computeEventMasks(["audio.algorithmConfigChanged"]);
    expect(masks).toBe("090101");
  });

  it("空列表 => 空串", () => {
    expect(computeEventMasks([])).toBe("");
  });

  it("isEventSubscribed 命中", () => {
    expect(isEventSubscribed("audio.algorithmConfigChanged", "090101")).toBe(true);
  });

  it("isEventSubscribed 未订阅（空 mask）", () => {
    expect(isEventSubscribed("audio.algorithmConfigChanged", "")).toBe(false);
  });

  it("isEventSubscribed 未订阅（mask 不含该 bit）", () => {
    expect(isEventSubscribed("audio.algorithmConfigChanged", "090100")).toBe(false);
  });
});
