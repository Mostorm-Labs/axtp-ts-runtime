// AxtpEndpoint 集成：流驱动器（pipeThrough 串接 transport↔core，reader 消费 CoreEvent，broker 分发）。
// loopback：两个 endpoint 背靠背（TransformStream 对接），验证 framed 握手 + RPC call + 事件。

import { describe, expect, it } from "vitest";
import { AxtpEndpoint } from "../../src/endpoint/endpoint.js";
import type { Bytes } from "../../src/io/bytes.js";
import type { StreamTransport } from "../../src/transport/contract.js";
import { framedBinaryProfile } from "../../src/transport/profile.js";
import { once } from "../helpers/eventStreamHelpers.js";

/** 一对背靠背 stream transport：A.writable→B.readable，B.writable→A.readable。 */
function loopback(): [StreamTransport, StreamTransport] {
  const ab = new TransformStream<Bytes, Bytes>(); // A→B
  const ba = new TransformStream<Bytes, Bytes>(); // B→A
  const profile = framedBinaryProfile("AXTP-TCP");
  const mk = (
    readable: ReadableStream<Bytes>,
    writable: WritableStream<Bytes>
  ): StreamTransport => ({
    profile,
    readable,
    writable,
    close: () => {}
  });
  return [mk(ba.readable, ab.writable), mk(ab.readable, ba.writable)];
}

function newServer(t: StreamTransport, seed = 1): AxtpEndpoint {
  return new AxtpEndpoint({
    transport: t,
    physicalRole: "server",
    logicalRole: "server",
    maxFrameSize: 4096,
    heartbeatIntervalMs: 60000,
    handshakeSeed: seed
  });
}

function newClient(t: StreamTransport): AxtpEndpoint {
  return new AxtpEndpoint({
    transport: t,
    physicalRole: "client",
    logicalRole: "client",
    maxFrameSize: 4096,
    heartbeatIntervalMs: 60000
  });
}

describe("AxtpEndpoint — loopback 端到端", () => {
  it("framed：双方握手后均 ready", async () => {
    const [ta, tb] = loopback();
    const server = newServer(ta);
    const client = newClient(tb);
    server.start();
    client.start();
    await Promise.all([once(server.onReady), once(client.onReady)]);
    expect(server.isReady).toBe(true);
    expect(client.isReady).toBe(true);
    expect(server.sid).toMatch(/^[0-9a-fA-F]{8}$/);
    server.close();
    client.close();
  });

  it("client.call → server handler → response 回流", async () => {
    const [ta, tb] = loopback();
    const server = newServer(ta);
    const client = newClient(tb);
    server.broker.setMethod("add", (_ctx, p) => ({
      value: (p as { a: number }).a + (p as { b: number }).b
    }));
    server.start();
    client.start();
    await Promise.all([once(server.onReady), once(client.onReady)]);

    const result = await client.call("add", { a: 2, b: 3 });
    expect(result).toEqual({ value: 5 });
    server.close();
    client.close();
  });

  it("client.emit → server 事件 handler 收到", async () => {
    const [ta, tb] = loopback();
    const server = newServer(ta);
    const client = newClient(tb);
    let received: unknown = undefined;
    server.broker.addEventListener("ping", (data) => {
      received = data;
    });
    server.start();
    client.start();
    await Promise.all([once(server.onReady), once(client.onReady)]);

    client.emit("ping", { hello: "world" });
    await new Promise((r) => setTimeout(r, 30));
    expect(received).toEqual({ hello: "world" });
    server.close();
    client.close();
  });

  it("STREAM：client.openStream → server onStream；双向 chunk 往返", async () => {
    const [ta, tb] = loopback();
    const server = newServer(ta);
    const client = newClient(tb);
    server.onStream("video.open", (_params, stream) => {
      stream.onChunk(() => stream.send(new TextEncoder().encode("ack")));
      return { streamId: stream.streamId };
    });
    server.start();
    client.start();
    await Promise.all([once(server.onReady), once(client.onReady)]);

    const { stream } = await client.openStream("video.open", { src: "hdmi" });
    const acks: string[] = [];
    stream.onChunk((data) => acks.push(new TextDecoder().decode(data)));
    stream.send(new TextEncoder().encode("hello"));
    await new Promise((r) => setTimeout(r, 30));
    expect(acks).toEqual(["ack"]);
    server.close();
    client.close();
  });
});
