import { describe, expect, it } from "vitest";
import type { Bytes } from "../../src/io/bytes.js";
import { Connection } from "../../src/protocol/connection.js";
import { AxtpSession } from "../../src/session/session.js";
import { createMockTransportPair } from "../../src/transport/mock/mockTransport.js";
import { framedBinaryCapabilities } from "../../src/transport/transport.js";

function settle(ms = 20): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

describe("STREAM P0 端到端（framed-binary）", () => {
  it("client openStream -> server handler 返回 streamId -> server send -> client onChunk", async () => {
    const { left, right } = createMockTransportPair(framedBinaryCapabilities());
    const clientConn = new Connection("client", left);
    const serverConn = new Connection("server", right);
    const server = new AxtpSession("server", serverConn);
    const client = new AxtpSession("client", clientConn);
    await Promise.all([client.onReady, server.onReady]);

    // server 注册 openStream handler（返回 streamId）
    server.onStream("video.openStream", (_ctx, _params) => {
      // server 分配 streamId（用 streams.open，但这里直接返回固定值便于断言）
      return { streamId: 42, streamProfile: "media.video", state: "open" } as never;
    });

    // server 端拿到 streamId=42 后，建 receive context 并 send 数据
    // （onStream 内部已 adopt streamId=42 建 context）
    // 模拟 server 主动发流：直接用 server.streams 发送
    setTimeout(() => {
      const ctx = server.streams.get(42);
      if (ctx) {
        serverConn.sendStream({
          streamId: 42,
          seqId: 0,
          cursor: 1000n,
          data: new Uint8Array([0xaa, 0xbb])
        });
      }
    }, 30);

    // client 发起建流
    const { streamId, stream } = await client.openStream("video.openStream", {
      source: "cam0",
      codec: "h264",
      streamProfile: "media.video"
    } as never);

    expect(streamId).toBe(42);

    const chunks: Bytes[] = [];
    const cursors: bigint[] = [];
    stream.onChunk((data, cursor) => {
      chunks.push(data);
      cursors.push(cursor);
    });

    await settle(50);
    expect(chunks.length).toBe(1);
    expect([...chunks[0]]).toEqual([0xaa, 0xbb]);
    expect(cursors[0]).toBe(1000n); // cursor 透传
  });

  it("断连时 stream 被 abort（onClose 触发）", async () => {
    const { left, right } = createMockTransportPair(framedBinaryCapabilities());
    const clientConn = new Connection("client", left);
    const serverConn = new Connection("server", right);
    const server = new AxtpSession("server", serverConn);
    const client = new AxtpSession("client", clientConn);
    await Promise.all([client.onReady, server.onReady]);

    server.onStream(
      "video.openStream",
      () => ({ streamId: 7, streamProfile: "media.video", state: "open" }) as never
    );

    const { stream } = await client.openStream("video.openStream", { source: "cam0" } as never);

    let closed = false;
    stream.onClose(() => (closed = true));

    client.close(); // 断连
    await settle(20);
    expect(closed).toBe(true);
    expect(stream.isClosed).toBe(true);
  });

  it("双向：client send -> server onChunk", async () => {
    const { left, right } = createMockTransportPair(framedBinaryCapabilities());
    const clientConn = new Connection("client", left);
    const serverConn = new Connection("server", right);
    const server = new AxtpSession("server", serverConn);
    const client = new AxtpSession("client", clientConn);
    await Promise.all([client.onReady, server.onReady]);

    const _serverChunks: Bytes[] = [];
    server.onStream("video.openStream", () => {
      return { streamId: 99, streamProfile: "media.video", state: "open" } as never;
    });

    const { stream } = await client.openStream("video.openStream", { source: "cam0" } as never);

    // server 端 streamId=99 的 context（onStream 内部已建），注册 onChunk
    // 但 onStream 当前没把 Stream 对象给 handler——用 streams.get 拿 context 设 handler
    await settle(10);
    const serverStreamCtx = server.streams.get(99);
    expect(serverStreamCtx).toBeDefined();
    if (serverStreamCtx === undefined) return;

    // client send（client 端 stream 用 streamId=99）
    stream.send(new Uint8Array([1, 2, 3]));
    await settle(20);
    // server 端应收到（通过 streams.onData 路由）
    expect(serverStreamCtx.chunks).toBeGreaterThanOrEqual(1);
  });
});
