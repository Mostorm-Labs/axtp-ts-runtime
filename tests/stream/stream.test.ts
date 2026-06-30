import { afterEach, describe, expect, it } from "vitest";
import type { Bytes } from "../../src/io/bytes.js";
import { AxtpSession } from "../../src/session/session.js";
import { createMockTransportPair } from "../../src/transport/mock/mockTransport.js";
import {
  framedBinaryProfile,
  unframedJsonProfile
} from "../../src/transport/contract.js";

function settle(ms = 20): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

// 跟踪创建的 session，afterEach 统一关闭，避免 Heartbeat 定时器在测试结束后继续存活。
const createdSessions: AxtpSession[] = [];

afterEach(() => {
  for (const s of createdSessions) s.close();
  createdSessions.length = 0;
});

describe("STREAM P0 端到端（framed-binary）", () => {
  it("client openStream -> server 返回 streamId", async () => {
    const { left, right } = createMockTransportPair(framedBinaryProfile());
    const server = new AxtpSession(() => Promise.resolve(right), { physicalRole: "server", logicalRole: "server" });
    const client = new AxtpSession(() => Promise.resolve(left), { physicalRole: "client", logicalRole: "client" });
    await Promise.all([new Promise<void>((r) => client.onReady.subscribe(() => r())), new Promise<void>((r) => server.onReady.subscribe(() => r()))]);
    createdSessions.push(client, server);

    server.onStream("video.openStream", (_ctx, _params, stream) => ({ streamId: stream.streamId, streamProfile: "media.video", state: "open" } as never));

    const { streamId, stream } = await client.openStream("video.openStream", {
      source: "cam0",
      codec: "h264",
      streamProfile: "media.video"
    } as never);

    expect(streamId).toBeGreaterThan(0);
    expect(stream).toBeDefined();
  });

  it("断连时 stream 被 abort（onClose 触发）", async () => {
    const { left, right } = createMockTransportPair(framedBinaryProfile());
    const server = new AxtpSession(() => Promise.resolve(right), { physicalRole: "server", logicalRole: "server" });
    const client = new AxtpSession(() => Promise.resolve(left), { physicalRole: "client", logicalRole: "client" });
    await Promise.all([new Promise<void>((r) => client.onReady.subscribe(() => r())), new Promise<void>((r) => server.onReady.subscribe(() => r()))]);
    createdSessions.push(client, server);

    server.onStream("video.openStream", (_ctx, _params, stream) => ({ streamId: stream.streamId, streamProfile: "media.video", state: "open" } as never));

    const { stream } = await client.openStream("video.openStream", { source: "cam0" } as never);

    let closed = false;
    stream.onClose(() => (closed = true));

    client.close();
    await settle(20);
    expect(closed).toBe(true);
    expect(stream.isClosed).toBe(true);
  });

  it("双向：client send -> server onStreamReady 收到数据", async () => {
    const { left, right } = createMockTransportPair(framedBinaryProfile());
    const server = new AxtpSession(() => Promise.resolve(right), { physicalRole: "server", logicalRole: "server" });
    const client = new AxtpSession(() => Promise.resolve(left), { physicalRole: "client", logicalRole: "client" });
    await Promise.all([new Promise<void>((r) => client.onReady.subscribe(() => r())), new Promise<void>((r) => server.onReady.subscribe(() => r()))]);
    createdSessions.push(client, server);

    // server 端 handler 直接接收 Stream（onStream 第三参数）
    const serverChunks: Bytes[] = [];
    server.onStream(
      "video.openStream",
      (_ctx, _params, stream) => {
        stream.onChunk((data) => serverChunks.push(data));
        return { streamId: stream.streamId, streamProfile: "media.video", state: "open" } as never;
      }
    );

    const { stream } = await client.openStream("video.openStream", { source: "cam0" } as never);
    await settle(10);

    // client send
    stream.send(new Uint8Array([1, 2, 3]));
    await settle(20);

    expect(serverChunks.length).toBe(1);
    expect([...serverChunks[0]]).toEqual([1, 2, 3]);
  });
});

describe("STREAM 在 WS 模式下拒绝", () => {
  it("WS 模式 openStream 抛 NotSupported", async () => {
    const { left, right } = createMockTransportPair(unframedJsonProfile());
    const server = new AxtpSession(() => Promise.resolve(right), { physicalRole: "server", logicalRole: "server" });
    const client = new AxtpSession(() => Promise.resolve(left), { physicalRole: "client", logicalRole: "client" });
    await Promise.all([new Promise<void>((r) => client.onReady.subscribe(() => r())), new Promise<void>((r) => server.onReady.subscribe(() => r()))]);
    createdSessions.push(client, server);

    server.onStream("video.openStream", () => ({ streamId: 1, streamProfile: "media.video", state: "open" } as never));

    // 预检：WS（unframed-json）不承载 STREAM，openStream 入口即抛 NotSupported（不发 RPC）
    await expect(client.openStream("video.openStream", { source: "cam0" } as never)).rejects.toThrow();
  });
});
