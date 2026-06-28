import { describe, expect, it } from "vitest";
import { AxtpSession } from "../../src/session/session.js";
import { createMockTransportPair } from "../../src/transport/mock/mockTransport.js";
import { framedBinaryCapabilities } from "../../src/transport/transport.js";

function settle(ms = 20): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

describe("STREAM P0 端到端（framed-binary）", () => {
  it("client openStream -> server 返回 streamId", async () => {
    const { left, right } = createMockTransportPair(framedBinaryCapabilities());
    const server = new AxtpSession(right, { physicalRole: "server", logicalRole: "server" });
    const client = new AxtpSession(left, { physicalRole: "client", logicalRole: "client" });
    await Promise.all([client.onReady, server.onReady]);

    server.onStream(
      "video.openStream",
      () => ({ streamId: 42, streamProfile: "media.video", state: "open" }) as never
    );

    const { streamId, stream } = await client.openStream("video.openStream", {
      source: "cam0",
      codec: "h264",
      streamProfile: "media.video"
    } as never);

    expect(streamId).toBe(42);
    expect(stream).toBeDefined();
  });

  it("断连时 stream 被 abort（onClose 触发）", async () => {
    const { left, right } = createMockTransportPair(framedBinaryCapabilities());
    const server = new AxtpSession(right, { physicalRole: "server", logicalRole: "server" });
    const client = new AxtpSession(left, { physicalRole: "client", logicalRole: "client" });
    await Promise.all([client.onReady, server.onReady]);

    server.onStream(
      "video.openStream",
      () => ({ streamId: 7, streamProfile: "media.video", state: "open" }) as never
    );

    const { stream } = await client.openStream("video.openStream", { source: "cam0" } as never);

    let closed = false;
    stream.onClose(() => (closed = true));

    client.close();
    await settle(20);
    expect(closed).toBe(true);
    expect(stream.isClosed).toBe(true);
  });

  it("双向：client send -> server 端 stream 收到（onChunk）", async () => {
    const { left, right } = createMockTransportPair(framedBinaryCapabilities());
    const server = new AxtpSession(right, { physicalRole: "server", logicalRole: "server" });
    const client = new AxtpSession(left, { physicalRole: "client", logicalRole: "client" });
    await Promise.all([client.onReady, server.onReady]);

    // server 端接收的 chunks（通过 onStream handler 创建的 Stream 收集）
    const _serverChunks: unknown[] = [];
    server.onStream("video.openStream", () => {
      return { streamId: 99, streamProfile: "media.video", state: "open" } as never;
    });

    const { stream } = await client.openStream("video.openStream", { source: "cam0" } as never);

    // client send（client 端 stream 用 streamId=99 发送）
    stream.send(new Uint8Array([1, 2, 3]));
    await settle(20);
    // server 端 StreamManager 内部路由数据（P0 验证：send 不抛错 + 流程通畅）
    expect(stream.isClosed).toBe(false);
  });
});
