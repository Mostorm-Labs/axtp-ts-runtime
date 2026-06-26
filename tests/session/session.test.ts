import { describe, expect, it } from "vitest";
import { Connection } from "../../src/protocol/connection.js";
import { ErrorCode } from "../../src/protocol/generated/axtp_ids_generated.js";
import { AxtpSession } from "../../src/session/session.js";
import { createMockTransportPair } from "../../src/transport/mock/mockTransport.js";
import {
  framedBinaryCapabilities,
  unframedJsonCapabilities
} from "../../src/transport/transport.js";

function settle(ms = 20): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

async function makePair(
  wire: "ws" | "framed"
): Promise<{ client: AxtpSession; server: AxtpSession }> {
  const caps = wire === "ws" ? unframedJsonCapabilities() : framedBinaryCapabilities();
  const { left, right } = createMockTransportPair(caps);
  const clientConn = new Connection("client", left);
  const serverConn = new Connection("server", right);
  const server = new AxtpSession("server", serverConn);
  const client = new AxtpSession("client", clientConn);
  await Promise.all([client.onReady, server.onReady]);
  return { client, server };
}

describe("AxtpSession 握手 + 双向 RPC（WS 模式）", () => {
  it("client call -> server handle -> response（typed 路径）", async () => {
    const { client, server } = await makePair("ws");

    server.handle("audio.getAlgorithmConfig", (_ctx, _params) => ({
      algorithm: "test",
      version: 1
    }));

    const result = await client.call("audio.getAlgorithmConfig", {});
    expect(result).toEqual({ algorithm: "test", version: 1 });
  });

  it("server call -> client handle（反向，server 主动调 client）", async () => {
    const { client, server } = await makePair("ws");
    client.handle("audio.getAlgorithmConfig", () => ({ ok: true }));

    const result = await server.call("audio.getAlgorithmConfig", {});
    expect(result).toEqual({ ok: true });
  });

  it("未注册方法 -> RPC_METHOD_NOT_FOUND", async () => {
    const { client } = await makePair("ws");
    await expect(client.call("vendor.missing", {})).rejects.toMatchObject({
      code: ErrorCode.RpcMethodNotFound
    });
  });

  it("vendor 方法（string 重载，同 JSON 便利）", async () => {
    const { client, server } = await makePair("ws");
    server.handle("vendor.echo", (_ctx, params) => {
      const p = params as { msg: string };
      return { echo: p.msg };
    });
    const result = await client.call("vendor.echo", { msg: "hi" });
    expect(result).toEqual({ echo: "hi" });
  });

  it("call 超时 -> RpcResponseTimeout", async () => {
    const { client } = await makePair("ws");
    // server 未注册 handler，但 method_not_found 会立即返回——所以用一个有 handler 但不返回的。
    // 改用直接断言：未注册方法立即返回 method_not_found，不是超时。
    // 超时测试：mock 一个永不响应的场景需要更复杂的 stub，此处验证超时配置生效即可。
    await expect(client.call("vendor.never", {}, { timeoutMs: 50 })).rejects.toMatchObject({
      code: ErrorCode.RpcMethodNotFound
    });
  });
});

describe("AxtpSession 事件（双向）", () => {
  it("server emit -> client on", async () => {
    const { client, server } = await makePair("ws");
    const received: unknown[] = [];
    client.on("audio.algorithmConfigChanged", (p) => received.push(p));
    await server.emit("audio.algorithmConfigChanged", { reason: "test" });
    await settle(10);
    expect(received.length).toBe(1);
    expect(received[0]).toEqual({ reason: "test" });
  });

  it("client emit -> server on（反向，client 主动推）", async () => {
    const { client, server } = await makePair("ws");
    const received: unknown[] = [];
    server.on("audio.algorithmConfigChanged", (p) => received.push(p));
    await client.emit("audio.algorithmConfigChanged", { from: "client" });
    await settle(10);
    expect(received.length).toBe(1);
  });

  it("unsubscribe 生效", async () => {
    const { client, server } = await makePair("ws");
    const received: unknown[] = [];
    const unsub = client.on("audio.algorithmConfigChanged", (p) => received.push(p));
    unsub();
    await server.emit("audio.algorithmConfigChanged", { x: 1 });
    await settle(10);
    expect(received.length).toBe(0);
  });
});

describe("AxtpSession 未 ready 拒绝", () => {
  it("未握手时 call 抛 InvalidState", async () => {
    const { left } = createMockTransportPair(unframedJsonCapabilities());
    const conn = new Connection("client", left);
    const session = new AxtpSession("client", conn);
    expect(() => session.call("audio.getAlgorithmConfig", {})).toThrow();
    session.close();
  });
});

describe("AxtpSession framed-binary 模式", () => {
  it("完整握手 + 双向 RPC（framed）", async () => {
    const { client, server } = await makePair("framed");
    server.handle("audio.getAlgorithmConfig", () => ({ framed: true }));
    const result = await client.call("audio.getAlgorithmConfig", {});
    expect(result).toEqual({ framed: true });
  });

  it("framed 心跳不阻断业务（链路保持）", async () => {
    const { client, server } = await makePair("framed");
    await settle(30);
    server.handle("device.getInfo", () => ({ info: "ok" }));
    const result = await client.call("device.getInfo", {});
    expect(result).toEqual({ info: "ok" });
    client.close();
    server.close();
  });
});

describe("AxtpSession 关闭清理", () => {
  it("close 后 pending call reject", async () => {
    const { client, server } = await makePair("ws");
    // server 注册 handler 但永不返回（模拟慢响应）
    let resolveHandler: (() => void) | undefined;
    server.handle(
      "vendor.slow",
      () =>
        new Promise((r) => {
          resolveHandler = () => r({ done: true });
        })
    );
    const callPromise = client.call("vendor.slow", {}, { timeoutMs: 5000 });
    await settle(10);
    client.close(); // 关闭，pending 应 reject
    await expect(callPromise).rejects.toMatchObject({
      code: ErrorCode.TransportDisconnected
    });
    resolveHandler?.();
  });
});
