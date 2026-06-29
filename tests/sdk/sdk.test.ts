import { afterEach, describe, expect, it } from "vitest";
import { AxtpClient } from "../../src/sdk/client.js";
import { AxtpServer } from "../../src/sdk/server.js";
import {
  MockClientTransport,
  MockServerTransport
} from "../../src/transport/mock/mockTransport.js";
import { unframedJsonCapabilities } from "../../src/transport/transport.js";

function settle(ms = 20): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

// 跟踪创建的 client/server，afterEach 统一关闭，避免 Connection/Heartbeat 定时器在测试结束后继续存活。
const createdClients: AxtpClient[] = [];
const createdServers: AxtpServer[] = [];

afterEach(async () => {
  for (const c of createdClients) await c.close();
  for (const s of createdServers) await s.close();
  createdClients.length = 0;
  createdServers.length = 0;
});

async function makeServerClient(): Promise<{
  server: AxtpServer;
  client: AxtpClient;
  serverTransport: MockServerTransport;
}> {
  // 经典场景：server 暴露能力（Logical Server），client 消费（Logical Client）。
  // 显式传 logicalRole 覆盖 Cloud Reverse 默认值，保持本组测试的经典语义意图。
  const serverTransport = new MockServerTransport(unframedJsonCapabilities());
  const server = new AxtpServer(serverTransport, { logicalRole: "server" });
  await server.listen();

  const clientTransport = new MockClientTransport(unframedJsonCapabilities(), serverTransport);
  const client = new AxtpClient(clientTransport, { logicalRole: "client" });
  createdClients.push(client);
  createdServers.push(server);
  await client.connect();
  await settle(10);
  return { server, client, serverTransport };
}

describe("AxtpServer 多 client + 全局 handle", () => {
  it("server 接受多个 client，各自独立 session", async () => {
    const { server, serverTransport } = await makeServerClient();
    const client2Transport = new MockClientTransport(unframedJsonCapabilities(), serverTransport);
    const client2 = new AxtpClient(client2Transport, { logicalRole: "client" });
    await client2.connect();
    await settle(10);
    expect(server.getSessions().length).toBe(2);
    client2.close();
    await settle(10);
  });

  it("全局 handle 所有 session 共享（委托 HandlerRegistry）", async () => {
    const { server, client } = await makeServerClient();
    server.handle("audio.getAlgorithmConfig", () => ({ shared: true }));
    const result = await client.call("audio.getAlgorithmConfig", {});
    expect(result).toEqual({ shared: true });
  });

  it("新连接自动应用已有全局 handler", async () => {
    const { server, serverTransport } = await makeServerClient();
    server.handle("audio.getAlgorithmConfig", () => ({ late: true }));
    const client2Transport = new MockClientTransport(unframedJsonCapabilities(), serverTransport);
    const client2 = new AxtpClient(client2Transport, { logicalRole: "client" });
    await client2.connect();
    await settle(10);
    const result = await client2.call("audio.getAlgorithmConfig", {});
    expect(result).toEqual({ late: true });
  });

  it("单播：server.call(sessionId, ...)", async () => {
    const { server, client } = await makeServerClient();
    let called = false;
    // client 注册 handler，server 主动调 client
    client.handle("audio.getAlgorithmConfig", () => {
      called = true;
      return { from: "client" };
    });
    await settle(10);
    const sessionId = server.getSessions()[0].id;
    const result = await server.call(sessionId, "audio.getAlgorithmConfig", {});
    expect(called).toBe(true);
    expect(result).toEqual({ from: "client" });
  });

  it("广播 emit 给所有 APP_READY session", async () => {
    const { server, client, serverTransport } = await makeServerClient();
    const client2Transport = new MockClientTransport(unframedJsonCapabilities(), serverTransport);
    const client2 = new AxtpClient(client2Transport, { logicalRole: "client" });
    await client2.connect();
    await settle(10);

    const received1: unknown[] = [];
    const received2: unknown[] = [];
    client.on("audio.algorithmConfigChanged", (p) => received1.push(p));
    client2.on("audio.algorithmConfigChanged", (p) => received2.push(p));
    await settle(10);

    await server.emit("audio.algorithmConfigChanged", { broadcast: true } as never);
    await settle(10);
    expect(received1.length).toBe(1);
    expect(received2.length).toBe(1);
  });

  it("广播 filter 定向", async () => {
    const { server, client, serverTransport } = await makeServerClient();
    const client2Transport = new MockClientTransport(unframedJsonCapabilities(), serverTransport);
    const client2 = new AxtpClient(client2Transport, { logicalRole: "client" });
    await client2.connect();
    await settle(10);

    const received1: unknown[] = [];
    const received2: unknown[] = [];
    client.on("audio.algorithmConfigChanged", (p) => received1.push(p));
    client2.on("audio.algorithmConfigChanged", (p) => received2.push(p));
    await settle(10);

    // 只发给第一个 session（用 sid 区分）
    const targetSid = client.sid;
    await server.emit(
      "audio.algorithmConfigChanged",
      { x: 1 } as never,
      (s) => s.sid === targetSid
    );
    await settle(10);
    expect(received1.length).toBe(1);
    expect(received2.length).toBe(0);
  });
});

describe("AxtpClient 重连机制", () => {
  it("断连后自动重连，handler 迁移", async () => {
    const serverTransport = new MockServerTransport(unframedJsonCapabilities());
    const server = new AxtpServer(serverTransport, { logicalRole: "server" });
    await server.listen();
    server.handle("audio.getAlgorithmConfig", () => ({ reconnected: true }));

    const clientTransport = new MockClientTransport(unframedJsonCapabilities(), serverTransport);
    const client = new AxtpClient(clientTransport, {
      reconnect: { enabled: true, initialDelayMs: 10, maxDelayMs: 50 },
      logicalRole: "client"
    });
    await client.connect();

    let reconnected = false;
    client.onReconnect.subscribe(() => (reconnected = true));

    // 模拟断连：client 端 transport 触发 onClose
    // 通过 client.call 失败或直接触发——这里用 server 关闭该 session
    const serverSession = server.getSessions()[0];
    serverSession?.close();
    await settle(100); // 等重连（退避 10ms）

    expect(reconnected).toBe(true);
    expect(client.isReady).toBe(true);

    // handler 仍可用（迁移成功）
    const result = await client.call("audio.getAlgorithmConfig", {});
    expect(result).toEqual({ reconnected: true });
  });

  it("断连后 call 抛错（不启用重连）", async () => {
    const serverTransport = new MockServerTransport(unframedJsonCapabilities());
    const server = new AxtpServer(serverTransport, { logicalRole: "server" });
    await server.listen();
    const clientTransport = new MockClientTransport(unframedJsonCapabilities(), serverTransport);
    const client = new AxtpClient(clientTransport, {
      logicalRole: "client"
    });
    await client.connect();

    server.getSessions()[0]?.close();
    await settle(20);
    // 断连后 client.call 应抛错（session not ready）
    expect(() => client.call("audio.getAlgorithmConfig", {})).toThrow();
  });

  it("主动 close 不触发重连", async () => {
    const { server: _server, client } = await makeServerClient();
    let reconnected = false;
    client.onReconnect.subscribe(() => (reconnected = true));
    await client.close();
    await settle(50);
    expect(reconnected).toBe(false);
  });

  it("重连失败：maxAttempts 耗尽 -> onReconnectFailed", async () => {
    const serverTransport = new MockServerTransport(unframedJsonCapabilities());
    const server = new AxtpServer(serverTransport, { logicalRole: "server" });
    await server.listen();

    const clientTransport = new MockClientTransport(unframedJsonCapabilities(), serverTransport);
    const client = new AxtpClient(clientTransport, {
      logicalRole: "client",
      reconnect: { enabled: true, initialDelayMs: 10, maxDelayMs: 10, maxAttempts: 2 }
    });
    await client.connect();

    // 使 transport 不可用，重连时 connect() 会 reject
    clientTransport.close();
    await server.close();

    let reconnectFailed = false;
    client.onReconnectFailed.subscribe(() => (reconnectFailed = true));

    // 触发断连
    server.getSessions()[0]?.close();

    // 等待重连耗尽（transport.connect 拒绝 → 快速失败 → 退避重试 → maxAttempts）
    await settle(500);

    expect(reconnectFailed).toBe(true);
    expect(client.isReady).toBe(false);
  });
});

describe("AxtpClient handle unsubscribe（重连安全）", () => {
  it("unsubscribe 操作 snapshot，重连后不再迁移", async () => {
    const { server, client, serverTransport: _serverTransport } = await makeServerClient();
    let callCount = 0;
    const unsub = client.handle("audio.getAlgorithmConfig", () => {
      callCount++;
      return {};
    });
    unsub();
    await settle(10);

    // 重新用全局 handler 测试
    server.handle("audio.getAlgorithmConfig", () => ({ ok: 1 }));
    await client.call("audio.getAlgorithmConfig", {});
    expect(callCount).toBe(0); // 已 unsubscribe，不应被调用
  });
});

describe("Cloud Reverse 默认场景（发起连接方=Logical Server）", () => {
  // 默认值：AxtpClient logicalRole="server"（发 Hello、分配 sid、暴露能力），
  //         AxtpServer logicalRole="client"（收 Hello、发 Identify、消费能力）。
  // 对应 spec Cloud Reverse：设备主动连云，设备是 Logical Server。

  it("默认 options：client（发起连接）发 Hello，server（接受连接）收 Hello", async () => {
    const serverTransport = new MockServerTransport(unframedJsonCapabilities());
    // 默认 logicalRole：server 端="client"，client 端="server"
    const server = new AxtpServer(serverTransport);
    await server.listen();

    const clientTransport = new MockClientTransport(unframedJsonCapabilities(), serverTransport);
    const client = new AxtpClient(clientTransport);
    await client.connect();
    await settle(10);

    // 双方都应握手成功（Hello 由 client 发，Identify 由 server 发，Identified 由 client 发）
    expect(client.isReady).toBe(true);
    expect(client.sid).toMatch(/^[0-9a-f]{8}$/);
    // server 端 session 也 ready
    const serverSession = server.getSessions()[0];
    expect(serverSession?.isReady).toBe(true);
    expect(serverSession?.sid).toBe(client.sid); // 同一 sid（client 作为 Logical Server 分配）
  });

  it("Cloud Reverse：client 注册 handler（能力提供方），server 主动 call", async () => {
    const serverTransport = new MockServerTransport(unframedJsonCapabilities());
    const server = new AxtpServer(serverTransport); // 默认 logicalRole="client"
    await server.listen();

    const clientTransport = new MockClientTransport(unframedJsonCapabilities(), serverTransport);
    const client = new AxtpClient(clientTransport); // 默认 logicalRole="server"
    await client.connect();
    await settle(10);

    // client（Logical Server）暴露能力
    client.handle("audio.getAlgorithmConfig", () => ({ provider: "client" }));

    // server（Logical Client）主动调用 client
    const sessionId = server.getSessions()[0].id;
    const result = await server.call(sessionId, "audio.getAlgorithmConfig", {});
    expect(result).toEqual({ provider: "client" });
  });

  it("Cloud Reverse：client 主动 emit 事件，server 收", async () => {
    const serverTransport = new MockServerTransport(unframedJsonCapabilities());
    const server = new AxtpServer(serverTransport); // 默认 logicalRole="client"
    await server.listen();

    const clientTransport = new MockClientTransport(unframedJsonCapabilities(), serverTransport);
    const client = new AxtpClient(clientTransport); // 默认 logicalRole="server"
    await client.connect();
    await settle(10);

    const received: unknown[] = [];
    server.on("audio.algorithmConfigChanged", (p) => received.push(p));
    await settle(10);

    await client.emit("audio.algorithmConfigChanged", { from: "cloud-reverse" });
    await settle(10);
    expect(received.length).toBe(1);
    expect(received[0]).toEqual({ from: "cloud-reverse" });
  });
});
