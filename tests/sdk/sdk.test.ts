// AxtpClient / AxtpServer over mock stream loopback（新栈 SDK 集成）。
// 覆盖 connect/call/handle/emit/广播/单播/close。

import { describe, expect, it } from "vitest";
import type { AxtpDiagnosticEntry } from "../../src/diagnostics.js";
import { AxtpClient } from "../../src/sdk/client.js";
import { AxtpServer } from "../../src/sdk/server.js";
import type { StreamClientTransport } from "../../src/transport/contract.js";
import { createMockStreamLoopback } from "../../src/transport/mock/mockStreamTransport.js";
import { framedBinaryProfile } from "../../src/transport/profile.js";
import { ErrorCode } from "../../src/types/error.js";
import { once } from "../helpers/eventStreamHelpers.js";

/** 标准 TCP 拓扑：server=device（logicalRole server 发 Hello），client=app（logicalRole client 发 Identify）。 */
async function setupStandard(
  handlers?: (server: AxtpServer) => void
): Promise<{ server: AxtpServer; client: AxtpClient }> {
  const loop = createMockStreamLoopback();
  const server = new AxtpServer(loop.server, { logicalRole: "server", heartbeatIntervalMs: 60000 });
  const client = new AxtpClient(loop.client, { logicalRole: "client", heartbeatIntervalMs: 60000 });
  if (handlers !== undefined) handlers(server);
  const clientReady = once(client.onConnect);
  const serverReady = once(server.onConnect);
  await server.listen();
  void client.connect().catch(() => {});
  await clientReady;
  await serverReady;
  return { server, client };
}

describe("AxtpClient / AxtpServer（新栈）", () => {
  it("connect → 双方 ready；client.call → server.handle → response", async () => {
    const { server, client } = await setupStandard((s) => {
      s.handleRaw("add", (_ctx, p) => (p as { a: number }).a + (p as { b: number }).b);
    });
    expect(client.isReady).toBe(true);
    const result = await client.callRaw("add", { a: 2, b: 3 });
    expect(result).toBe(5);
    await client.close();
    await server.close();
  });

  it("queues client events emitted before ready when delivery.events offline is queue", async () => {
    let received: unknown;
    const loop = createMockStreamLoopback();
    const server = new AxtpServer(loop.server, {
      logicalRole: "server",
      heartbeatIntervalMs: 60000
    });
    const client = new AxtpClient(loop.client, {
      logicalRole: "client",
      heartbeatIntervalMs: 60000,
      delivery: { events: { offline: "queue" } }
    });
    server.onRaw("early", (data) => {
      received = data;
    });

    await server.listen();
    const connected = client.connect();
    const emitted = client.emitRaw("early", { queued: true });

    await connected;
    await emitted;
    await new Promise((r) => setTimeout(r, 30));
    expect(received).toEqual({ queued: true });

    await client.close();
    await server.close();
  });

  it("queues client events emitted before ready when delivery.events offline is queue", async () => {
    let received: unknown;
    const loop = createMockStreamLoopback();
    const server = new AxtpServer(loop.server, {
      logicalRole: "server",
      heartbeatIntervalMs: 60000
    });
    const client = new AxtpClient(loop.client, {
      logicalRole: "client",
      heartbeatIntervalMs: 60000,
      delivery: {
        events: {
          offline: "queue",
          queue: { maxSize: 1000, overflow: "reject" }
        }
      }
    });
    server.onRaw("early", (data) => {
      received = data;
    });

    await server.listen();
    const connected = client.connect();
    const emitted = client.emitRaw("early", { queued: true });

    await connected;
    await emitted;
    await new Promise((r) => setTimeout(r, 30));
    expect(received).toEqual({ queued: true });

    await client.close();
    await server.close();
  });

  it("rejects new client events when the delivery event queue is full", async () => {
    const loop = createMockStreamLoopback();
    const client = new AxtpClient(loop.client, {
      logicalRole: "client",
      delivery: { events: { offline: "queue", queue: { maxSize: 1, overflow: "reject" } } }
    });

    const first = client.emitRaw("queued", { n: 1 });
    await expect(client.emitRaw("queued", { n: 2 })).rejects.toMatchObject({
      code: ErrorCode.InvalidState
    });
    await client.close();
    await expect(first).rejects.toMatchObject({ code: ErrorCode.TransportDisconnected });
  });

  it("drops newest client events when configured delivery event queue overflow is drop-newest", async () => {
    const loop = createMockStreamLoopback();
    const received: unknown[] = [];
    const server = new AxtpServer(loop.server, {
      logicalRole: "server",
      heartbeatIntervalMs: 60000
    });
    const client = new AxtpClient(loop.client, {
      logicalRole: "client",
      heartbeatIntervalMs: 60000,
      delivery: { events: { offline: "queue", queue: { maxSize: 1, overflow: "drop-newest" } } }
    });
    server.onRaw("queued", (data) => received.push(data));

    await server.listen();
    const first = client.emitRaw("queued", { n: 1 });
    await expect(client.emitRaw("queued", { n: 2 })).resolves.toBeUndefined();
    await client.connect();
    await first;
    await new Promise((r) => setTimeout(r, 30));
    expect(received).toEqual([{ n: 1 }]);

    await client.close();
    await server.close();
  });

  it("drops oldest client events when configured delivery event queue overflow is drop-oldest", async () => {
    const loop = createMockStreamLoopback();
    const received: unknown[] = [];
    const server = new AxtpServer(loop.server, {
      logicalRole: "server",
      heartbeatIntervalMs: 60000
    });
    const client = new AxtpClient(loop.client, {
      logicalRole: "client",
      heartbeatIntervalMs: 60000,
      delivery: { events: { offline: "queue", queue: { maxSize: 1, overflow: "drop-oldest" } } }
    });
    server.onRaw("queued", (data) => received.push(data));

    await server.listen();
    const first = client.emitRaw("queued", { n: 1 });
    const second = client.emitRaw("queued", { n: 2 });
    await expect(first).resolves.toBeUndefined();
    await client.connect();
    await second;
    await new Promise((r) => setTimeout(r, 30));
    expect(received).toEqual([{ n: 2 }]);

    await client.close();
    await server.close();
  });

  it("waits until ready before sending calls when offlinePolicy is wait-ready", async () => {
    const loop = createMockStreamLoopback();
    const server = new AxtpServer(loop.server, {
      logicalRole: "server",
      heartbeatIntervalMs: 60000
    });
    const client = new AxtpClient(loop.client, {
      logicalRole: "client",
      heartbeatIntervalMs: 60000
    });
    server.handleRaw("add", (_ctx, p) => (p as { a: number }).a + (p as { b: number }).b);

    await server.listen();
    const called = client.callRaw("add", { a: 4, b: 6 }, { offlinePolicy: "wait-ready" });
    await client.connect();
    await expect(called).resolves.toBe(10);

    await client.close();
    await server.close();
  });

  it("uses delivery.calls.default for wait-ready calls", async () => {
    const loop = createMockStreamLoopback();
    const server = new AxtpServer(loop.server, {
      logicalRole: "server",
      heartbeatIntervalMs: 60000
    });
    const client = new AxtpClient(loop.client, {
      logicalRole: "client",
      heartbeatIntervalMs: 60000,
      delivery: {
        calls: {
          default: { offlinePolicy: "wait-ready", timeoutMs: 5_000 }
        }
      }
    });
    server.handleRaw("add", (_ctx, p) => (p as { a: number }).a + (p as { b: number }).b);

    await server.listen();
    const called = client.callRaw("add", { a: 4, b: 6 });
    await client.connect();
    await expect(called).resolves.toBe(10);

    await client.close();
    await server.close();
  });

  it("uses delivery.calls.methods to override call default policy", async () => {
    const loop = createMockStreamLoopback();
    const client = new AxtpClient(loop.client, {
      logicalRole: "client",
      delivery: {
        calls: {
          default: { offlinePolicy: "wait-ready" },
          methods: {
            "dangerous.deleteFile": { offlinePolicy: "fail-fast" }
          }
        }
      }
    });

    await expect(client.callRaw("dangerous.deleteFile", { path: "/tmp/a" })).rejects.toMatchObject({
      code: ErrorCode.InvalidState
    });
    await client.close();
  });

  it("uses per-call options before delivery call policy", async () => {
    const loop = createMockStreamLoopback();
    const client = new AxtpClient(loop.client, {
      logicalRole: "client",
      delivery: {
        calls: {
          default: { offlinePolicy: "wait-ready" }
        }
      }
    });

    await expect(
      client.callRaw("device.getInfo", {}, { offlinePolicy: "fail-fast" })
    ).rejects.toMatchObject({ code: ErrorCode.InvalidState });
    await client.close();
  });

  it("queues RPC calls until ready when offlinePolicy is queue", async () => {
    const loop = createMockStreamLoopback();
    const received: unknown[] = [];
    const server = new AxtpServer(loop.server, {
      logicalRole: "server",
      heartbeatIntervalMs: 60000
    });
    const client = new AxtpClient(loop.client, {
      logicalRole: "client",
      heartbeatIntervalMs: 60000
    });
    server.handleRaw("setName", (_ctx, p) => {
      received.push(p);
      return (p as { name: string }).name;
    });

    await server.listen();
    const called = client.callRaw("setName", { name: "Room A" }, { offlinePolicy: "queue" });
    await client.connect();

    await expect(called).resolves.toBe("Room A");
    expect(received).toEqual([{ name: "Room A" }]);

    await client.close();
    await server.close();
  });

  it("coalesces queued RPC calls by coalesceKey and resolves previous calls with the newest result", async () => {
    const loop = createMockStreamLoopback();
    const received: unknown[] = [];
    const server = new AxtpServer(loop.server, {
      logicalRole: "server",
      heartbeatIntervalMs: 60000
    });
    const client = new AxtpClient(loop.client, {
      logicalRole: "client",
      heartbeatIntervalMs: 60000
    });
    server.handleRaw("setName", (_ctx, p) => {
      received.push(p);
      return `applied:${(p as { name: string }).name}`;
    });

    await server.listen();
    const first = client.callRaw(
      "setName",
      { name: "Room A" },
      { offlinePolicy: "queue", coalesceKey: "setName", coalescePrevious: "resolve-with-next" }
    );
    const second = client.callRaw(
      "setName",
      { name: "Room B" },
      { offlinePolicy: "queue", coalesceKey: "setName", coalescePrevious: "resolve-with-next" }
    );
    const third = client.callRaw(
      "setName",
      { name: "Room C" },
      { offlinePolicy: "queue", coalesceKey: "setName", coalescePrevious: "resolve-with-next" }
    );

    await client.connect();

    await expect(first).resolves.toBe("applied:Room C");
    await expect(second).resolves.toBe("applied:Room C");
    await expect(third).resolves.toBe("applied:Room C");
    expect(received).toEqual([{ name: "Room C" }]);

    await client.close();
    await server.close();
  });

  it("rejects previous queued RPC calls when coalescePrevious is reject", async () => {
    const loop = createMockStreamLoopback();
    const server = new AxtpServer(loop.server, {
      logicalRole: "server",
      heartbeatIntervalMs: 60000
    });
    const client = new AxtpClient(loop.client, {
      logicalRole: "client",
      heartbeatIntervalMs: 60000
    });
    server.handleRaw("setName", (_ctx, p) => (p as { name: string }).name);

    await server.listen();
    const first = client.callRaw(
      "setName",
      { name: "Room A" },
      { offlinePolicy: "queue", coalesceKey: "setName", coalescePrevious: "reject" }
    );
    const second = client.callRaw(
      "setName",
      { name: "Room B" },
      { offlinePolicy: "queue", coalesceKey: "setName", coalescePrevious: "reject" }
    );

    await expect(first).rejects.toMatchObject({ code: ErrorCode.InvalidState });
    await client.connect();
    await expect(second).resolves.toBe("Room B");

    await client.close();
    await server.close();
  });

  it("drops previous queued RPC calls when coalescePrevious is drop", async () => {
    const loop = createMockStreamLoopback();
    const received: unknown[] = [];
    const server = new AxtpServer(loop.server, {
      logicalRole: "server",
      heartbeatIntervalMs: 60000
    });
    const client = new AxtpClient(loop.client, {
      logicalRole: "client",
      heartbeatIntervalMs: 60000
    });
    server.handleRaw("setName", (_ctx, p) => {
      received.push(p);
      return (p as { name: string }).name;
    });

    await server.listen();
    const first = client.callRaw(
      "setName",
      { name: "Room A" },
      { offlinePolicy: "queue", coalesceKey: "setName", coalescePrevious: "drop" }
    );
    const second = client.callRaw(
      "setName",
      { name: "Room B" },
      { offlinePolicy: "queue", coalesceKey: "setName", coalescePrevious: "drop" }
    );

    await expect(first).resolves.toBeUndefined();
    await client.connect();
    await expect(second).resolves.toBe("Room B");
    expect(received).toEqual([{ name: "Room B" }]);

    await client.close();
    await server.close();
  });

  it("keeps unsent queued RPC calls queued when the endpoint disconnects during flush", async () => {
    const loop = createMockStreamLoopback();
    const received: Array<{ id: number | undefined; params: unknown }> = [];
    const server = new AxtpServer(loop.server, {
      logicalRole: "server",
      heartbeatIntervalMs: 60000
    });
    const client = new AxtpClient(loop.client, {
      logicalRole: "client",
      heartbeatIntervalMs: 60000,
      reconnect: { enabled: true, initialDelayMs: 1, maxDelayMs: 1, jitter: false }
    });

    server.handleRaw("setName", async (ctx, p) => {
      received.push({ id: ctx.id, params: p });
      if ((p as { name: string }).name === "Room A") {
        server.getEndpoint(ctx.id as number)?.close(false, true);
        await new Promise((r) => setTimeout(r, 10));
      }
      return (p as { name: string }).name;
    });

    await server.listen();
    const first = client.callRaw("setName", { name: "Room A" }, { offlinePolicy: "queue" });
    const second = client.callRaw("setName", { name: "Room B" }, { offlinePolicy: "queue" });

    await client.connect();

    await expect(first).rejects.toMatchObject({ code: ErrorCode.TransportDisconnected });
    await expect(second).resolves.toBe("Room B");
    expect(received).toHaveLength(2);
    expect(received[0]).toEqual({ id: expect.any(Number), params: { name: "Room A" } });
    expect(received[1]).toEqual({ id: expect.any(Number), params: { name: "Room B" } });
    expect(received[1].id).not.toBe(received[0].id);

    await client.close();
    await server.close();
  });

  it("rejects new queued RPC calls when the call queue is full", async () => {
    const loop = createMockStreamLoopback();
    const client = new AxtpClient(loop.client, {
      logicalRole: "client",
      delivery: { calls: { queue: { maxSize: 1, overflow: "reject" } } }
    });

    const first = client.callRaw("setName", { name: "Room A" }, { offlinePolicy: "queue" });
    await expect(
      client.callRaw("setName", { name: "Room B" }, { offlinePolicy: "queue" })
    ).rejects.toMatchObject({ code: ErrorCode.InvalidState });

    await client.close();
    await expect(first).rejects.toMatchObject({ code: ErrorCode.TransportDisconnected });
  });

  it("coalesces queued RPC calls using delivery.calls.methods policy", async () => {
    const loop = createMockStreamLoopback();
    const received: unknown[] = [];
    const server = new AxtpServer(loop.server, {
      logicalRole: "server",
      heartbeatIntervalMs: 60000
    });
    const client = new AxtpClient(loop.client, {
      logicalRole: "client",
      heartbeatIntervalMs: 60000,
      delivery: {
        calls: {
          methods: {
            setName: {
              offlinePolicy: "queue",
              coalesceKey: "setName",
              coalescePrevious: "resolve-with-next"
            }
          }
        }
      }
    });
    server.handleRaw("setName", (_ctx, p) => {
      received.push(p);
      return `applied:${(p as { name: string }).name}`;
    });

    await server.listen();
    const first = client.callRaw("setName", { name: "Room A" });
    const second = client.callRaw("setName", { name: "Room B" });
    const third = client.callRaw("setName", { name: "Room C" });

    await client.connect();

    await expect(first).resolves.toBe("applied:Room C");
    await expect(second).resolves.toBe("applied:Room C");
    await expect(third).resolves.toBe("applied:Room C");
    expect(received).toEqual([{ name: "Room C" }]);

    await client.close();
    await server.close();
  });

  it("rejects queued RPC calls when reconnect attempts are exhausted", async () => {
    const failingTransport: StreamClientTransport = {
      profile: framedBinaryProfile("AXTP-TCP"),
      connect: () => Promise.reject(new Error("offline"))
    };
    const client = new AxtpClient(failingTransport, {
      logicalRole: "client",
      reconnect: {
        enabled: true,
        initialDelayMs: 1,
        maxDelayMs: 1,
        maxAttempts: 1,
        jitter: false
      }
    });

    const connected = client.connect(50);
    const called = client.callRaw("setName", { name: "Room A" }, { offlinePolicy: "queue" });

    await expect(connected).rejects.toMatchObject({ code: ErrorCode.TransportDisconnected });
    await expect(called).rejects.toMatchObject({ code: ErrorCode.TransportDisconnected });
  });

  it("rejects queued client events and wait-ready calls when closed before ready", async () => {
    const loop = createMockStreamLoopback();
    const client = new AxtpClient(loop.client, {
      logicalRole: "client",
      delivery: { events: { offline: "queue" } }
    });

    const emitted = client.emitRaw("queued", { n: 1 });
    const called = client.callRaw("add", { a: 1, b: 2 }, { offlinePolicy: "wait-ready" });
    await client.close();

    await expect(emitted).rejects.toMatchObject({ code: ErrorCode.TransportDisconnected });
    await expect(called).rejects.toMatchObject({ code: ErrorCode.TransportDisconnected });
  });

  it("rejects queued client events when reconnect attempts are exhausted", async () => {
    const failingTransport: StreamClientTransport = {
      profile: framedBinaryProfile("AXTP-TCP"),
      connect: () => Promise.reject(new Error("offline"))
    };
    const client = new AxtpClient(failingTransport, {
      logicalRole: "client",
      reconnect: {
        enabled: true,
        initialDelayMs: 1,
        maxDelayMs: 1,
        maxAttempts: 1,
        jitter: false
      },
      delivery: { events: { offline: "queue" } }
    });

    const connected = client.connect(50);
    const emitted = client.emitRaw("queued", { n: 1 });

    await expect(connected).rejects.toMatchObject({ code: ErrorCode.TransportDisconnected });
    await expect(emitted).rejects.toMatchObject({ code: ErrorCode.TransportDisconnected });
  });

  it("client.emit → server.on 收到", async () => {
    let received: unknown;
    const { server, client } = await setupStandard((s) => {
      s.onRaw("ping", (data) => {
        received = data;
      });
    });
    client.emitRaw("ping", { hello: "world" });
    await new Promise((r) => setTimeout(r, 20));
    expect(received).toEqual({ hello: "world" });
    await client.close();
    await server.close();
  });

  it("server.emit 广播；client.on 收到", async () => {
    const { server, client } = await setupStandard();
    let received: unknown;
    const evt: string = "broadcast";
    client.onRaw(evt, (data) => {
      received = data;
    });
    await new Promise((r) => setTimeout(r, 10));
    await server.emitRaw(evt, { x: 1 });
    await new Promise((r) => setTimeout(r, 20));
    expect(received).toEqual({ x: 1 });
    await client.close();
    await server.close();
  });

  it("server.call(id) 单播", async () => {
    const { server, client } = await setupStandard();
    client.handleRaw("echo", (_ctx, p) => p);
    const endpoints = server.getEndpoints();
    expect(endpoints.length).toBe(1);
    const id = server.getId(endpoints[0]) as number;
    const result = await server.callRaw(id, "echo", { msg: "hi" });
    expect(result).toEqual({ msg: "hi" });
    await client.close();
    await server.close();
  });

  it("close → onDisconnect/onClose", async () => {
    const { server, client } = await setupStandard();
    await client.close();
    await once(server.onDisconnect);
    await server.close();
    expect(client.isClosed).toBe(true);
    expect(server.isClosed).toBe(true);
  });

  it("CallContext.id 在 server handler 中可用（numeric localId）", async () => {
    let capturedId: number | undefined;
    const { server, client } = await setupStandard((s) => {
      s.handleRaw("getId", (ctx) => {
        capturedId = ctx.id;
        return ctx.id;
      });
    });
    const result = await client.callRaw("getId", {});
    expect(capturedId).toBeDefined();
    expect(typeof capturedId).toBe("number");
    expect(result).toBe(capturedId);
    await client.close();
    await server.close();
  });

  it("server.emitTo(id) 定向发送到指定 endpoint", async () => {
    const { server, client } = await setupStandard();
    let received: unknown;
    client.onRaw("targeted", (data) => {
      received = data;
    });
    await new Promise((r) => setTimeout(r, 10));
    const endpoints = server.getEndpoints();
    const id = server.getId(endpoints[0]) as number;
    await server.emitToRaw(id, "targeted", { direct: true });
    await new Promise((r) => setTimeout(r, 20));
    expect(received).toEqual({ direct: true });
    await client.close();
    await server.close();
  });

  it("server.emitTo(不存在 id) 为 no-op（不抛错）", async () => {
    const { server, client } = await setupStandard();
    await expect(server.emitToRaw(99999, "noop", {})).resolves.toBeUndefined();
    await client.close();
    await server.close();
  });

  it("handler 内通过 ctx.id 调用 server.callRaw 回调同一 endpoint", async () => {
    const { server, client } = await setupStandard((s) => {
      s.handleRaw("trigger", (ctx) => {
        // handler 收到 Request 后，用 ctx.id 向同一 endpoint 发起 RPC
        void server.callRaw(ctx.id as number, "ack", {}).then(() => {
          // ack 成功
        });
        return "triggered";
      });
    });
    let ackReceived = false;
    client.handleRaw("ack", () => {
      ackReceived = true;
      return "ok";
    });
    const result = await client.callRaw("trigger", {});
    expect(result).toBe("triggered");
    await new Promise((r) => setTimeout(r, 50));
    expect(ackReceived).toBe(true);
    await client.close();
    await server.close();
  });

  it("ctx.emitRaw 在 handler 内发送事件 → client.onRaw 收到", async () => {
    let received: unknown;
    const { server, client } = await setupStandard((s) => {
      s.handleRaw("trigger", (ctx) => {
        ctx.emitRaw("handlerEvent", { from: "handler" });
        return "ok";
      });
    });
    client.onRaw("handlerEvent", (data) => {
      received = data;
    });
    await new Promise((r) => setTimeout(r, 10));
    await client.callRaw("trigger", {});
    await new Promise((r) => setTimeout(r, 30));
    expect(received).toEqual({ from: "handler" });
    await client.close();
    await server.close();
  });

  it("ctx.emit typed 在 handler 内发送事件 → client.on 收到", async () => {
    let received: unknown;
    const { server, client } = await setupStandard((s) => {
      s.handleRaw("trigger", (ctx) => {
        ctx.emit("cast.sessionStateChanged", { receiverPhase: "playing" });
        return "ok";
      });
    });
    client.onRaw("cast.sessionStateChanged", (data) => {
      received = data;
    });
    await new Promise((r) => setTimeout(r, 10));
    await client.callRaw("trigger", {});
    await new Promise((r) => setTimeout(r, 30));
    expect(received).toEqual({ receiverPhase: "playing" });
    await client.close();
    await server.close();
  });

  it("ep.emit 在 onConnect 回调发送事件 → client 收到", async () => {
    let received: unknown;
    const loop = createMockStreamLoopback();
    const server = new AxtpServer(loop.server, {
      logicalRole: "server",
      heartbeatIntervalMs: 60000
    });
    const client = new AxtpClient(loop.client, {
      logicalRole: "client",
      heartbeatIntervalMs: 60000
    });
    client.onRaw("welcome", (data) => {
      received = data;
    });
    server.onConnect.subscribe((ep) => {
      ep.emit("welcome", { hello: "client" });
    });
    const clientReady = once(client.onConnect);
    await server.listen();
    void client.connect().catch(() => {});
    await clientReady;
    await new Promise((r) => setTimeout(r, 30));
    expect(received).toEqual({ hello: "client" });
    await client.close();
    await server.close();
  });

  it("diagnostics 记录事件监听、出站、入站和无 handler 分发", async () => {
    const clientLogs: AxtpDiagnosticEntry[] = [];
    const serverLogs: AxtpDiagnosticEntry[] = [];
    const loop = createMockStreamLoopback();
    const server = new AxtpServer(loop.server, {
      logicalRole: "server",
      heartbeatIntervalMs: 60000,
      diagnostics: {
        includePayload: true,
        logger: (entry) => serverLogs.push(entry)
      }
    });
    const client = new AxtpClient(loop.client, {
      logicalRole: "client",
      heartbeatIntervalMs: 60000,
      diagnostics: {
        includePayload: true,
        logger: (entry) => clientLogs.push(entry)
      }
    });

    client.onRaw("cast.sessionStateChanged", () => {});
    const clientReady = once(client.onConnect);
    const serverReady = once(server.onConnect);
    await server.listen();
    void client.connect().catch(() => {});
    await clientReady;
    await serverReady;

    await server.emitRaw("cast.sessionStateChanged", { receiverPhase: "playing" });
    await client.emitRaw("cast.sessionStarted", { sessionId: "s1" });
    await new Promise((r) => setTimeout(r, 30));

    expect(clientLogs).toContainEqual(
      expect.objectContaining({
        scope: "client",
        event: "listener.add",
        name: "cast.sessionStateChanged",
        known: true
      })
    );
    expect(clientLogs).toContainEqual(
      expect.objectContaining({
        scope: "client",
        event: "identify.eventMasks",
        data: expect.objectContaining({ events: ["cast.sessionStateChanged"] })
      })
    );
    expect(serverLogs).toContainEqual(
      expect.objectContaining({
        scope: "core",
        event: "rpc.out.event",
        direction: "out",
        name: "cast.sessionStateChanged"
      })
    );
    expect(clientLogs).toContainEqual(
      expect.objectContaining({
        scope: "core",
        event: "rpc.in.event",
        direction: "in",
        name: "cast.sessionStateChanged"
      })
    );
    expect(clientLogs).toContainEqual(
      expect.objectContaining({
        scope: "broker",
        event: "event.dispatch",
        name: "cast.sessionStateChanged",
        handlerCount: 1
      })
    );
    expect(serverLogs).toContainEqual(
      expect.objectContaining({
        scope: "broker",
        event: "event.dispatch",
        name: "cast.sessionStarted",
        handlerCount: 0
      })
    );

    await client.close();
    await server.close();
  });

  it("diagnostics logger errors do not break runtime event flow", async () => {
    let received: unknown;
    const loop = createMockStreamLoopback();
    const server = new AxtpServer(loop.server, {
      logicalRole: "server",
      heartbeatIntervalMs: 60000,
      diagnostics: {
        includePayload: true,
        logger: () => {
          throw new Error("logger failed");
        }
      }
    });
    const client = new AxtpClient(loop.client, {
      logicalRole: "client",
      heartbeatIntervalMs: 60000,
      diagnostics: {
        logger: () => {
          throw new Error("logger failed");
        }
      }
    });

    expect(() =>
      client.onRaw("safe", (data) => {
        received = data;
      })
    ).not.toThrow();

    const clientReady = once(client.onConnect);
    const serverReady = once(server.onConnect);
    await server.listen();
    void client.connect().catch(() => {});
    await clientReady;
    await serverReady;

    await expect(server.emitRaw("safe", { ok: true })).resolves.toBeUndefined();
    await new Promise((r) => setTimeout(r, 30));
    expect(received).toEqual({ ok: true });

    await client.close();
    await server.close();
  });
});
