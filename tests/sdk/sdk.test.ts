// AxtpClient / AxtpServer over mock stream loopback（新栈 SDK 集成）。
// 覆盖 connect/call/handle/emit/广播/单播/close。

import { describe, expect, it } from "vitest";
import type { AxtpDiagnosticEntry } from "../../src/diagnostics.js";
import { AxtpClient } from "../../src/sdk/client.js";
import { AxtpServer } from "../../src/sdk/server.js";
import type { StreamClientTransport } from "../../src/transport/contract.js";
import { ErrorCode } from "../../src/types/error.js";
import { createMockStreamLoopback } from "../../src/transport/mock/mockStreamTransport.js";
import { framedBinaryProfile } from "../../src/transport/profile.js";
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

  it("queues client events emitted before ready when outbox is enabled", async () => {
    let received: unknown;
    const loop = createMockStreamLoopback();
    const server = new AxtpServer(loop.server, { logicalRole: "server", heartbeatIntervalMs: 60000 });
    const client = new AxtpClient(loop.client, {
      logicalRole: "client",
      heartbeatIntervalMs: 60000,
      outbox: { enabled: true }
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

  it("rejects new client events when the outbox is full", async () => {
    const loop = createMockStreamLoopback();
    const client = new AxtpClient(loop.client, {
      logicalRole: "client",
      outbox: { enabled: true, maxSize: 1, overflow: "reject" }
    });

    const first = client.emitRaw("queued", { n: 1 });
    await expect(client.emitRaw("queued", { n: 2 })).rejects.toMatchObject({
      code: ErrorCode.InvalidState
    });
    await client.close();
    await expect(first).rejects.toMatchObject({ code: ErrorCode.TransportDisconnected });
  });

  it("drops newest client events when configured outbox overflow is drop-newest", async () => {
    const loop = createMockStreamLoopback();
    const received: unknown[] = [];
    const server = new AxtpServer(loop.server, { logicalRole: "server", heartbeatIntervalMs: 60000 });
    const client = new AxtpClient(loop.client, {
      logicalRole: "client",
      heartbeatIntervalMs: 60000,
      outbox: { enabled: true, maxSize: 1, overflow: "drop-newest" }
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

  it("drops oldest client events when configured outbox overflow is drop-oldest", async () => {
    const loop = createMockStreamLoopback();
    const received: unknown[] = [];
    const server = new AxtpServer(loop.server, { logicalRole: "server", heartbeatIntervalMs: 60000 });
    const client = new AxtpClient(loop.client, {
      logicalRole: "client",
      heartbeatIntervalMs: 60000,
      outbox: { enabled: true, maxSize: 1, overflow: "drop-oldest" }
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
    const server = new AxtpServer(loop.server, { logicalRole: "server", heartbeatIntervalMs: 60000 });
    const client = new AxtpClient(loop.client, { logicalRole: "client", heartbeatIntervalMs: 60000 });
    server.handleRaw("add", (_ctx, p) => (p as { a: number }).a + (p as { b: number }).b);

    await server.listen();
    const called = client.callRaw("add", { a: 4, b: 6 }, { offlinePolicy: "wait-ready" });
    await client.connect();
    await expect(called).resolves.toBe(10);

    await client.close();
    await server.close();
  });

  it("rejects queued client events and wait-ready calls when closed before ready", async () => {
    const loop = createMockStreamLoopback();
    const client = new AxtpClient(loop.client, {
      logicalRole: "client",
      outbox: { enabled: true }
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
      outbox: { enabled: true }
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
});
