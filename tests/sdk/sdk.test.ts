// AxtpClient / AxtpServer over mock stream loopback（新栈 SDK 集成）。
// 覆盖 connect/call/handle/emit/广播/单播/close。

import { describe, expect, it } from "vitest";
import { AxtpClient } from "../../src/sdk/client.js";
import { AxtpServer } from "../../src/sdk/server.js";
import { createMockStreamLoopback } from "../../src/transport/mock/mockStreamTransport.js";
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
});
