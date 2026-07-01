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
      s.handle("add", (_ctx, p) => (p as { a: number }).a + (p as { b: number }).b);
    });
    expect(client.isReady).toBe(true);
    const result = await client.call("add", { a: 2, b: 3 });
    expect(result).toBe(5);
    await client.close();
    await server.close();
  });

  it("client.emit → server.on 收到", async () => {
    let received: unknown;
    const { server, client } = await setupStandard((s) => {
      s.on("ping", (data) => {
        received = data;
      });
    });
    client.emit("ping", { hello: "world" });
    await new Promise((r) => setTimeout(r, 20));
    expect(received).toEqual({ hello: "world" });
    await client.close();
    await server.close();
  });

  it("server.emit 广播；client.on 收到", async () => {
    const { server, client } = await setupStandard();
    let received: unknown;
    const evt: string = "broadcast";
    client.on(evt, (data) => {
      received = data;
    });
    await new Promise((r) => setTimeout(r, 10));
    await (server.emit as (event: string, payload: unknown) => Promise<void>)(evt, { x: 1 });
    await new Promise((r) => setTimeout(r, 20));
    expect(received).toEqual({ x: 1 });
    await client.close();
    await server.close();
  });

  it("server.call(localId) 单播", async () => {
    const { server, client } = await setupStandard();
    client.handle("echo", (_ctx, p) => p);
    const endpoints = server.getEndpoints();
    expect(endpoints.length).toBe(1);
    const localId = server.getLocalId(endpoints[0]) as number;
    const result = await server.call(localId, "echo", { msg: "hi" });
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
});
