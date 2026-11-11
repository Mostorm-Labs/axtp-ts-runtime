// 真实 WebSocket loopback：AxtpEndpoint over NodeWsTransport（unframed-json）。
// 验证新栈在真实 WS 上完成 RPC 握手（无 CONTROL）+ call。

import { describe, expect, it } from "vitest";
import { AxtpEndpoint } from "../../src/endpoint/endpoint.js";
import {
  NodeWsClientTransport,
  NodeWsServerTransport
} from "../../src/transport/ws/nodeWsTransport.js";
import { once } from "../helpers/eventStreamHelpers.js";

describe("AxtpEndpoint over 真实 WebSocket", () => {
  it("unframed-json 握手 + client.call → server handler → response", async () => {
    const server = new NodeWsServerTransport({ port: 0 });
    await server.listen();
    const port = server.boundPort as number;

    const serverEpPromise = new Promise<AxtpEndpoint>((resolve) => {
      server.onConnection.subscribe((t) => {
        const ep = new AxtpEndpoint({
          transport: t,
          physicalRole: "server",
          logicalRole: "server",
          maxFrameSize: 4096,
          heartbeatIntervalMs: 60000,
          handshakeSeed: 1
        });
        ep.broker.setMethod("add", (_ctx, p) => (p as { a: number }).a + (p as { b: number }).b);
        ep.start();
        resolve(ep);
      });
    });

    const clientT = await new NodeWsClientTransport({
      url: `ws://127.0.0.1:${port}`
    }).connect();
    const clientEp = new AxtpEndpoint({
      transport: clientT,
      physicalRole: "client",
      logicalRole: "client",
      maxFrameSize: 4096,
      heartbeatIntervalMs: 60000
    });
    clientEp.start();

    const serverEp = await serverEpPromise;
    await Promise.all([once(serverEp.onReady), once(clientEp.onReady)]);
    expect(clientEp.isReady).toBe(true);

    const result = await clientEp.call("add", { a: 2, b: 3 });
    expect(result).toBe(5);

    clientEp.close();
    serverEp.close();
    await server.close();
  });

  // 回归：readable 取消（endpoint.close → pipe 链 abort，如「同设备新会话踢旧」）后，
  // close 握手竞态下 ws 仍会派发缓冲的迟到 message 帧——不得向已关闭 controller enqueue，
  // 否则同步抛 ERR_INVALID_STATE 成为未捕获异常击穿进程（aw-saas-platform 曾以 pnpm patch 修复）。
  it("readable 取消后的迟到 message 帧被丢弃，不产生未捕获异常", async () => {
    const uncaught: Error[] = [];
    const onUncaught = (err: Error): void => {
      uncaught.push(err);
    };
    process.on("uncaughtException", onUncaught);
    try {
      const server = new NodeWsServerTransport({ port: 0 });
      await server.listen();
      const port = server.boundPort as number;

      const serverTPromise = once(server.onConnection);
      const clientT = await new NodeWsClientTransport({
        url: `ws://127.0.0.1:${port}`
      }).connect();
      const serverT = await serverTPromise;

      // 模拟被踢端 endpoint.close → pipe 链 abort → readable cancel
      await serverT.readable.getReader().cancel();

      // 取消后从对端再发一帧（迟到帧，message 事件仍会派发）
      await clientT.writable.getWriter().write(new TextEncoder().encode('{"late":true}'));

      // 等待 ws message 事件派发完毕
      await new Promise((resolve) => setTimeout(resolve, 100));

      clientT.close();
      serverT.close();
      await server.close();
    } finally {
      process.off("uncaughtException", onUncaught);
    }
    expect(uncaught).toEqual([]);
  });
}, 15000);
