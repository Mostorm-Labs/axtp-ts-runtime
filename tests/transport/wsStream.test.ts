// 真实 WebSocket loopback：AxtpEndpoint over NodeWsStreamTransport（unframed-json）。
// 验证新栈在真实 WS 上完成 RPC 握手（无 CONTROL）+ call。

import { describe, expect, it } from "vitest";
import { AxtpEndpoint } from "../../src/endpoint/endpoint.js";
import {
  NodeWsStreamClientTransport,
  NodeWsStreamServerTransport
} from "../../src/transport/ws/nodeWsStreamTransport.js";
import { once } from "../helpers/eventStreamHelpers.js";

describe("AxtpEndpoint over 真实 WebSocket", () => {
  it("unframed-json 握手 + client.call → server handler → response", async () => {
    const server = new NodeWsStreamServerTransport({ port: 0 });
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

    const clientT = await new NodeWsStreamClientTransport({
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
}, 15000);
