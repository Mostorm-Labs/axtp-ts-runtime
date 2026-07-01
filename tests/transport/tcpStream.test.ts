// 真实 TCP loopback：AxtpEndpoint over NodeTcpStreamTransport（Duplex.toWeb）。
// 验证新栈在真实 socket 上完成 framed 握手 + RPC call + close。

import { describe, expect, it } from "vitest";
import { AxtpEndpoint } from "../../src/endpoint/endpoint.js";
import type { StreamTransport } from "../../src/transport/contract.js";
import {
  NodeTcpStreamClientTransport,
  NodeTcpStreamServerTransport
} from "../../src/transport/tcp/nodeTcpStreamTransport.js";
import { once } from "../helpers/eventStreamHelpers.js";

function serverEndpoint(t: StreamTransport): AxtpEndpoint {
  const ep = new AxtpEndpoint({
    transport: t,
    physicalRole: "server",
    logicalRole: "server",
    maxFrameSize: 4096,
    heartbeatIntervalMs: 60000,
    handshakeSeed: 1
  });
  ep.broker.setMethod("add", (_ctx, p) => (p as { a: number }).a + (p as { b: number }).b);
  return ep;
}

function clientEndpoint(t: StreamTransport): AxtpEndpoint {
  return new AxtpEndpoint({
    transport: t,
    physicalRole: "client",
    logicalRole: "client",
    maxFrameSize: 4096,
    heartbeatIntervalMs: 60000
  });
}

describe("AxtpEndpoint over 真实 TCP", () => {
  it("framed 握手 + client.call → server handler → response", async () => {
    const server = new NodeTcpStreamServerTransport({ port: 0 });
    await server.listen();
    const port = server.boundPort;
    expect(port).toBeGreaterThan(0);

    const serverEpPromise = new Promise<AxtpEndpoint>((resolve) => {
      server.onConnection.subscribe((t) => {
        const ep = serverEndpoint(t);
        ep.start();
        resolve(ep);
      });
    });

    const clientT = await new NodeTcpStreamClientTransport({ port: port as number }).connect();
    const clientEp = clientEndpoint(clientT);
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
