import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { AxtpClient } from "../../src/sdk/client.js";
import { AxtpServer } from "../../src/sdk/server.js";
import {
  NodeTcpClientTransport,
  NodeTcpServerTransport
} from "../../src/transport/tcp/nodeTcpTransport.js";
import {
  NodeWsClientTransport,
  NodeWsServerTransport
} from "../../src/transport/ws/nodeWsTransport.js";

function settle(ms = 30): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

// 找一个空闲端口
let port = 18700;
function nextPort(): number {
  return port++;
}

describe("回环 TCP（framed-binary）真实网络", () => {
  let server: AxtpServer;
  let client: AxtpClient;
  const tcpPort = nextPort();

  beforeAll(async () => {
    const serverTransport = new NodeTcpServerTransport({ port: tcpPort });
    server = new AxtpServer(serverTransport);
    await server.listen();
  });

  afterAll(async () => {
    await client?.close();
    await server?.close();
  });

  it("握手 + 双向 RPC（真实 TCP）", async () => {
    server.handle("audio.getAlgorithmConfig", () => ({ real: "tcp", ok: true }));

    const clientTransport = new NodeTcpClientTransport({ port: tcpPort });
    client = new AxtpClient(clientTransport);
    await client.connect();

    const result = await client.call("audio.getAlgorithmConfig", {});
    expect(result).toEqual({ real: "tcp", ok: true });
  });

  it("client handle -> server call（反向）", async () => {
    client.handle("audio.getAlgorithmConfig", () => ({ reverse: true }));
    await settle(20);
    const sessionId = server.getSessions()[0].id;
    const result = await server.call(sessionId, "audio.getAlgorithmConfig", {});
    expect(result).toEqual({ reverse: true });
  });
});

describe("回环 WebSocket（unframed-json）真实网络", () => {
  let server: AxtpServer;
  let client: AxtpClient;
  const wsPort = nextPort();

  beforeAll(async () => {
    const serverTransport = new NodeWsServerTransport({ port: wsPort });
    server = new AxtpServer(serverTransport);
    await server.listen();
  });

  afterAll(async () => {
    await client?.close();
    await server?.close();
  });

  it("握手 + RPC（真实 WS）", async () => {
    server.handle("audio.getAlgorithmConfig", () => ({ real: "ws", ok: true }));
    const clientTransport = new NodeWsClientTransport({ url: `ws://127.0.0.1:${wsPort}` });
    client = new AxtpClient(clientTransport);
    await client.connect();
    const result = await client.call("audio.getAlgorithmConfig", {});
    expect(result).toEqual({ real: "ws", ok: true });
  });

  it("WS 事件广播", async () => {
    const received: unknown[] = [];
    client.on("audio.algorithmConfigChanged", (p) => received.push(p));
    await settle(20);
    const sessionId = server.getSessions()[0].id;
    const session = server.getSession(sessionId);
    await session?.emit("audio.algorithmConfigChanged", { event: "ws" });
    await settle(30);
    expect(received.length).toBe(1);
    expect(received[0]).toEqual({ event: "ws" });
  });
});
