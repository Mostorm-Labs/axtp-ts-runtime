import { afterEach, describe, expect, it } from "vitest";
import { WebSocketServer, type WebSocket } from "ws";
import { RpcOp } from "../../src/protocol/model.js";
import { AxtpClient } from "../../src/sdk/client.js";
import { NodeWsClientTransport } from "../../src/transport/ws/nodeWsTransport.js";
import { ErrorCode } from "../../src/types/error.js";

interface RawPeer {
  readonly url: string;
  readonly connectionCount: () => number;
  close(): Promise<void>;
}

async function startRawPeer(onConnection: (socket: WebSocket) => void): Promise<RawPeer> {
  const server = new WebSocketServer({ host: "127.0.0.1", port: 0 });
  let connections = 0;
  server.on("connection", (socket) => {
    connections += 1;
    onConnection(socket);
  });
  await new Promise<void>((resolve, reject) => {
    server.once("listening", resolve);
    server.once("error", reject);
  });
  const address = server.address();
  if (typeof address !== "object" || address === null) throw new Error("missing test server port");
  return {
    url: `ws://127.0.0.1:${address.port}`,
    connectionCount: () => connections,
    close: () =>
      new Promise<void>((resolve) => {
        for (const socket of server.clients) socket.terminate();
        server.close(() => resolve());
      })
  };
}

function sendHello(socket: WebSocket, version: string): void {
  socket.send(JSON.stringify({ sid: "", op: RpcOp.Hello, d: { axtpVersion: version } }));
}

describe("AxtpClient connection errors over real WebSocket", () => {
  const peers: RawPeer[] = [];
  const clients: AxtpClient[] = [];

  afterEach(async () => {
    while (clients.length > 0) await clients.pop()?.close();
    while (peers.length > 0) await peers.pop()?.close();
  });

  it("accepts an incompatible advisory version when the peer completes the handshake", async () => {
    const peer = await startRawPeer((socket) => {
      sendHello(socket, "2.0.0");
      socket.once("message", () => {
        socket.send(JSON.stringify({ sid: "advisory", op: RpcOp.Identified, d: {} }));
      });
    });
    peers.push(peer);
    const client = new AxtpClient(new NodeWsClientTransport({ url: peer.url }), {
      logicalRole: "client"
    });
    clients.push(client);
    await client.connect(1_000);
    expect(client.isReady).toBe(true);
  });

  it("does not reject a major-different advisory version", async () => {
    const peer = await startRawPeer((socket) => {
      sendHello(socket, "2.0.0");
      socket.once("message", () => {
        socket.send(JSON.stringify({ sid: "major-different", op: RpcOp.Identified, d: {} }));
      });
    });
    peers.push(peer);
    const client = new AxtpClient(new NodeWsClientTransport({ url: peer.url }), {
      logicalRole: "client",
      reconnect: { enabled: true, initialDelayMs: 1, maxDelayMs: 1, maxAttempts: 3, jitter: false }
    });
    clients.push(client);

    await client.connect(1_000);
    expect(client.isReady).toBe(true);
    expect(peer.connectionCount()).toBe(1);
  });

  it("accepts an incompatible advisory version after reconnect", async () => {
    const peer = await startRawPeer((socket) => {
      sendHello(socket, "2.0.0");
      socket.once("message", () => {
        socket.send(JSON.stringify({ sid: "reconnected", op: RpcOp.Identified, d: {} }));
      });
    });
    peers.push(peer);
    const client = new AxtpClient(new NodeWsClientTransport({ url: peer.url }), {
      logicalRole: "client",
      reconnect: {
        enabled: true,
        initialDelayMs: 1,
        maxDelayMs: 1,
        maxAttempts: 3,
        jitter: false
      }
    });
    clients.push(client);
    await client.connect(1_000);
    expect(peer.connectionCount()).toBe(1);
    expect(client.isReady).toBe(true);
  });

  it("retries a handshake timeout and succeeds on the next real WebSocket connection", async () => {
    let connection = 0;
    const peer = await startRawPeer((socket) => {
      connection += 1;
      if (connection === 1) return;
      sendHello(socket, "1.0.0");
      socket.once("message", () => {
        socket.send(JSON.stringify({ sid: "recovered", op: RpcOp.Identified, d: {} }));
      });
    });
    peers.push(peer);
    const client = new AxtpClient(new NodeWsClientTransport({ url: peer.url }), {
      logicalRole: "client",
      handshakeTimeoutMs: 20,
      reconnect: {
        enabled: true,
        initialDelayMs: 1,
        maxDelayMs: 1,
        maxAttempts: 2,
        jitter: false
      }
    });
    clients.push(client);
    const observedErrors: Error[] = [];
    client.onError.subscribe((error) => observedErrors.push(error));

    await client.connect(1_000);

    expect(peer.connectionCount()).toBe(2);
    expect(client.isReady).toBe(true);
    expect(observedErrors).toContainEqual(
      expect.objectContaining({
        code: ErrorCode.Timeout,
        message: "handshake timed out after 20ms"
      })
    );
  });

  it("rejects with the handshake timeout when reconnect is disabled", async () => {
    const peer = await startRawPeer(() => {
      // Keep the real WebSocket open without sending AXTP Hello.
    });
    peers.push(peer);
    const client = new AxtpClient(new NodeWsClientTransport({ url: peer.url }), {
      logicalRole: "client",
      handshakeTimeoutMs: 20
    });
    clients.push(client);

    await expect(client.connect(1_000)).rejects.toMatchObject({
      code: ErrorCode.Timeout,
      message: "handshake timed out after 20ms"
    });

    expect(client.isClosed).toBe(true);
  });
});
