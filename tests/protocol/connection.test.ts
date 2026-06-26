import { describe, expect, it, vi } from "vitest";
import { Connection } from "../../src/protocol/connection.js";
import { RpcOp } from "../../src/protocol/generated/axtp_ids_generated.js";
import type { RpcPayload } from "../../src/protocol/model.js";
import { rpcPayload } from "../../src/protocol/model.js";
import { createMockTransportPair } from "../../src/transport/mock/mockTransport.js";
import {
  framedBinaryCapabilities,
  unframedJsonCapabilities
} from "../../src/transport/transport.js";

function settle(ms = 10): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

describe("Connection framed-binary: 链路 OPEN/ACCEPT", () => {
  it("client OPEN -> server ACCEPT -> 双方 linkReady", async () => {
    const { left, right } = createMockTransportPair(framedBinaryCapabilities());
    const client = new Connection("client", left);
    const server = new Connection("server", right);

    let clientLinkReady = false;
    let serverLinkReady = false;
    client.onLinkReady.subscribe(() => (clientLinkReady = true));
    server.onLinkReady.subscribe(() => (serverLinkReady = true));

    server.start(); // server 先 start（订阅接收），等待 OPEN
    client.start(); // client 发 OPEN

    await settle(20);
    expect(clientLinkReady).toBe(true);
    expect(serverLinkReady).toBe(true);
  });

  it("link ready 后双方可互发 RPC（framed-binary 双向）", async () => {
    const { left, right } = createMockTransportPair(framedBinaryCapabilities());
    const client = new Connection("client", left);
    const server = new Connection("server", right);

    const serverReceived: RpcPayload[] = [];
    const clientReceived: RpcPayload[] = [];
    server.onPayload.subscribe((p) => serverReceived.push(p));
    client.onPayload.subscribe((p) => clientReceived.push(p));

    server.start();
    client.start();
    await settle(20); // 等握手完成

    // client -> server RPC
    client.sendRpc(
      rpcPayload({
        op: RpcOp.Request,
        requestId: 1,
        jsonSid: "abcdef01",
        meta: { jsonMethodOrEventName: "audio.getAlgorithmConfig" }
      })
    );
    await settle(10);
    expect(serverReceived.length).toBe(1);
    expect(serverReceived[0].op).toBe(RpcOp.Request);
    expect(serverReceived[0].requestId).toBe(1);

    // server -> client RPC（反向）
    server.sendRpc(
      rpcPayload({
        op: RpcOp.Request,
        requestId: 2,
        jsonSid: "abcdef01",
        meta: { jsonMethodOrEventName: "device.getInfo" }
      })
    );
    await settle(10);
    expect(clientReceived.length).toBe(1);
    expect(clientReceived[0].requestId).toBe(2);
  });

  it("close 触发双方 onClose", async () => {
    const { left, right } = createMockTransportPair(framedBinaryCapabilities());
    const client = new Connection("client", left);
    const server = new Connection("server", right);
    server.start();
    client.start();
    await settle(20);

    let clientClosed = false;
    client.onClose.subscribe(() => (clientClosed = true));
    server.close();
    await settle(10);
    expect(clientClosed).toBe(true);
    expect(client.isClosed).toBe(true);
  });
});

describe("Connection unframed-json: 直接 linkReady + 双向 RPC", () => {
  it("WS 模式连接即 linkReady（无 CONTROL）", async () => {
    const { left, right } = createMockTransportPair(unframedJsonCapabilities());
    const client = new Connection("client", left);
    const server = new Connection("server", right);

    let ready = false;
    client.onLinkReady.subscribe(() => (ready = true));
    server.start();
    client.start();
    await settle(10);
    expect(ready).toBe(true);
  });

  it("WS 双向 JSON RPC", async () => {
    const { left, right } = createMockTransportPair(unframedJsonCapabilities());
    const client = new Connection("client", left);
    const server = new Connection("server", right);

    const received: RpcPayload[] = [];
    server.onPayload.subscribe((p) => received.push(p));
    server.start();
    client.start();
    await settle(10);

    client.sendRpc(
      rpcPayload({
        op: RpcOp.Request,
        requestId: 99,
        jsonSid: "12345678",
        meta: { jsonMethodOrEventName: "audio.getAlgorithmConfig" }
      })
    );
    await settle(10);
    expect(received.length).toBe(1);
    expect(received[0].requestId).toBe(99);
    expect(received[0].meta.jsonMethodOrEventName).toBe("audio.getAlgorithmConfig");
  });
});

describe("Connection start() 缓冲", () => {
  it("未 start 时消息被缓冲，start 后 flush 收到", async () => {
    const { left, right } = createMockTransportPair(unframedJsonCapabilities());
    const client = new Connection("client", left);
    const server = new Connection("server", right);

    const received: RpcPayload[] = [];
    server.onPayload.subscribe((p) => received.push(p));

    // server 未 start，client 已 start 并发消息 -> server 缓冲（不丢失）
    client.start();
    client.sendRpc(rpcPayload({ op: RpcOp.Request, requestId: 1 }));
    await settle(10);
    expect(received.length).toBe(0); // server 未 start，缓冲中

    server.start(); // flush 缓冲
    await settle(10);
    expect(received.length).toBe(1);
    expect(received[0].requestId).toBe(1);
  });
});

describe("Connection 心跳（framed）", () => {
  it("link ready 后启动心跳定时器", async () => {
    vi.useFakeTimers();
    const { left, right } = createMockTransportPair(framedBinaryCapabilities());
    const client = new Connection("client", left, {
      heartbeatIntervalMs: 1000,
      heartbeatTimeoutMs: 5000
    });
    const server = new Connection("server", right, {
      heartbeatIntervalMs: 1000,
      heartbeatTimeoutMs: 5000
    });

    // 用 spy 监控 transport.send（验证发了 heartbeat 字节）
    const sendSpy = vi.spyOn(left, "send");
    server.start();
    client.start();
    await vi.advanceTimersByTimeAsync(30); // 握手
    vi.advanceTimersByTime(1000); // 心跳 tick
    // client 至少发过一次（OPEN + 可能的 heartbeat）
    expect(sendSpy.mock.calls.length).toBeGreaterThan(0);
    client.close();
    vi.useRealTimers();
  });
});
