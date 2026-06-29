import { describe, expect, it, vi } from "vitest";
import { Connection } from "../../src/connection/connection.js";
import { RpcOp } from "../../src/protocol/generated/axtp_ids_generated.js";
import type { RpcPayload } from "../../src/protocol/model.js";
import { rpcPayload } from "../../src/protocol/model.js";
import { createMockTransportPair, MockTransport } from "../../src/transport/mock/mockTransport.js";
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

describe("Connection 心跳（WS unframed-json）", () => {
  it("link ready 后启动 ws keepalive 心跳", async () => {
    // 用真实 NodeWsTransport 回环：WS 心跳用 sendKeepalive/onKeepaliveAck。
    const { NodeWsServerTransport, NodeWsClientTransport } =
      await import("../../src/transport/ws/nodeWsTransport.js");
    const port = 19300;

    const serverTransport = new NodeWsServerTransport({ port });
    serverTransport.onConnection.subscribe((t) => {
      // server 侧 Connection 启动心跳
      const conn = new Connection("server", t, {
        heartbeatIntervalMs: 50,
        heartbeatTimeoutMs: 5000
      });
      conn.start();
    });
    await serverTransport.listen();

    const clientTransport = new NodeWsClientTransport({ url: `ws://127.0.0.1:${port}` });
    const clientConn = await clientTransport.connect();

    // client transport 支持 keepalive，spy sendKeepalive() 调用
    const keepaliveSpy = vi.spyOn(
      clientConn as unknown as { sendKeepalive: () => void },
      "sendKeepalive"
    );

    // client 端 Connection 启动（WS 模式 fireLinkReady + startHeartbeat）
    const clientConnection = new Connection("client", clientConn, {
      heartbeatIntervalMs: 50,
      heartbeatTimeoutMs: 5000
    });
    clientConnection.start();

    // 等待心跳周期（interval 50ms，等 200ms 应至少触发几次）
    await new Promise((r) => setTimeout(r, 200));

    // WS 心跳应已启动：client 的 sendKeepalive() 被定时调用
    expect(keepaliveSpy.mock.calls.length).toBeGreaterThan(0);

    clientConnection.close();
    await serverTransport.close();
  });
});

describe("Connection 重连修复回归", () => {
  it("本地 close() 不误发 onDisconnect、不武装重连复活（framed/unframed 通用）", async () => {
    const { left, right } = createMockTransportPair(unframedJsonCapabilities());
    const server = new Connection("server", right);
    server.start();

    let factoryCalls = 0;
    const client = new Connection(
      "client",
      left,
      { reconnect: { enabled: true, initialDelayMs: 5, jitter: false } },
      () => {
        factoryCalls += 1;
        return Promise.resolve(new MockTransport(unframedJsonCapabilities()));
      }
    );
    client.start();
    await settle(10); // unframed 立即 linkReady -> onSuccess（active=false）

    let disconnectCount = 0;
    client.onDisconnect.subscribe(() => {
      disconnectCount += 1;
    });

    client.close(); // 本地主动关闭
    await settle(20);

    // 修复后：本地关闭不发 onDisconnect；transport.close() 同步触发 onClose 时 connState 已 closed，
    // handleTransportClose 提前返回，不会在已 stop 的协调器上重新武装重连（“复活”已关闭连接）。
    expect(disconnectCount).toBe(0);
    expect(factoryCalls).toBe(0);
    expect(client.isClosed).toBe(true);
  });

  it("重连交还的新 transport 在 link-ready 前断开 -> 继续重连至 maxAttempts，不永久卡死", async () => {
    vi.useFakeTimers();
    try {
      const { left, right } = createMockTransportPair(framedBinaryCapabilities());
      const server = new Connection("server", right);
      server.start();

      let factoryCalls = 0;
      const client = new Connection(
        "client",
        left,
        { reconnect: { enabled: true, initialDelayMs: 1, maxAttempts: 5, jitter: false } },
        () => {
          factoryCalls += 1;
          const t = new MockTransport(framedBinaryCapabilities());
          // 新 transport 不连任何对端（OPEN 得不到 ACCEPT，link 永不会 ready），
          // 并在 attach 之后、link-ready 之前关闭 —— 正是原 bug 的卡死窗口。
          setTimeout(() => t.close(), 0);
          return Promise.resolve(t);
        }
      );
      client.start();
      await vi.advanceTimersByTimeAsync(20); // 初始 framed 握手完成 -> onSuccess

      let reconnectCount = 0;
      let failed = false;
      client.onReconnect.subscribe(() => {
        reconnectCount += 1;
      });
      client.onReconnectFailed.subscribe(() => {
        failed = true;
      });

      left.close(); // 触发重连
      // 退避 1+2+4+8+16ms + 各次 0ms 关闭，共 < 50ms 即可走完 5 次尝试并 onReconnectFailed。
      await vi.advanceTimersByTimeAsync(100);

      // 修复前：首次交还后 active 仍为 true，新 transport 断开时 start() 空转、timer 为陈旧 id，
      // 连接永久卡在 reconnecting（factoryCalls=1、永不 onReconnectFailed）。
      // 修复后：交还即置 active=false，每次断开都能重新编排，直到 maxAttempts -> onReconnectFailed。
      expect(factoryCalls).toBeGreaterThanOrEqual(2);
      expect(reconnectCount).toBeGreaterThanOrEqual(2);
      expect(failed).toBe(true);
      expect(client.isClosed).toBe(true);
    } finally {
      vi.useRealTimers();
    }
  });
});
