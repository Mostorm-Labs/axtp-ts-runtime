import { describe, expect, it, vi } from "vitest";
import { Connection } from "../../src/connection/connection.js";
import { RpcOp } from "../../src/protocol/generated/axtp_ids_generated.js";
import type { RpcMessage } from "../../src/protocol/model.js";
import { requestMsg } from "../../src/protocol/model.js";
import { createMockTransportPair, MockTransport } from "../../src/transport/mock/mockTransport.js";
import {
  CloseCode,
  framedBinaryCapabilities,
  unframedJsonCapabilities
} from "../../src/transport/transport.js";

function settle(ms = 10): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

describe("Connection framed-binary: 链路 OPEN/ACCEPT", () => {
  it("client OPEN -> server ACCEPT -> 双方 linkReady", async () => {
    const { left, right } = createMockTransportPair(framedBinaryCapabilities());
    const client = new Connection("client", () => Promise.resolve(left));
    const server = new Connection("server", () => Promise.resolve(right));

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
    const client = new Connection("client", () => Promise.resolve(left));
    const server = new Connection("server", () => Promise.resolve(right));

    const serverReceived: RpcMessage[] = [];
    const clientReceived: RpcMessage[] = [];
    server.onPayload.subscribe((p) => serverReceived.push(p));
    client.onPayload.subscribe((p) => clientReceived.push(p));

    server.start();
    client.start();
    await settle(20); // 等握手完成

    // client -> server RPC
    client.sendRpc(
      requestMsg("abcdef01", 1, "audio.getAlgorithmConfig", {})
    );
    await settle(10);
    expect(serverReceived.length).toBe(1);
    expect(serverReceived[0].op).toBe(RpcOp.Request);
    expect(serverReceived[0].requestId).toBe(1);

    // server -> client RPC（反向）
    server.sendRpc(
      requestMsg("abcdef01", 2, "device.getInfo", {})
    );
    await settle(10);
    expect(clientReceived.length).toBe(1);
    expect(clientReceived[0].requestId).toBe(2);
  });

  it("close 触发双方 onClose", async () => {
    const { left, right } = createMockTransportPair(framedBinaryCapabilities());
    const client = new Connection("client", () => Promise.resolve(left));
    const server = new Connection("server", () => Promise.resolve(right));
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

  it("close(HeartbeatTimeout) 经 transport.terminate 强制断开（非 close 握手）", async () => {
    const { left } = createMockTransportPair(framedBinaryCapabilities());
    const client = new Connection("client", () => Promise.resolve(left));
    client.start();
    await settle(20);
    const terminateSpy = vi.spyOn(left, "terminate");
    client.close(CloseCode.HeartbeatTimeout, "heartbeat timeout");
    expect(terminateSpy).toHaveBeenCalledTimes(1);
  });

  it("close(Normal) 经 transport.close 优雅关闭，不调 terminate", async () => {
    const { left } = createMockTransportPair(framedBinaryCapabilities());
    const client = new Connection("client", () => Promise.resolve(left));
    client.start();
    await settle(20);
    const terminateSpy = vi.spyOn(left, "terminate");
    const closeSpy = vi.spyOn(left, "close");
    client.close(CloseCode.Normal, "local close");
    expect(closeSpy).toHaveBeenCalledTimes(1);
    expect(terminateSpy).not.toHaveBeenCalled();
  });
});

describe("Connection unframed-json: 直接 linkReady + 双向 RPC", () => {
  it("WS 模式连接即 linkReady（无 CONTROL）", async () => {
    const { left, right } = createMockTransportPair(unframedJsonCapabilities());
    const client = new Connection("client", () => Promise.resolve(left));
    const server = new Connection("server", () => Promise.resolve(right));

    let ready = false;
    client.onLinkReady.subscribe(() => (ready = true));
    server.start();
    client.start();
    await settle(10);
    expect(ready).toBe(true);
  });

  it("WS 双向 JSON RPC", async () => {
    const { left, right } = createMockTransportPair(unframedJsonCapabilities());
    const client = new Connection("client", () => Promise.resolve(left));
    const server = new Connection("server", () => Promise.resolve(right));

    const received: RpcMessage[] = [];
    server.onPayload.subscribe((p) => received.push(p));
    server.start();
    client.start();
    await settle(10);

    client.sendRpc(
      requestMsg("12345678", 99, "audio.getAlgorithmConfig", {})
    );
    await settle(10);
    expect(received.length).toBe(1);
    expect(received[0].requestId).toBe(99);
    expect(received[0]).toMatchObject({ method: "audio.getAlgorithmConfig" });
  });
});

describe("Connection factory 首次建立（异步 attach）", () => {
  it("start 后异步建立 transport，link ready 后可收发 RPC", async () => {
    const { left, right } = createMockTransportPair(unframedJsonCapabilities());
    const client = new Connection("client", () => Promise.resolve(left));
    const server = new Connection("server", () => Promise.resolve(right));

    const received: RpcMessage[] = [];
    server.onPayload.subscribe((p) => received.push(p));

    server.start();
    client.start();
    await settle(10); // factory 异步 attach + unframed 立即 linkReady

    client.sendRpc(requestMsg("", 1, "", {}));
    await settle(10);
    expect(received.length).toBe(1);
    expect(received[0].requestId).toBe(1);

    client.close();
    server.close();
  });
});

describe("Connection 心跳（framed）", () => {
  it("link ready 后启动心跳定时器", async () => {
    vi.useFakeTimers();
    const { left, right } = createMockTransportPair(framedBinaryCapabilities());
    const client = new Connection("client", () => Promise.resolve(left), {
      heartbeatIntervalMs: 1000,
      heartbeatTimeoutMs: 5000
    });
    const server = new Connection("server", () => Promise.resolve(right), {
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
      const conn = new Connection("server", () => Promise.resolve(t), {
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
    const clientConnection = new Connection("client", () => Promise.resolve(clientConn), {
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
    let factoryCalls = 0;
    const client = new Connection(
      "client",
      () => {
        factoryCalls += 1;
        return Promise.resolve(new MockTransport(unframedJsonCapabilities()));
      },
      { reconnect: { enabled: true, initialDelayMs: 5, jitter: false } }
    );
    client.start();
    await settle(10); // 首次 factory -> attach -> unframed 立即 linkReady -> onSuccess（active=false）

    let disconnectCount = 0;
    client.onDisconnect.subscribe(() => {
      disconnectCount += 1;
    });

    client.close(); // 本地主动关闭
    await settle(20);

    // 本地关闭不发 onDisconnect；close() 已 stop 协调器，transport.close() 同步触发 onClose 时
    // connState 已 closed，handleTransportClose 提前返回，不会在已 stop 的协调器上重新武装重连（“复活”）。
    // factory 仅被首次连接调用一次（close 未触发额外重连）。
    expect(disconnectCount).toBe(0);
    expect(factoryCalls).toBe(1);
    expect(client.isClosed).toBe(true);
  });

  it("重连交还的新 transport 在 link-ready 前断开 -> 继续重连至 maxAttempts，不永久卡死", async () => {
    vi.useFakeTimers();
    try {
      const { left, right } = createMockTransportPair(framedBinaryCapabilities());
      const server = new Connection("server", () => Promise.resolve(right));
      server.start();

      let factoryCalls = 0;
      const client = new Connection(
        "client",
        () => {
          factoryCalls += 1;
          if (factoryCalls === 1) {
            // 首次连接：正常 transport（与 right 对接，framed 握手可完成 -> onSuccess）
            return Promise.resolve(left);
          }
          // 后续重连：孤立 transport，attach 后立即关闭（link 永不会 ready）—— 原 bug 的卡死窗口
          const t = new MockTransport(framedBinaryCapabilities());
          setTimeout(() => t.close(), 0);
          return Promise.resolve(t);
        },
        { reconnect: { enabled: true, initialDelayMs: 1, maxAttempts: 5, jitter: false } }
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

describe("Connection sendRpc 状态守卫", () => {
  it("idle（未 start）状态 sendRpc 抛 TransportDisconnected", () => {
    const { left } = createMockTransportPair(unframedJsonCapabilities());
    const client = new Connection("client", () => Promise.resolve(left));
    expect(() => client.sendRpc(requestMsg("", 1, "", {}))).toThrow();
  });

  it("closed 后 sendRpc 静默丢弃（不抛错）", async () => {
    const { left, right } = createMockTransportPair(unframedJsonCapabilities());
    const client = new Connection("client", () => Promise.resolve(left));
    const server = new Connection("server", () => Promise.resolve(right));
    server.start();
    client.start();
    await settle(10);
    client.close();
    await settle(10);
    expect(client.isClosed).toBe(true);
    expect(() => client.sendRpc(requestMsg("", 1, "", {}))).not.toThrow();
  });
});
