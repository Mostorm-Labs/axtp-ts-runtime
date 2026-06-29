// MockTransport：测试用传输。成对连接（bridge），支持多连接，可控事件驱动。
// 提供一对：左端发 -> 右端 onMessage 收，反之亦然。用于 Connection/Session 端到端测试。

import type { Bytes } from "../../io/bytes.js";
import { AxtpError, ErrorCode } from "../../types/error.js";
import { EventStream } from "../../types/events.js";
import {
  CloseCode,
  framedBinaryCapabilities,
  type CloseReason,
  type IClientTransport,
  type IServerTransport,
  type ITransport,
  type TransportCapabilities
} from "../transport.js";

/** 创建一对互连的 MockTransport：左右端双向互通。 */
export function createMockTransportPair(
  capabilities: TransportCapabilities = framedBinaryCapabilities()
): { left: MockTransport; right: MockTransport } {
  const left = new MockTransport(capabilities);
  const right = new MockTransport(capabilities);
  left.linkPeer(right);
  right.linkPeer(left);
  return { left, right };
}

/** 把 server/client 用 mock 对接：返回 client transport，server 自动接受。 */
export function bridgeMockServer(server: MockServerTransport): MockTransport {
  const capabilities = server.capabilities;
  const client = new MockTransport(capabilities);
  server.accept(client);
  return client;
}

/** 单条 mock 连接。send 直接写入 peer 的 onMessage。 */
export class MockTransport implements ITransport {
  readonly onMessage = new EventStream<Bytes>();
  readonly onClose = new EventStream<CloseReason>();
  readonly onError = new EventStream<AxtpError>();
  peer: MockTransport | undefined;
  private connected = true;
  /** 暂停投递（测试可控时序），缓冲待投递字节。 */
  private paused = false;
  private pending: Bytes[] = [];
  /** peer 尚未建立时，send 的出站缓冲（accept 设 peer 后 flush）。
   *  使 mock 对"谁先发"无关——双向握手（经典/Cloud Reverse）都能走通。 */
  private outboundPending: Bytes[] = [];

  constructor(readonly capabilities: TransportCapabilities) {}

  send(bytes: Bytes): void {
    if (!this.connected) return;
    if (this.peer === undefined) {
      // peer 尚未建立（如 client 先于 server accept 发送）：缓冲，等 linkPeer flush。
      this.outboundPending.push(bytes);
      return;
    }
    this.peer.deliver(bytes);
  }

  /** 建立与 peer 的双向连接，并 flush 双方在连接前缓冲的出站字节。 */
  linkPeer(peer: MockTransport): void {
    this.peer = peer;
    const buffered = this.outboundPending.splice(0);
    for (const bytes of buffered) peer.deliver(bytes);
  }

  /** 内部投递：异步（模拟网络延迟，避免同步 emit 竞态）。受 paused 控制。 */
  deliver(bytes: Bytes): void {
    if (!this.connected) return;
    if (this.paused) {
      this.pending.push(bytes);
      return;
    }
    // 异步投递：真实传输有延迟，且避免 sender 在 send 调用栈内同步触发 receiver
    // （否则 receiver 在未完成订阅注册前可能收到消息）。
    const snapshot = bytes.slice();
    queueMicrotask(() => {
      if (!this.connected) return;
      this.onMessage.emit(snapshot);
    });
  }

  /** 暂停向本端投递字节（缓冲）。 */
  pause(): void {
    this.paused = true;
  }

  /** 恢复投递，异步 flush 缓冲（与 deliver 的 queueMicrotask 语义一致）。 */
  resume(): void {
    this.paused = false;
    const buffered = this.pending.splice(0);
    for (const bytes of buffered) this.deliver(bytes);
  }

  close(code: CloseCode = CloseCode.Normal, reason = "closed", remote = false): void {
    if (!this.connected) return;
    this.connected = false;
    this.onClose.emit({ code, reason, remote });
    if (this.peer !== undefined && this.peer.connected) {
      // 异步传播：模拟真实网络（对端感知断连是异步的），避免同步递归。
      const peer = this.peer;
      queueMicrotask(() => peer.close(code, reason, true));
    }
    this.onMessage.close();
    this.onError.close();
  }

}

/** Mock server：可手动 accept 多个连接，每连接产出 MockTransport。 */
export class MockServerTransport implements IServerTransport {
  readonly onConnection = new EventStream<ITransport>();
  readonly onClose = new EventStream<void>();
  private listening = false;
  private accepted: MockTransport[] = [];

  constructor(readonly capabilities: TransportCapabilities = framedBinaryCapabilities()) {}

  listen(): Promise<void> {
    this.listening = true;
    return Promise.resolve();
  }

  /** 测试主动注入一个新 client 连接。 */
  accept(client: MockTransport): void {
    if (!this.listening) return;
    const serverSide = new MockTransport(this.capabilities);
    // 双向 linkPeer：flush 双方在连接前缓冲的出站字节（支持 client 先发 Hello 的 Cloud Reverse）。
    serverSide.linkPeer(client);
    client.linkPeer(serverSide);
    this.accepted.push(serverSide);
    this.onConnection.emit(serverSide);
  }


  close(): Promise<void> {
    this.listening = false;
    for (const t of this.accepted) t.close();
    this.accepted.length = 0;
    this.onClose.emit(undefined);
    this.onConnection.close();
    return Promise.resolve();
  }
}

/** Mock client：connect() 返回一个新 MockTransport，配合 MockServerTransport 使用。 */
export class MockClientTransport implements IClientTransport {
  readonly onClose = new EventStream<void>();
  private available = true;
  constructor(
    readonly capabilities: TransportCapabilities,
    private readonly server: MockServerTransport
  ) {}

  connect(): Promise<ITransport> {
    if (!this.available) return Promise.reject(new AxtpError(ErrorCode.Unavailable, "transport unavailable"));
    const client = new MockTransport(this.capabilities);
    // 级联：client transport 断开时通知 MockClientTransport.onClose（#6b 修复）
    client.onClose.subscribe(() => this.onClose.emit(undefined));
    // 异步 accept：让调用方（establishSession）先建好 client Connection 再触发 server 发 Hello，
    // 避免 Hello 在 client Connection 订阅前到达而丢失。
    setTimeout(() => {
      if (this.available) this.server.accept(client);
    }, 0);
    return Promise.resolve(client);
  }


  close(): void {
    this.available = false;
    this.onClose.emit(undefined);
  }
}
