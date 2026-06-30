// Node TCP transport：Standard Framed Binary（字节流，非 message 边界）。
// frame 的 resync/CRC 由 protocol codec 层负责，transport 只搬运原始字节。
// server 支持多连接：每个 socket 产出一个 ITransport 经 onConnection 上报。

import net from "node:net";
import type { Bytes } from "../../io/bytes.js";
import { AxtpError, ErrorCode } from "../../types/error.js";
import { EventStream } from "../../types/events.js";
import {
  CloseCode,
  framedBinaryCapabilities,
  type CloseReason,
  type IClientTransport,
  type IServerTransport,
  type ITransport
} from "../transport.js";

export interface TcpOptions {
  host?: string;
  port: number;
}

/** 一条已建立的 TCP 连接（client connect 产物 / server accept 产物）。 */
class TcpTransport implements ITransport {
  readonly onMessage = new EventStream<Bytes>();
  readonly onClose = new EventStream<CloseReason>();
  readonly onError = new EventStream<AxtpError>();
  readonly capabilities = framedBinaryCapabilities();
  private connected = true;
  /** attach 前的消息缓冲（防止 socket data 在 Connection 订阅前到达丢失）。 */
  private attached = false;
  private readonly buffered: Bytes[] = [];

  constructor(private readonly socket: net.Socket) {
    socket.on("data", (data: Buffer) => {
      // 拷贝而非别名：Node 的 Buffer 共享内部池 ArrayBuffer，部分帧会跨多次 'data' 事件
      // 在 CodecPipeline 中缓冲拼接；若零拷贝建视图，下一次读会覆盖池内存，重组帧被静默损坏。
      const bytes = new Uint8Array(data);
      if (!this.attached) {
        this.buffered.push(bytes);
        return;
      }
      this.onMessage.emit(bytes);
    });
    socket.on("error", (err: Error) => {
      this.onError.emit(new AxtpError(ErrorCode.TransportDisconnected, err.message, err));
    });
    socket.on("close", (hadError: boolean) => {
      if (!this.connected) return;
      this.connected = false;
      this.onClose.emit({
        code: hadError ? CloseCode.TransportError : CloseCode.Normal,
        reason: hadError ? "socket error" : "socket closed",
        remote: true
      });
    });
  }

  send(bytes: Bytes): void {
    if (!this.connected) return;
    this.socket.write(bytes);
  }

  close(): void {
    if (!this.connected) return;
    this.connected = false;
    this.socket.destroy();
    this.onClose.emit({ code: CloseCode.Normal, reason: "local close", remote: false });
    this.onMessage.close();
    this.onError.close();
  }

  /**
   * 强制立即断开。TCP transport 的 close() 已用 socket.destroy()（立即销毁、无优雅握手），
   * 故 terminate 与 close 行为一致；显式实现以满足 ITransport 契约，表达"强制"语义。
   */
  terminate(): void {
    this.close();
  }


  /** Connection 接管：停止缓冲，flush 已缓冲消息到 onMessage。 */
  attach(): void {
    if (this.attached) return;
    this.attached = true;
    const buffered = this.buffered.splice(0);
    for (const bytes of buffered) this.onMessage.emit(bytes);
  }
}

/** TCP server：多连接。 */
export class NodeTcpServerTransport implements IServerTransport {
  readonly onConnection = new EventStream<ITransport>();
  readonly onClose = new EventStream<void>();
  readonly onError = new EventStream<AxtpError>();
  readonly capabilities = framedBinaryCapabilities();
  private server: net.Server | undefined;
  private listening = false;

  constructor(private readonly options: TcpOptions) {}

  listen(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.server = net.createServer((socket) => {
        this.onConnection.emit(new TcpTransport(socket));
      });
      this.server.on("error", (err) => {
        if (!this.listening) {
          // pre-listen 错误（如 EADDRINUSE）经 reject 上抛
          reject(err);
          return;
        }
        // listen 成功后的 server 错误（accept 期 ECONNRESET/EMFILE 等）：reject 对已 resolve
        // 的 promise 是 no-op，必须经 onError 显式上抛，否则被静默吞掉、server 静默停止接受连接。
        this.onError.emit(new AxtpError(ErrorCode.TransportDisconnected, err.message, err));
      });
      this.server.listen(this.options.port, this.options.host, () => {
        this.listening = true;
        resolve();
      });
    });
  }


  async close(): Promise<void> {
    this.listening = false;
    if (this.server === undefined) return;
    const server = this.server;
    await new Promise<void>((resolve) => {
      server.close(() => resolve());
    });
    this.onClose.emit(undefined);
    this.onConnection.close();
  }
}

/** TCP client：发起单连接。 */
export class NodeTcpClientTransport implements IClientTransport {
  readonly onClose = new EventStream<void>();
  readonly capabilities = framedBinaryCapabilities();
  private available = true;

  constructor(private readonly options: TcpOptions) {}

  connect(): Promise<ITransport> {
    if (!this.available) return Promise.reject(new AxtpError(ErrorCode.Unavailable, "transport unavailable"));
    return new Promise((resolve, reject) => {
      let settled = false; // B3: resolve/reject 互斥
      const socket = net.createConnection(
        { host: this.options.host ?? "127.0.0.1", port: this.options.port },
        () => {
          if (settled) return;
          settled = true;
          const transport = new TcpTransport(socket);
          transport.onClose.subscribe(() => this.onClose.emit(undefined));
          resolve(transport);
        }
      );
      socket.once("error", (err) => {
        if (settled) return;
        settled = true;
        reject(err);
      });
    });
  }

}
