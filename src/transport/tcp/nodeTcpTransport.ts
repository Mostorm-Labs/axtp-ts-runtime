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

interface TcpOptions {
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
      const bytes = new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
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

  isConnected(): boolean {
    return this.connected;
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
        if (!this.listening) reject(err);
      });
      this.server.listen(this.options.port, this.options.host, () => {
        this.listening = true;
        resolve();
      });
    });
  }

  isListening(): boolean {
    return this.listening;
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

  isAvailable(): boolean {
    return this.available;
  }
}
