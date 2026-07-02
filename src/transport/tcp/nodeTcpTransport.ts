// Node TCP transport：Standard Framed Binary，经 Web Streams 暴露（Core/Endpoint 消费）。
// 用 Duplex.toWeb(socket) 把 net.Socket 转成 { readable, writable }。transport 只搬运原始字节，
// 帧的 resync/CRC 由 Core 的 wire adapter 负责。server 每个接受的 socket 产出一个 StreamTransport。

import net from "node:net";
import { Duplex } from "node:stream";
import type { Bytes } from "../../io/bytes.js";
import { EventStream } from "../../types/events.js";
import { framedBinaryProfile } from "../profile.js";
import type { StreamClientTransport, StreamServerTransport, StreamTransport } from "../contract.js";

export interface TcpOptions {
  host?: string;
  port: number;
}

/** 把已建立的 net.Socket 包成 StreamTransport。 */
function socketToTransport(socket: net.Socket): StreamTransport {
  const { readable, writable } = Duplex.toWeb(socket) as unknown as {
    readable: ReadableStream<Bytes>;
    writable: WritableStream<Bytes>;
  };
  return {
    profile: framedBinaryProfile("AXTP-TCP"),
    readable,
    writable,
    close: () => socket.destroy(),
    terminate: () => socket.destroy()
  };
}

/** TCP server（stream）：每个接受的 socket → StreamTransport。 */
export class NodeTcpServerTransport implements StreamServerTransport {
  readonly profile = framedBinaryProfile("AXTP-TCP");
  readonly onConnection = new EventStream<StreamTransport>();
  private server: net.Server | undefined;
  private listening = false;

  constructor(private readonly options: TcpOptions) {}

  /** 监听端口（port=0 → 随机端口，listen 后读 port）。 */
  listen(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.server = net.createServer((socket) => this.onConnection.emit(socketToTransport(socket)));
      this.server.on("error", (err) => {
        if (!this.listening) reject(err);
      });
      this.server.listen(this.options.port, this.options.host ?? "127.0.0.1", () => {
        this.listening = true;
        resolve();
      });
    });
  }

  /** 实际监听端口（port=0 时用）。listen 前为 undefined。 */
  get boundPort(): number | undefined {
    const addr = this.server?.address();
    return typeof addr === "object" && addr !== null ? addr.port : undefined;
  }

  async close(): Promise<void> {
    this.listening = false;
    const server = this.server;
    if (server !== undefined) {
      await new Promise<void>((resolve) => server.close(() => resolve()));
    }
    this.onConnection.close();
  }
}

/** TCP client（stream）：connect → StreamTransport。可复用（每次 connect 新建连接，供 AxtpClient 重连）。 */
export class NodeTcpClientTransport implements StreamClientTransport {
  readonly profile = framedBinaryProfile("AXTP-TCP");

  constructor(private readonly options: TcpOptions) {}

  connect(): Promise<StreamTransport> {
    return new Promise((resolve, reject) => {
      const socket = net.createConnection(
        { host: this.options.host ?? "127.0.0.1", port: this.options.port },
        () => resolve(socketToTransport(socket))
      );
      socket.once("error", (err) => reject(err));
    });
  }
}
