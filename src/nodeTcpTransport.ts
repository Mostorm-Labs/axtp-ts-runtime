import net from "node:net";
import { type Bytes } from "./bytes.js";
import type { ByteSink } from "./io.js";
import { rpcEncodingJsonBinary } from "./rpcEncoding.js";
import { AxtpWireMode, TransportKind, type ITransport, type TransportProfile } from "./transport.js";

export interface NodeTcpClientTransportOptions {
  host: string;
  port: number;
}

export interface NodeTcpServerTransportOptions {
  host?: string;
  port?: number;
}

function tcpProfile(): TransportProfile {
  return {
    kind: TransportKind.Tcp,
    wireMode: AxtpWireMode.FramedBinary,
    defaultRpcEncoding: rpcEncodingJsonBinary,
    messageOriented: false,
    supportsTextMessage: false,
    supportsBinaryMessage: true,
    preferredFrameSize: 4096
  };
}

function toBytes(buffer: Buffer): Bytes {
  return new Uint8Array(buffer.buffer, buffer.byteOffset, buffer.byteLength).slice();
}

function writeSocket(socket: net.Socket | undefined, bytes: Bytes): Promise<void> {
  if (socket === undefined || socket.destroyed) return Promise.resolve();
  return new Promise((resolve, reject) => {
    const done = (error?: Error | null): void => {
      if (error) reject(error);
      else resolve();
    };
    if (socket.write(Buffer.from(bytes), done)) {
      return;
    }
    socket.once("drain", () => resolve());
    socket.once("error", reject);
  });
}

export class NodeTcpClientTransport implements ITransport {
  private sink: ByteSink | undefined;
  private socket: net.Socket | undefined;

  constructor(private readonly options: NodeTcpClientTransportOptions) {}

  bind(sink: ByteSink): void {
    this.sink = sink;
  }

  open(): Promise<void> {
    void this.close();
    return new Promise((resolve, reject) => {
      const socket = net.createConnection({
        host: this.options.host,
        port: this.options.port
      });
      this.socket = socket;
      socket.setNoDelay(true);
      socket.on("data", (chunk) => this.sink?.onBytes(toBytes(chunk)));
      socket.once("connect", () => resolve());
      socket.once("error", reject);
      socket.once("close", () => {
        if (this.socket === socket) this.socket = undefined;
      });
    });
  }

  close(): Promise<void> {
    const socket = this.socket;
    this.socket = undefined;
    if (socket === undefined || socket.destroyed) return Promise.resolve();
    return new Promise((resolve) => {
      socket.once("close", () => resolve());
      socket.destroy();
    });
  }

  sendBytes(bytes: Bytes): Promise<void> {
    return writeSocket(this.socket, bytes);
  }

  profile(): TransportProfile {
    return tcpProfile();
  }

  isOpen(): boolean {
    return this.socket !== undefined && !this.socket.destroyed;
  }

  localPort(): number {
    return this.socket?.localPort ?? 0;
  }
}

export class NodeTcpServerTransport implements ITransport {
  private sink: ByteSink | undefined;
  private server: net.Server | undefined;
  private activeSocket: net.Socket | undefined;
  private readonly host: string;
  private readonly port: number;

  constructor(options: NodeTcpServerTransportOptions = {}) {
    this.host = options.host ?? "127.0.0.1";
    this.port = options.port ?? 0;
  }

  bind(sink: ByteSink): void {
    this.sink = sink;
  }

  open(): Promise<void> {
    void this.close();
    this.server = net.createServer((socket) => {
      if (this.activeSocket !== undefined && !this.activeSocket.destroyed) {
        socket.destroy();
        return;
      }
      this.activeSocket = socket;
      socket.setNoDelay(true);
      socket.on("data", (chunk) => this.sink?.onBytes(toBytes(chunk)));
      socket.once("close", () => {
        if (this.activeSocket === socket) this.activeSocket = undefined;
      });
    });
    return new Promise((resolve, reject) => {
      this.server?.once("error", reject);
      this.server?.listen(this.port, this.host, () => resolve());
    });
  }

  close(): Promise<void> {
    const server = this.server;
    const socket = this.activeSocket;
    this.server = undefined;
    this.activeSocket = undefined;
    socket?.destroy();
    if (server === undefined || !server.listening) return Promise.resolve();
    return new Promise((resolve, reject) => {
      server.close((error) => {
        if (error) reject(error);
        else resolve();
      });
    });
  }

  sendBytes(bytes: Bytes): Promise<void> {
    return writeSocket(this.activeSocket, bytes);
  }

  profile(): TransportProfile {
    return tcpProfile();
  }

  localPort(): number {
    const address = this.server?.address();
    return typeof address === "object" && address !== null ? address.port : 0;
  }

  hasConnection(): boolean {
    return this.activeSocket !== undefined && !this.activeSocket.destroyed;
  }

  isOpen(): boolean {
    return this.server?.listening ?? false;
  }
}
