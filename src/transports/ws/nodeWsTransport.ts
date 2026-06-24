import { WebSocket, WebSocketServer, type RawData } from 'ws';
import { RpcEncoding } from '../../core/protocol/generated/axtp_ids_generated.js';
import { AxtpWireMode, TransportKind, type ITransport, type TransportProfile } from '../../core/runtime/transport/transport.js';
import { toBytes, type Bytes } from '../../core/support/io/bytes.js';
import type { ByteSink } from '../../core/support/io/io.js';

export interface NodeWsClientTransportOptions {
  /** WebSocket URL, e.g. "ws://host:port/path". */
  url: string;
}

export interface NodeWsServerTransportOptions {
  host?: string;
  port?: number;
  path?: string;
}

// One WebSocket text message carries one AXTP JSON-RPC envelope (messageOriented).
// Mirrors the C++ websocket transports: text frames only, no length-prefix framing.
function wsProfile(): TransportProfile {
  return {
    kind: TransportKind.WebSocket,
    wireMode: AxtpWireMode.WebSocketJsonRpc,
    defaultRpcEncoding: RpcEncoding.Json,
    messageOriented: true,
    supportsTextMessage: true,
    supportsBinaryMessage: false,
    preferredFrameSize: 4096,
  };
}

function rawDataToBytes(data: RawData): Bytes {
  if (Array.isArray(data)) return toBytes(Buffer.concat(data));
  if (data instanceof ArrayBuffer) return toBytes(new Uint8Array(data));
  return toBytes(data as Buffer);
}

function sendText(socket: WebSocket | undefined, bytes: Bytes): Promise<void> {
  if (socket === undefined || socket.readyState !== WebSocket.OPEN) return Promise.resolve();
  return new Promise((resolve, reject) => {
    socket.send(Buffer.from(bytes), { binary: false }, (error) => {
      if (error) reject(error);
      else resolve();
    });
  });
}

export class NodeWsClientTransport implements ITransport {
  private sink: ByteSink | undefined;
  private socket: WebSocket | undefined;

  constructor(private readonly options: NodeWsClientTransportOptions) {}

  bind(sink: ByteSink): void {
    this.sink = sink;
  }

  open(): Promise<void> {
    void this.close();
    return new Promise((resolve, reject) => {
      const socket = new WebSocket(this.options.url);
      this.socket = socket;
      socket.on('message', (data, isBinary) => {
        if (isBinary) return; // text-only JSON-RPC transport; ignore binary frames
        this.sink?.onBytes(rawDataToBytes(data));
      });
      socket.once('open', () => resolve());
      socket.once('error', reject);
      socket.once('close', () => {
        if (this.socket === socket) this.socket = undefined;
      });
    });
  }

  close(): Promise<void> {
    const socket = this.socket;
    this.socket = undefined;
    if (socket === undefined || socket.readyState === WebSocket.CLOSED) return Promise.resolve();
    return new Promise((resolve) => {
      socket.once('close', () => resolve());
      socket.close();
    });
  }

  sendBytes(bytes: Bytes): Promise<void> {
    return sendText(this.socket, bytes);
  }

  profile(): TransportProfile {
    return wsProfile();
  }

  isOpen(): boolean {
    return this.socket !== undefined && this.socket.readyState === WebSocket.OPEN;
  }
}

export class NodeWsServerTransport implements ITransport {
  private sink: ByteSink | undefined;
  private server: WebSocketServer | undefined;
  private activeSocket: WebSocket | undefined;
  private listening = false;
  private readonly host: string;
  private readonly port: number;
  private readonly path: string | undefined;

  constructor(options: NodeWsServerTransportOptions = {}) {
    this.host = options.host ?? '127.0.0.1';
    this.port = options.port ?? 0;
    this.path = options.path;
  }

  bind(sink: ByteSink): void {
    this.sink = sink;
  }

  open(): Promise<void> {
    void this.close();
    this.server = new WebSocketServer({ host: this.host, port: this.port, path: this.path });
    this.server.on('connection', (socket) => {
      if (this.activeSocket !== undefined && this.activeSocket.readyState === WebSocket.OPEN) {
        socket.close();
        return;
      }
      this.activeSocket = socket;
      socket.on('message', (data, isBinary) => {
        if (isBinary) return; // text-only JSON-RPC transport; ignore binary frames
        this.sink?.onBytes(rawDataToBytes(data));
      });
      socket.once('close', () => {
        if (this.activeSocket === socket) this.activeSocket = undefined;
      });
    });
    return new Promise((resolve, reject) => {
      this.server?.once('error', reject);
      this.server?.once('listening', () => {
        this.listening = true;
        resolve();
      });
    });
  }

  close(): Promise<void> {
    const server = this.server;
    const socket = this.activeSocket;
    this.server = undefined;
    this.activeSocket = undefined;
    this.listening = false;
    if (socket !== undefined && socket.readyState === WebSocket.OPEN) socket.close();
    if (server === undefined) return Promise.resolve();
    return new Promise((resolve) => {
      server.close(() => resolve());
    });
  }

  sendBytes(bytes: Bytes): Promise<void> {
    return sendText(this.activeSocket, bytes);
  }

  profile(): TransportProfile {
    return wsProfile();
  }

  localPort(): number {
    const address = this.server?.address();
    return typeof address === 'object' && address !== null ? address.port : 0;
  }

  hasConnection(): boolean {
    return this.activeSocket !== undefined && this.activeSocket.readyState === WebSocket.OPEN;
  }

  // No-op pump hook for WebSocketJsonRpcAdapter.poll(transport): ws is event-driven,
  // unlike the C++ poll-based transports, so there is nothing to drive manually.
  poll(): void {}

  isOpen(): boolean {
    return this.listening;
  }
}
