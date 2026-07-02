// Node WebSocket transport：Unframed JSON（每条 WS message = 一个 AXTP JSON envelope）。
// WS 是 message 边界（非字节流），不能用 Duplex.toWeb——自定义 ReadableStream（每 message 一个 chunk）
// + WritableStream（每 chunk 发一条 text message），保持 envelope 边界给 Core 的 unframed wire adapter。
// 心跳：WS 原生 ping/pong（KeepaliveStreamTransport；spec 明确 WS 不走 CONTROL）。

import { WebSocket, WebSocketServer } from "ws";
import { bytesToText, type Bytes } from "../../io/bytes.js";
import { EventStream } from "../../types/events.js";
import { unframedJsonProfile } from "../profile.js";
import type {
  KeepaliveStreamTransport,
  StreamClientTransport,
  StreamServerTransport,
  StreamTransport
} from "../contract.js";

type WsMessageData = Buffer | ArrayBuffer | Buffer[];

function wsMessageToBytes(data: WsMessageData): Bytes {
  if (Array.isArray(data)) {
    const total = data.reduce((s, b) => s + b.length, 0);
    const out = new Uint8Array(total);
    let off = 0;
    for (const b of data) {
      out.set(b, off);
      off += b.length;
    }
    return out;
  }
  if (data instanceof ArrayBuffer) return new Uint8Array(data);
  // 单 Buffer：拷贝（Node Buffer 共享池，避免别名被复用改写）
  return new Uint8Array(data);
}

/** 把已建立的 ws 包成 KeepaliveStreamTransport。 */
function wsToTransport(ws: WebSocket): KeepaliveStreamTransport {
  const readable = new ReadableStream<Bytes>({
    start(controller) {
      ws.on("message", (data: WsMessageData, isBinary: boolean) => {
        if (isBinary) return; // WS-JSON profile 仅承载文本 JSON，拒绝二进制帧
        controller.enqueue(wsMessageToBytes(data));
      });
      ws.on("close", () => {
        try {
          controller.close();
        } catch {
          /* 已关闭 */
        }
      });
      ws.on("error", (err: Error) => {
        try {
          controller.error(err);
        } catch {
          /* 已关闭 */
        }
      });
    }
  });
  const writable = new WritableStream<Bytes>({
    write(chunk) {
      ws.send(bytesToText(chunk)); // text 帧（opcode 0x1）
    }
  });
  return {
    profile: unframedJsonProfile(),
    readable,
    writable,
    sendKeepalive: () => {
      try {
        ws.ping();
      } catch {
        /* 已关闭 */
      }
    },
    onKeepaliveAck: (listener) => {
      const h = (): void => listener();
      ws.on("pong", h);
      return () => ws.off("pong", h);
    },
    close: () => {
      try {
        ws.close(1000, "local close");
      } catch {
        try {
          ws.terminate();
        } catch {
          /* best-effort */
        }
      }
    },
    terminate: () => {
      try {
        ws.terminate();
      } catch {
        /* best-effort */
      }
    }
  };
}

export interface WsClientOptions {
  url: string;
  protocols?: string | string[];
  headers?: Record<string, string>;
}

/** WS client（stream）：connect → KeepaliveStreamTransport。可复用（每次 connect 新建连接，供 AxtpClient 重连）。 */
export class NodeWsClientTransport implements StreamClientTransport {
  readonly profile = unframedJsonProfile();

  constructor(private readonly options: WsClientOptions) {}

  connect(): Promise<KeepaliveStreamTransport> {
    return new Promise((resolve, reject) => {
      const ws = new WebSocket(this.options.url, this.options.protocols, {
        headers: this.options.headers
      });
      ws.once("open", () => resolve(wsToTransport(ws)));
      ws.once("error", (err) => reject(err));
    });
  }
}

export interface WsServerOptions {
  port: number;
  host?: string;
}

/** WS server（stream）：每个接受的 ws → KeepaliveStreamTransport。 */
export class NodeWsServerTransport implements StreamServerTransport {
  readonly profile = unframedJsonProfile();
  readonly onConnection = new EventStream<StreamTransport>();
  private wss: WebSocketServer | undefined;
  private listening = false;

  constructor(private readonly options: WsServerOptions) {}

  listen(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.wss = new WebSocketServer({ port: this.options.port, host: this.options.host });
      this.wss.on("error", (err) => {
        if (!this.listening) reject(err);
      });
      this.wss.on("connection", (ws) => this.onConnection.emit(wsToTransport(ws)));
      this.wss.on("listening", () => {
        this.listening = true;
        resolve();
      });
    });
  }

  get boundPort(): number | undefined {
    const addr = this.wss?.address();
    return typeof addr === "object" && addr !== null ? addr.port : undefined;
  }

  async close(): Promise<void> {
    this.listening = false;
    const wss = this.wss;
    if (wss !== undefined) {
      await new Promise<void>((resolve) => wss.close(() => resolve()));
    }
    this.onConnection.close();
  }
}
