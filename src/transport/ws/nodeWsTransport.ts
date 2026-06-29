// Node WebSocket transport：Unframed JSON（message 边界，仅 RPC，无 CONTROL/STREAM）。
// 每个 WS message 即一个 AXTP JSON envelope {sid,op,d}。
// 多连接：每个 ws 连接产出一个 ITransport 经 onConnection 上报。
//
// 心跳：WS 用原生 ping/pong（spec 明确 WS 不走 CONTROL），
// 通过 ITransport.sendKeepalive/onKeepaliveAck 暴露（capabilities.supportsKeepalive=true）。

import { WebSocket, WebSocketServer } from "ws";
import { bytesToText, type Bytes } from "../../io/bytes.js";
import { AxtpError, ErrorCode } from "../../types/error.js";
import { EventStream } from "../../types/events.js";
import {
  CloseCode,
  unframedJsonCapabilities,
  type CloseReason,
  type IClientTransport,
  type IServerTransport,
  type ITransport
} from "../transport.js";

interface WsClientOptions {
  url: string;
  /** 协议子列表（可选）。 */
  protocols?: string | string[];
  /** 额外 headers（可选）。 */
  headers?: Record<string, string>;
}

/** 一条已建立的 WS 连接。 */
class WsTransport implements ITransport {
  readonly onMessage = new EventStream<Bytes>();
  readonly onClose = new EventStream<CloseReason>();
  readonly onError = new EventStream<AxtpError>();
  readonly capabilities = unframedJsonCapabilities();
  private connected = true;
  /** attach 前的消息缓冲（防止 ws message 在 Connection 订阅前到达丢失）。 */
  private attached = false;
  private readonly buffered: Bytes[] = [];

  constructor(private readonly ws: WebSocket) {
    ws.on("message", (data: Buffer | ArrayBuffer | Buffer[], isBinary: boolean) => {
      // WS-JSON profile 仅承载文本 JSON，拒绝二进制帧（spec: WS MUST NOT 承载 STREAM/CRC/Frame Header）。
      if (isBinary) return;
      let bytes: Bytes;
      if (Array.isArray(data)) {
        const total = data.reduce((s, b) => s + b.length, 0);
        bytes = new Uint8Array(total);
        let off = 0;
        for (const b of data) {
          bytes.set(b, off);
          off += b.length;
        }
      } else if (data instanceof ArrayBuffer) {
        bytes = new Uint8Array(data);
      } else {
        // 单 Buffer 文本帧：拷贝而非别名（Node Buffer 共享池 ArrayBuffer，attach 前缓冲
        // 到 buffered[] 期间内存可能被复用，导致后续解码的 JSON envelope 被改写）。
        bytes = new Uint8Array(data);
      }
      if (!this.attached) {
        this.buffered.push(bytes);
        return;
      }
      this.onMessage.emit(bytes);
    });
    ws.on("error", (err: Error) => {
      this.onError.emit(new AxtpError(ErrorCode.TransportDisconnected, err.message, err));
    });
    ws.on("close", (code: number, reason: Buffer) => {
      if (!this.connected) return;
      this.connected = false;
      this.onClose.emit({
        code: CloseCode.Normal,
        reason: reason.toString() || `ws closed ${code}`,
        remote: true
      });
    });
  }

  send(bytes: Bytes): void {
    if (!this.connected) return;
    // WS-JSON profile 是文本 JSON：必须以文本帧（opcode 0x1）发送，而非二进制帧。
    this.ws.send(bytesToText(bytes), { binary: false });
  }

  /** 保活探测：WS 映射到 ws.ping()。 */
  sendKeepalive(): void {
    if (this.connected) this.ws.ping();
  }

  /** 保活确认：WS 映射到 ws pong 事件。 */
  onKeepaliveAck(listener: () => void): () => void {
    const handler = (): void => listener();
    this.ws.on("pong", handler);
    return () => {
      this.ws.off("pong", handler);
    };
  }

  close(): void {
    if (!this.connected) return;
    this.connected = false;
    try {
      this.ws.close(1000, "local close");
    } catch {
      this.ws.terminate();
    }
    this.onClose.emit({ code: CloseCode.Normal, reason: "local close", remote: false });
    this.onMessage.close();
    this.onError.close();
  }


  /** Connection 接管：停止缓冲，flush 已缓冲消息到 onMessage。 */
  attach(): void {
    if (this.attached) return;
    this.attached = true;
    const buffered = this.buffered.splice(0);
    for (const bytes of buffered) this.onMessage.emit(bytes);
  }
}

interface WsServerOptions {
  port: number;
  host?: string;
}

/** WS server：多连接。 */
export class NodeWsServerTransport implements IServerTransport {
  readonly onConnection = new EventStream<ITransport>();
  readonly onClose = new EventStream<void>();
  readonly onError = new EventStream<AxtpError>();
  readonly capabilities = unframedJsonCapabilities();
  private wss: WebSocketServer | undefined;
  private listening = false;

  constructor(private readonly options: WsServerOptions) {}

  listen(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.wss = new WebSocketServer({
        port: this.options.port,
        host: this.options.host
      });
      this.wss.on("error", (err) => {
        if (!this.listening) {
          reject(err);
          return;
        }
        // listen 成功后的 server 错误：reject 对已 resolve 的 promise 是 no-op，经 onError 显式上抛。
        this.onError.emit(new AxtpError(ErrorCode.TransportDisconnected, err.message, err));
      });
      this.wss.on("connection", (ws: WebSocket) => {
        this.onConnection.emit(new WsTransport(ws));
      });
      this.wss.on("listening", () => {
        this.listening = true;
        resolve();
      });
    });
  }


  async close(): Promise<void> {
    this.listening = false;
    if (this.wss === undefined) return;
    const wss = this.wss;
    await new Promise<void>((resolve) => {
      wss.close(() => resolve());
    });
    this.onClose.emit(undefined);
    this.onConnection.close();
  }
}

/** WS client：发起单连接。 */
export class NodeWsClientTransport implements IClientTransport {
  readonly onClose = new EventStream<void>();
  readonly capabilities = unframedJsonCapabilities();
  private available = true;

  constructor(private readonly options: WsClientOptions) {}

  connect(): Promise<ITransport> {
    if (!this.available)
      return Promise.reject(new AxtpError(ErrorCode.Unavailable, "transport unavailable"));
    return new Promise((resolve, reject) => {
      let settled = false; // B3: resolve/reject 互斥
      const ws = new WebSocket(this.options.url, this.options.protocols, {
        headers: this.options.headers
      });
      ws.once("open", () => {
        if (settled) return;
        settled = true;
        const transport = new WsTransport(ws);
        transport.onClose.subscribe(() => this.onClose.emit(undefined));
        resolve(transport);
      });
      ws.once("error", (err) => {
        if (settled) return;
        settled = true;
        reject(err);
      });
    });
  }

}
