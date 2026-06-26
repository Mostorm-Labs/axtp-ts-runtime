// Node WebSocket transport：Unframed JSON（message 边界，仅 RPC，无 CONTROL/STREAM）。
// 每个 WS message 即一个 AXTP JSON envelope {sid,op,d}。
// 多连接：每个 ws 连接产出一个 ITransport 经 onConnection 上报。
//
// 心跳：WS 用原生 ping/pong（spec 明确 WS 不走 CONTROL）。
// 为保持 ITransport 接口纯净，WS transport 额外实现可选能力探测接口 NativePingCapable，
// Connection 用鸭子类型（hasNativePing）取用——不污染 ITransport，也不让 Connection 硬依赖 ws 类型。

import { WebSocket, WebSocketServer } from "ws";
import type { Bytes } from "../../io/bytes.js";
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

/** 可选能力探测：WS transport 额外提供原生 ping/pong，Connection 用它做心跳。 */
export interface NativePingCapable {
  /** 发送原生 ping 帧。 */
  ping(): void;
  /** 订阅原生 pong 到达。 */
  onPong(listener: () => void): () => void;
}

/** 鸭子类型判断 transport 是否提供原生 ping 能力。 */
export function hasNativePing(t: ITransport): t is ITransport & NativePingCapable {
  return typeof (t as Partial<NativePingCapable>).ping === "function";
}

interface WsClientOptions {
  url: string;
  /** 协议子列表（可选）。 */
  protocols?: string | string[];
  /** 额外 headers（可选）。 */
  headers?: Record<string, string>;
}

/** 一条已建立的 WS 连接。 */
class WsTransport implements ITransport, NativePingCapable {
  readonly onMessage = new EventStream<Bytes>();
  readonly onClose = new EventStream<CloseReason>();
  readonly onError = new EventStream<AxtpError>();
  readonly capabilities = unframedJsonCapabilities();
  private connected = true;
  /** WS-JSON 以文本帧传输 JSON，发送前需 bytes→string。 */
  private readonly textDecoder = new TextDecoder();
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
        bytes = new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
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
    // bytes 是 UTF-8 编码的 JSON 文本，解码成 string 后以文本帧发送。
    this.ws.send(this.textDecoder.decode(bytes), { binary: false });
  }

  ping(): void {
    if (this.connected) this.ws.ping();
  }

  onPong(listener: () => void): () => void {
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

interface WsServerOptions {
  port: number;
  host?: string;
}

/** WS server：多连接。 */
export class NodeWsServerTransport implements IServerTransport {
  readonly onConnection = new EventStream<ITransport>();
  readonly onClose = new EventStream<void>();
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
        if (!this.listening) reject(err);
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

  isListening(): boolean {
    return this.listening;
  }

  async close(): Promise<void> {
    this.listening = false;
    if (this.wss === undefined) return;
    await new Promise<void>((resolve) => {
      this.wss!.close(() => resolve());
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
    if (!this.available) return Promise.reject(new AxtpError(ErrorCode.Unavailable, "transport unavailable"));
    return new Promise((resolve, reject) => {
      const ws = new WebSocket(this.options.url, this.options.protocols, {
        headers: this.options.headers
      });
      ws.once("open", () => {
        const transport = new WsTransport(ws);
        transport.onClose.subscribe(() => this.onClose.emit(undefined));
        resolve(transport);
      });
      ws.once("error", (err) => {
        if (this.available) reject(err);
      });
    });
  }

  isAvailable(): boolean {
    return this.available;
  }
}
