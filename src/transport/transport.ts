// Layer 2 transport：纯净的连接抽象。
// ITransport = 一条已建立连接（client 侧 / server 接受的每条）。
// IServerTransport.onConnection 每接受一个 client 产出一个新 ITransport——多 client 根基。
// 接口刻意不含心跳/WS 特有字段（WS 的 ping/pong 在构造 Connection 时闭包注入，不污染 ITransport）。

import type { Bytes } from "../io/bytes.js";
import type { AxtpError } from "../types/error.js";
import type { EventStream } from "../types/events.js";

/**
 * Physical 角色：谁发起传输连接（TCP/WS socket）。
 * 驱动 CONTROL OPEN/ACCEPT——Physical Client 发 OPEN，Physical Server 回 ACCEPT。
 * （spec: OPEN 跟随 Physical Client -> Physical Server）
 */
export type PhysicalRole = "client" | "server";

/**
 * Logical 角色：谁是能力提供方。
 * 驱动 RPC Hello/Identify/Identified——Logical Server 永远发 Hello、分配 sid；Logical Client 发 Identify。
 * （spec: Hello 永远由 Logical Server -> Logical Client）
 *
 * Physical 角色与 Logical 角色正交：经典场景下 AxtpClient=Physical Client+Logical Client、
 * AxtpServer=Physical Server+Logical Server；反向连接拓扑（设备主动连云但仍是 Logical Server）
 * 下二者分离，需分别指定。
 */
export type LogicalRole = "client" | "server";

/** 传输能力声明。 */
export interface TransportCapabilities {
  readonly messageOriented: boolean;
  /** framed=true（存在 CONTROL OPEN/ACCEPT/HEARTBEAT），WS=false。 */
  readonly supportsControl: boolean;
  /** 是否支持原生保活探测（WS=true 用 sendKeepalive/onKeepaliveAck；framed=false 走 CONTROL Heartbeat）。 */
  readonly supportsKeepalive: boolean;
}

/** 连接关闭原因。 */
export interface CloseReason {
  readonly code: CloseCode;
  readonly reason: string;
  /** 对端是否主动发起关闭（false=本端或传输错误）。 */
  readonly remote: boolean;
}

export const CloseCode = {
  Normal: 0,
  TransportError: 1,
  HeartbeatTimeout: 2,
  HandshakeFailed: 3,
  ProtocolError: 4,
  Reconnect: 5
} as const;

export type CloseCode = (typeof CloseCode)[keyof typeof CloseCode];

/** 单条已建立连接的传输接口。 */
export interface ITransport {
  readonly capabilities: TransportCapabilities;
  /** 发送原始字节。framed-binary 为帧字节，unframed-json 为 JSON 文本字节。 */
  send(bytes: Bytes): void;
  /** 入站字节流（事件驱动，无需 poll）。 */
  readonly onMessage: EventStream<Bytes>;
  readonly onClose: EventStream<CloseReason>;
  readonly onError: EventStream<AxtpError>;
  /** 关闭连接。 */
  close(): void;
  /** 是否仍处于连接态。 */
  isConnected(): boolean;
  /** Connection 接管：停止内部缓冲，flush 已缓冲消息（真实 transport 实现，mock 可选）。 */
  attach?(): void;
  /** 发送保活探测（supportsKeepalive=true 时实现）。 */
  sendKeepalive?(): void;
  /** 订阅保活确认到达（supportsKeepalive=true 时实现）。 */
  onKeepaliveAck?(listener: () => void): () => void;
}

/** server 侧：接受多连接。 */
export interface IServerTransport {
  readonly capabilities: TransportCapabilities;
  /** 开始监听；onConnection 每接受一个 client 产出一个新 ITransport。 */
  listen(): Promise<void>;
  readonly onConnection: EventStream<ITransport>;
  readonly onClose: EventStream<void>;
  close(): Promise<void>;
  /** 是否正在监听。 */
  isListening(): boolean;
}

/** client 侧：发起单连接。 */
export interface IClientTransport {
  readonly capabilities: TransportCapabilities;
  /** 发起连接，成功后返回一条已建立的 ITransport。 */
  connect(): Promise<ITransport>;
  readonly onClose: EventStream<void>;
  /** 是否仍可重连（transport 未被销毁）。 */
  isAvailable(): boolean;
}

/** transport 工厂：返回一条已建立的传输连接。供 Connection 重连使用。 */
export type TransportFactory = () => Promise<ITransport>;

/** 默认能力工厂，供具体 transport 复用。 */
export function framedBinaryCapabilities(): TransportCapabilities {
  return {
    messageOriented: false,
    supportsControl: true,
    supportsKeepalive: false
  };
}

export function unframedJsonCapabilities(): TransportCapabilities {
  return {
    messageOriented: true,
    supportsControl: false,
    supportsKeepalive: true
  };
}
