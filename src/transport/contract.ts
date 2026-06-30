// Layer 2 transport：纯净的连接抽象。
// ITransport = 一条已建立连接（client 侧 / server 接受的每条）。
// IServerTransport.onConnection 每接受一个 client 产出一个新 ITransport——多 client 根基。
// 接口刻意不含心跳/WS 特有字段（WS 的 ping/pong 在 KeepaliveTransport 声明，不污染 ITransport）。

import type { Bytes } from "../io/bytes.js";
import type { AxtpError } from "../types/error.js";
import type { EventStream } from "../types/events.js";
import type { TransportProfile } from "./profile.js";

// profile 模型——能力与心跳的单一事实来源（取代旧的 TransportCapabilities）。
export {
  framedBinaryProfile,
  keepaliveMode,
  supportsControl,
  supportsStream,
  unframedJsonProfile,
  type FrameMode,
  type KeepaliveMode,
  type TransportProfile,
  type TransportProfileId
} from "./profile.js";

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
  // 4 (ProtocolError) 已移除——使用 HandshakeFailed 替代
  Reconnect: 5
} as const;

export type CloseCode = (typeof CloseCode)[keyof typeof CloseCode];

/** 单条已建立连接的传输接口。 */
export interface ITransport {
  /** Transport profile——frameMode/能力/心跳的单一事实来源。 */
  readonly profile: TransportProfile;
  /** 发送原始字节。framed-binary 为帧字节，unframed-json 为 JSON 文本字节。 */
  send(bytes: Bytes): void;
  /** 入站字节流（事件驱动，无需 poll）。 */
  readonly onMessage: EventStream<Bytes>;
  readonly onClose: EventStream<CloseReason>;
  readonly onError: EventStream<AxtpError>;
  /** 关闭连接（实现可选择优雅：WS 发 close 帧、TCP destroy）。 */
  close(): void;
  /**
   * 强制立即断开，不发起/等待关闭握手（WS ws.terminate() / TCP socket.destroy()）。
   * 用于对端已无响应的场景（心跳超时、传输错误、重连失败）——优雅 close 握手在死连接上会悬空。
   * 未实现时由调用方回落到 close()。可安全多次调用。
   */
  terminate?(): void;
  /** Connection 接管：停止内部缓冲，flush 已缓冲消息（真实 transport 实现，mock 可选）。 */
  attach?(): void;
}

/**
 * 原生 keepalive 能力。仅由 keepaliveMode(profile)==="native-keepalive" 的 transport 实现
 * （unframed-json：WS ping/pong）。把原本散落在 ITransport 上的可选 sendKeepalive?/onKeepaliveAck?
 * 收敛为独立接口的必需方法——声明 unframed profile 但未实现它的 transport 在编译时即暴露，
 * 而非心跳启动时才运行时报错。
 */
export interface KeepaliveTransport extends ITransport {
  /** 发送保活探测（WS 映射到 ws.ping()）。 */
  sendKeepalive(): void;
  /** 订阅保活确认到达（WS 映射到 pong 事件）。返回退订函数。 */
  onKeepaliveAck(listener: () => void): () => void;
}

/** server 侧：接受多连接。 */
export interface IServerTransport {
  readonly profile: TransportProfile;
  /** 开始监听；onConnection 每接受一个 client 产出一个新 ITransport。 */
  listen(): Promise<void>;
  readonly onConnection: EventStream<ITransport>;
  readonly onClose: EventStream<void>;
  /** server 级错误（listen 成功后的 accept 期错误等）。 */
  readonly onError: EventStream<AxtpError>;
  close(): Promise<void>;
}

/** client 侧：发起单连接。 */
export interface IClientTransport {
  readonly profile: TransportProfile;
  /** 发起连接，成功后返回一条已建立的 ITransport。 */
  connect(): Promise<ITransport>;
  readonly onClose: EventStream<void>;
}

/** transport 工厂：返回一条已建立的传输连接。供 Connection 重连使用。 */
export type TransportFactory = () => Promise<ITransport>;
