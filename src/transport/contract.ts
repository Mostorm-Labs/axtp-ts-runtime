// Transport 契约：Core/Endpoint 经 Web Streams 消费的传输抽象。
// profile（来自 profile.ts）是能力与心跳的单一事实源；StreamTransport 是已建立连接的流式抽象。

import type { Bytes } from "../io/bytes.js";
import type { EventStream } from "../types/events.js";
import type { TransportProfile } from "./profile.js";

export {
  framedBinaryProfile,
  keepaliveMode,
  supportsControl,
  supportsStream,
  unframedJsonProfile
} from "./profile.js";
export type { FrameMode, KeepaliveMode, TransportProfile, TransportProfileId } from "./profile.js";

/** Physical 角色：谁发起传输连接（TCP/WS socket）。驱动 CONTROL OPEN/ACCEPT。 */
export type PhysicalRole = "client" | "server";

/** Logical 角色：谁是能力提供方。驱动 RPC Hello/Identify/Identified（与 Physical 正交）。 */
export type LogicalRole = "client" | "server";

/** 已建立的流式传输连接：readable/writable Web Stream + close。 */
export interface StreamTransport {
  readonly profile: TransportProfile;
  readonly readable: ReadableStream<Bytes>;
  readonly writable: WritableStream<Bytes>;
  close(): void;
  terminate?(): void;
}

/** native keepalive 能力（unframed-json/WS）。Endpoint 据此驱动 ping/pong（framed 走 CONTROL 心跳）。 */
export interface KeepaliveStreamTransport extends StreamTransport {
  sendKeepalive(): void;
  onKeepaliveAck(listener: () => void): () => void;
}

/** stream client transport 契约：connect() 返回一条已建立的 StreamTransport（可复用，供重连）。 */
/** stream client transport 契约：connect() 返回一条已建立的 StreamTransport（可复用，供重连）。 */
export interface StreamClientTransport {
  readonly profile: TransportProfile;
  connect(): Promise<StreamTransport>;
}

/** stream server transport 契约：listen + onConnection(每条接受的连接) + close。 */
export interface StreamServerTransport {
  readonly profile: TransportProfile;
  listen(): Promise<void>;
  readonly onConnection: EventStream<StreamTransport>;
  close(): Promise<void>;
}

/** 流式 transport 工厂（供 Endpoint 首次连接与重连）。 */
export type StreamTransportFactory = () => Promise<StreamTransport>;
