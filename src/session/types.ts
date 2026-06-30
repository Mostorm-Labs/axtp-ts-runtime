// Session 层共享类型（消除子组件循环依赖）。
// CallContext/CallOptions/MethodHandler/EventHandler/SessionIO 定义在此。
// Handler 类型（UntypedMethodHandler/UntypedEventHandler/GlobalHandlerSource）也定义在此，
// 消除 handlerRouter ↔ handlerRegistry 的循环 type-import。

import type { ReconnectPolicy } from "../connection/reconnect/reconnect.js";
import type { RpcMessage } from "../protocol/model.js";
import type {
  CloseCode,
  LogicalRole,
  PhysicalRole
} from "../transport/transport.js";
import type {
  EventName,
  EventPayload,
  MethodName,
  MethodRequest,
  MethodResponse
} from "../types/registry.js";

// ===== Handler 类型（定义在此，handlerRouter/handlerRegistry 都从这里取） =====

export type UntypedMethodHandler = (ctx: unknown, params: unknown) => unknown | Promise<unknown>;
export type UntypedEventHandler = (payload: unknown) => void;

/** 全局 handler registry 接口（server 多 session 共享）。 */
export interface GlobalHandlerSource {
  getMethod: (name: string) => UntypedMethodHandler | undefined;
  getEventListeners: (name: string) => Set<UntypedEventHandler> | undefined;
}

// ===== SessionIO =====

/** Session 提供给子组件的发送接口（避免子组件直接依赖 Connection）。 */
export interface SessionIO {
  sendRpc(payload: RpcMessage): void;
  sendStream(streamId: number, data: Uint8Array, seqId: number, cursor?: bigint): void;
}

// ===== Call/Handler 类型 =====

/** call 选项。 */
export interface CallOptions {
  timeoutMs?: number;
}

/** handler 上下文。 */
export interface CallContext {
  readonly requestId: number;
  readonly sid: string;
  /** 向该对端推送事件（非 RPC 响应，是独立的 Event 消息）。 */
  emit: <K extends EventName>(event: K, payload: EventPayload<K>) => Promise<void>;
}

export type MethodHandler<K extends MethodName> = (
  ctx: CallContext,
  params: MethodRequest<K>
) => MethodResponse<K> | Promise<MethodResponse<K>>;

export type EventHandler<K extends EventName> = (payload: EventPayload<K>) => void;

// ===== Options =====

/**
 * Client/Server 共享的通用选项（消除 ClientOptions 与 ServerOptions 的字段重复）。
 * SDK 层 ClientOptions/ServerOptions 各自 extends 此接口。
 */
export interface CommonOptions {
  logicalRole?: LogicalRole;
  defaultTimeoutMs?: number;
  handshakeTimeoutMs?: number;
  heartbeatIntervalMs?: number;
  heartbeatTimeoutMs?: number;
  maxFrameSize?: number;
}

/**
 * Session 选项——用户可见部分（不含 SDK 内部注入字段）。
 * ReconnectPolicy 类型从 connection/reconnect 取（session 层依赖 connection 层是合理的上层→下层依赖）。
 */
export interface SessionOptions extends CommonOptions {
  /** 传输重连策略（透传给 Connection）。类型来自 connection/reconnect/reconnect.js，但只在此处声明。 */
  reconnect?: ReconnectPolicy;
}

/**
 * Session 内部配置——SDK 层注入（不暴露给最终用户）。
 */
export interface SessionInternalConfig {
  physicalRole?: PhysicalRole;
  globalHandlers?: GlobalHandlerSource;
  handshakeSeed?: number;
  /** client 在 Identify 携带的 eventMasks（订阅意图，hex 编码）。由 SDK 用 computeEventMasks 计算。 */
  eventMasks?: string;
}

/** Session 完整配置 = 用户选项 + 内部配置。 */
export type SessionConfig = SessionOptions & SessionInternalConfig;

/** Session 关闭信息（保留 CloseCode）。 */
export interface SessionCloseInfo {
  readonly code: CloseCode;
  readonly reason: string;
  readonly remote: boolean;
}
