// Session 层共享类型（消除子组件循环依赖）。
// CallContext/CallOptions/MethodHandler/EventHandler/SessionIO 定义在此。

import type { ReconnectPolicy } from "../connection/reconnect.js";
import type { RpcPayload } from "../protocol/model.js";
import type {
  CloseCode,
  LogicalRole,
  PhysicalRole,
  TransportFactory
} from "../transport/transport.js";
import type {
  EventName,
  EventPayload,
  MethodName,
  MethodRequest,
  MethodResponse
} from "../types/registry.js";
import type {
  GlobalHandlerSource,
  UntypedEventHandler,
  UntypedMethodHandler
} from "./handler/handlerRouter.js";

/** Session 提供给子组件的发送接口（避免子组件直接依赖 Connection）。 */
export interface SessionIO {
  sendRpc(payload: RpcPayload): void;
}

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

/**
 * Session 选项（不继承 ConnectionOptions，避免连接层参数泄漏到用户 API）。
 * 连接参数在此内联声明，Session 内部构造 ConnectionOptions 传递。
 */
export interface SessionOptions {
  // === 连接参数（透传给 Connection，但不暴露 negotiationParams 等链路细节） ===
  heartbeatIntervalMs?: number;
  heartbeatTimeoutMs?: number;
  maxFrameSize?: number;
  reconnect?: ReconnectPolicy;

  // === 会话语义 ===
  physicalRole?: PhysicalRole;
  logicalRole?: LogicalRole;
  defaultTimeoutMs?: number;
  /** 握手超时 ms（超时后 onReady reject + close）。 */
  handshakeTimeoutMs?: number;
  globalHandlers?: GlobalHandlerSource;
  transportFactory?: TransportFactory;
  handshakeSeed?: number;
}

/** Session 关闭信息（保留 CloseCode）。 */
export interface SessionCloseInfo {
  readonly code: CloseCode;
  readonly reason: string;
  readonly remote: boolean;
}

// 重新导出 handler 类型（子组件统一从这里取）
export type { GlobalHandlerSource, UntypedEventHandler, UntypedMethodHandler };
