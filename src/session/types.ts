// Session 层共享类型（消除 session.ts ↔ rpcExchange.ts 循环类型依赖）。
// CallContext/CallOptions/MethodHandler/EventHandler 定义在此，
// 子组件和 Session 门面都从 types.ts 引入，避免互相 import。

import type { TransportFactory } from "../protocol/connection.js";
import type { ReconnectPolicy } from "../protocol/reconnect.js";
import type { LogicalRole, PhysicalRole } from "../transport/transport.js";
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
} from "./handlerRouter.js";

/** call 选项。 */
export interface CallOptions {
  timeoutMs?: number;
}

/** handler 上下文。 */
export interface CallContext {
  readonly requestId: number;
  readonly sid: string;
  reply: <K extends EventName>(event: K, payload: EventPayload<K>) => Promise<void>;
}

export type MethodHandler<K extends MethodName> = (
  ctx: CallContext,
  params: MethodRequest<K>
) => MethodResponse<K> | Promise<MethodResponse<K>>;

export type EventHandler<K extends EventName> = (payload: EventPayload<K>) => void;

/**
 * Session 选项（不继承 ConnectionOptions，避免连接层参数泄漏到用户 API）。
 * 连接参数（heartbeat/maxFrameSize/reconnect）在此内联声明，Session 内部构造 ConnectionOptions 传递。
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
  globalHandlers?: GlobalHandlerSource;
  transportFactory?: TransportFactory;
  handshakeSeed?: number;
}

// 重新导出 handler 类型（子组件统一从这里或 handlerRouter 取）
export type { GlobalHandlerSource, UntypedEventHandler, UntypedMethodHandler };
