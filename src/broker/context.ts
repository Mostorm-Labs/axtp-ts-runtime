// broker/context.ts — Broker 层共享类型（消除 router↔broker 循环 type-import）。
// 与 session 层解耦：Broker 零协议知识，只认 RpcMessage 语义 + handler 注册表。

/** 方法 handler（无类型；SDK 层用 typed 重载包一层）。 */
export type UntypedMethodHandler = (
  ctx: CallContext,
  params: unknown
) => unknown | Promise<unknown>;

/** 事件 handler（无类型）。 */
export type UntypedEventHandler = (payload: unknown) => void;

/** 全局 handler registry 接口（AxtpServer 多 endpoint 共享，注入每个 endpoint 的 broker）。 */
export interface GlobalHandlerSource {
  getMethod: (name: string) => UntypedMethodHandler | undefined;
  getEventListeners: (name: string) => Set<UntypedEventHandler> | undefined;
}

/** handler 上下文（每次入站 Request 构造一个）。 */
export interface CallContext {
  readonly requestId: number;
  readonly sid: string;
  /** 向该对端推送事件（独立 Event 消息，非 RPC 响应）。Endpoint 注入（绑定 core.emit）。 */
  emit: (event: string, payload: unknown) => void;
}
