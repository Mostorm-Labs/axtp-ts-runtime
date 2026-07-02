// broker/context.ts — Broker 层共享类型（消除 router↔broker 循环 type-import）。
// 与 session 层解耦：Broker 零协议知识，只认 RpcMessage 语义 + handler 注册表。

import type { EventName, EventPayload } from "../types/registry.js";

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
  /** Server 管理时的 endpoint localId（独立用 Endpoint 时 undefined）。供 handler 做 server 级定向操作（callRaw/emitTo）。 */
  readonly id?: number;
  /** typed 事件推送：EventName 类型检查 + payload 类型约束。 */
  emit<K extends EventName>(event: K, payload: EventPayload<K>): void;
  /** 弱类型事件推送：任意事件名。 */
  emitRaw(event: string, payload: unknown): void;
}
