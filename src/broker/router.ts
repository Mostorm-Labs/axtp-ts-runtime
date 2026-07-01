// HandlerRouter：method/event handler 路由（broker 层，单一职责）。
// local Map + 可选 global source fallback；按 name 索引（规避 method id 与 event id 共享数字空间）。
// 实现 GlobalHandlerSource——AxtpServer 用一个 router 作全局表，注入每个 endpoint 的 router。

import type { GlobalHandlerSource, UntypedEventHandler, UntypedMethodHandler } from "./context.js";

const EMPTY_SET: Set<UntypedEventHandler> = new Set();

export class HandlerRouter implements GlobalHandlerSource {
  private readonly methodHandlers = new Map<string, UntypedMethodHandler>();
  private readonly eventHandlers = new Map<string, Set<UntypedEventHandler>>();
  private readonly globalSource?: GlobalHandlerSource;

  constructor(globalSource?: GlobalHandlerSource) {
    this.globalSource = globalSource;
  }

  // ===== method handler =====

  setMethod(name: string, handler: UntypedMethodHandler): () => void {
    this.methodHandlers.set(name, handler);
    return () => {
      if (this.methodHandlers.get(name) === handler) this.methodHandlers.delete(name);
    };
  }

  getMethod(name: string): UntypedMethodHandler | undefined {
    return this.methodHandlers.get(name) ?? this.globalSource?.getMethod(name);
  }

  // ===== event handler =====

  addEventListener(event: string, handler: UntypedEventHandler): () => void {
    const set = this.eventHandlers.get(event) ?? new Set<UntypedEventHandler>();
    if (!this.eventHandlers.has(event)) this.eventHandlers.set(event, set);
    set.add(handler);
    return () => set.delete(handler);
  }

  /** local event handlers（实现 GlobalHandlerSource：供嵌套 router 查询）。 */
  getEventListeners(name: string): Set<UntypedEventHandler> | undefined {
    return this.eventHandlers.get(name);
  }

  /** event 的全部 handler（local + global 合并快照）。 */
  getEventHandlers(event: string): Set<UntypedEventHandler> {
    const local = this.eventHandlers.get(event);
    const global = this.globalSource?.getEventListeners(event);
    if (local === undefined && global === undefined) return EMPTY_SET;
    if (global === undefined) return local as Set<UntypedEventHandler>;
    if (local === undefined) return global;
    const merged = new Set<UntypedEventHandler>();
    for (const h of local) merged.add(h);
    for (const h of global) merged.add(h);
    return merged;
  }
}
