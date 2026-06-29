// HandlerRouter：method/event handler 路由（单一职责）。
// 组合一个 HandlerRegistry 做本地存储 + 全局委托 fallback。
// 按 name 索引——规避 method id 与 event id 共享同一数字空间。

import { HandlerRegistry } from "./handlerRegistry.js";
import type { GlobalHandlerSource, UntypedEventHandler, UntypedMethodHandler } from "../types.js";

export class HandlerRouter {
  private readonly local = new HandlerRegistry();
  private readonly globalHandlers?: GlobalHandlerSource;

  constructor(globalHandlers?: GlobalHandlerSource) {
    this.globalHandlers = globalHandlers;
  }

  // ===== method handler =====

  setMethod(name: string, handler: UntypedMethodHandler): () => void {
    return this.local.setMethod(name, handler);
  }

  removeMethod(name: string, handler: UntypedMethodHandler): void {
    this.local.removeMethod(name, handler);
  }

  getMethod(name: string): UntypedMethodHandler | undefined {
    return this.local.getMethod(name) ?? this.globalHandlers?.getMethod(name);
  }

  // ===== event handler =====

  addEventListener(event: string, handler: UntypedEventHandler): () => void {
    return this.local.addEventListener(event, handler);
  }

  getEventHandlers(event: string): Set<UntypedEventHandler> {
    return mergeSets(
      this.local.getEventListeners(event),
      this.globalHandlers?.getEventListeners(event)
    );
  }
}

/** 合并多个 Set 为一个新的快照副本（调用方遍历安全）。 */
function mergeSets<T>(...sets: (Set<T> | undefined)[]): Set<T> {
  const result = new Set<T>();
  for (const set of sets) {
    if (set !== undefined) for (const item of set) result.add(item);
  }
  return result;
}
