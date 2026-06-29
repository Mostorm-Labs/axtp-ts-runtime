// HandlerRegistry：handler 表存储原语（method + event）。
// 既作为 server 端全局共享表（实现 GlobalHandlerSource），也作为 HandlerRouter 的本地存储。
// 类型定义在 types.ts（消除 handlerRouter ↔ handlerRegistry 的循环 type-import）。

import type { GlobalHandlerSource, UntypedEventHandler, UntypedMethodHandler } from "../types.js";

export class HandlerRegistry implements GlobalHandlerSource {
  private readonly methodHandlers = new Map<string, UntypedMethodHandler>();
  private readonly eventHandlers = new Map<string, Set<UntypedEventHandler>>();

  setMethod(name: string, handler: UntypedMethodHandler): () => void {
    this.methodHandlers.set(name, handler);
    return () => this.removeMethod(name, handler);
  }

  removeMethod(name: string, handler: UntypedMethodHandler): void {
    if (this.methodHandlers.get(name) === handler) this.methodHandlers.delete(name);
  }

  getMethod(name: string): UntypedMethodHandler | undefined {
    return this.methodHandlers.get(name);
  }

  addEventListener(name: string, handler: UntypedEventHandler): () => void {
    const set = this.eventHandlers.get(name) ?? new Set<UntypedEventHandler>();
    if (!this.eventHandlers.has(name)) this.eventHandlers.set(name, set);
    set.add(handler);
    return () => set.delete(handler);
  }

  getEventListeners(name: string): Set<UntypedEventHandler> | undefined {
    return this.eventHandlers.get(name);
  }
}
