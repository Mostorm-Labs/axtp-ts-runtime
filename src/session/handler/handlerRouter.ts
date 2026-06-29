// HandlerRouter：method/event handler 路由（单一职责）。
// 组合一个 HandlerRegistry 做本地存储 + 全局委托 fallback。
// 按 name 索引——规避 method id 与 event id 共享同一数字空间。

import { HandlerRegistry } from "./handlerRegistry.js";

export type UntypedMethodHandler = (ctx: unknown, params: unknown) => unknown | Promise<unknown>;
export type UntypedEventHandler = (payload: unknown) => void;

/** 全局 handler registry 接口（server 多 session 共享）。 */
export interface GlobalHandlerSource {
  getMethod: (name: string) => UntypedMethodHandler | undefined;
  getEventListeners: (name: string) => Set<UntypedEventHandler> | undefined;
}

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
    // 每次 new Set 合并 local + global：保证调用方拿到的是快照副本（不会因后续注册/注销影响正在遍历的集合）。
    // 事件频率低（spec: Event 是低频投递），无需缓存。
    const handlers = new Set<UntypedEventHandler>();
    const localSet = this.local.getEventListeners(event);
    if (localSet !== undefined) for (const h of localSet) handlers.add(h);
    const globalSet = this.globalHandlers?.getEventListeners(event);
    if (globalSet !== undefined) for (const h of globalSet) handlers.add(h);
    return handlers;
  }
}
