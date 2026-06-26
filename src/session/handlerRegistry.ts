// HandlerRegistry：全局 handler 表（server 端）。
// server.handle 注册到此，所有 session 共享查询（委托，不复制到每 session）。
// 按 name 索引——规避 method id 与 event id 共享空间。
// event 全局 handler：所有 session 的事件聚合上报。

import type { UntypedEventHandler, UntypedMethodHandler } from "./session.js";

export class HandlerRegistry {
  private readonly methodHandlers = new Map<string, UntypedMethodHandler>();
  private readonly eventHandlers = new Map<string, Set<UntypedEventHandler>>();

  setMethod(name: string, handler: UntypedMethodHandler): () => void {
    this.methodHandlers.set(name, handler);
    return () => {
      if (this.methodHandlers.get(name) === handler) this.methodHandlers.delete(name);
    };
  }

  getMethod(name: string): UntypedMethodHandler | undefined {
    return this.methodHandlers.get(name);
  }

  addEventListener(name: string, handler: UntypedEventHandler): () => void {
    let set = this.eventHandlers.get(name);
    if (set === undefined) {
      set = new Set();
      this.eventHandlers.set(name, set);
    }
    set.add(handler);
    return () => set!.delete(handler);
  }

  getEventListeners(name: string): Set<UntypedEventHandler> | undefined {
    return this.eventHandlers.get(name);
  }
}
