// HandlerRouter：method/event handler 表 + 全局委托（单一职责：handler 路由）。
// 按 name 索引——规避 method id 与 event id 共享同一数字空间。
// server 端全局 handler 委托（dispatchRequest/dispatchEvent miss 时查 globalHandlers）。

export type UntypedMethodHandler = (ctx: unknown, params: unknown) => unknown | Promise<unknown>;
export type UntypedEventHandler = (payload: unknown) => void;

/** 全局 handler registry 接口（server 多 session 共享）。 */
export interface GlobalHandlerSource {
  getMethod: (name: string) => UntypedMethodHandler | undefined;
  getEventListeners: (name: string) => Set<UntypedEventHandler> | undefined;
}

export class HandlerRouter {
  private readonly methodHandlers = new Map<string, UntypedMethodHandler>();
  private readonly eventHandlers = new Map<string, Set<UntypedEventHandler>>();
  private readonly globalHandlers?: GlobalHandlerSource;

  constructor(globalHandlers?: GlobalHandlerSource) {
    this.globalHandlers = globalHandlers;
  }

  // ===== method handler =====

  setMethod(name: string, handler: UntypedMethodHandler): () => void {
    this.methodHandlers.set(name, handler);
    return () => {
      if (this.methodHandlers.get(name) === handler) this.methodHandlers.delete(name);
    };
  }

  removeMethod(name: string, handler: UntypedMethodHandler): void {
    if (this.methodHandlers.get(name) === handler) this.methodHandlers.delete(name);
  }

  getMethod(name: string): UntypedMethodHandler | undefined {
    return this.methodHandlers.get(name) ?? this.globalHandlers?.getMethod(name);
  }

  // ===== event handler =====

  addEventListener(event: string, handler: UntypedEventHandler): () => void {
    const set = this.eventHandlers.get(event) ?? new Set<UntypedEventHandler>();
    if (!this.eventHandlers.has(event)) this.eventHandlers.set(event, set);
    set.add(handler);
    return () => set.delete(handler);
  }

  getEventHandlers(event: string): Set<UntypedEventHandler> {
    const handlers = new Set<UntypedEventHandler>();
    const localSet = this.eventHandlers.get(event);
    if (localSet !== undefined) for (const h of localSet) handlers.add(h);
    const globalSet = this.globalHandlers?.getEventListeners(event);
    if (globalSet !== undefined) for (const h of globalSet) handlers.add(h);
    return handlers;
  }
}
