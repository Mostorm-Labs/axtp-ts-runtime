// EventStream: 事件驱动的多播原语。
// transport.onMessage / session.onClose / server.onConnect 等都用它。
// subscribe 返回 unsubscribe 函数；emit 同步通知所有订阅者。

export type Listener<T> = (value: T) => void;

export class EventStream<T> {
  private readonly listeners = new Set<Listener<T>>();
  private emitting = false;
  private pendingRemovals: Listener<T>[] = [];

  /** 订阅。返回 unsubscribe 函数。 */
  subscribe(listener: Listener<T>): () => void {
    this.listeners.add(listener);
    return () => this.unsubscribe(listener);
  }

  /** 取消订阅。可在 emit 期间安全调用（延迟到 emit 结束）。 */
  unsubscribe(listener: Listener<T>): void {
    if (this.emitting) {
      this.pendingRemovals.push(listener);
      return;
    }
    this.listeners.delete(listener);
  }

  /** 发出事件，同步通知所有当前订阅者。 */
  emit(value: T): void {
    if (this.emitting) {
      // 防止重入 emit 造成不可预期的扩散；直接忽略嵌套 emit。
      return;
    }
    this.emitting = true;
    try {
      for (const listener of this.listeners) {
        try {
          listener(value);
        } catch {
          // 单个监听器抛错不影响其它监听器与后续 emit。
        }
      }
    } finally {
      this.emitting = false;
      this.flushPendingRemovals();
    }
  }

  /** 关闭流：清除所有订阅者。 */
  close(): void {
    this.listeners.clear();
    this.pendingRemovals.length = 0;
  }

  private flushPendingRemovals(): void {
    if (this.pendingRemovals.length === 0) return;
    for (const listener of this.pendingRemovals) {
      this.listeners.delete(listener);
    }
    this.pendingRemovals.length = 0;
  }
}
