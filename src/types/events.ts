// EventStream：事件驱动的多播原语。
// transport.onMessage / session.onClose / server.onConnect 等都用它。
// subscribe 返回 unsubscribe 函数；emit 同步通知所有订阅者。
//
// 嵌套 emit（listener 回调内又 emit 同一 stream）：排队到 pendingEmits，
// 当前 emit 结束后 flush（不再静默丢弃）。
//
// close() 后进入终态：后续 subscribe/emit 为 no-op，不可逆。

export type Listener<T> = (value: T) => void;

export class EventStream<T> {
  private readonly listeners = new Set<Listener<T>>();
  private emitting = false;
  private pendingRemovals: Listener<T>[] = [];
  /** 嵌套 emit 的排队缓冲（emit 期间 listener 又触发 emit 时，延后到当前 emit 结束）。 */
  private pendingEmits: T[] = [];
  /** 终态标志：close 后不可再 subscribe/emit。 */
  private closed = false;

  /** 订阅。返回 unsubscribe 函数。close 后为 no-op。 */
  subscribe(listener: Listener<T>): () => void {
    if (this.closed) return () => {};
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

  /** 发出事件，同步通知所有当前订阅者。嵌套 emit 排队延后。close 后为 no-op。 */
  emit(value: T): void {
    if (this.closed) return;
    if (this.emitting) {
      // 嵌套 emit：排队，当前 emit 结束后 flush
      this.pendingEmits.push(value);
      return;
    }
    this.emitting = true;
    try {
      this.notifyListeners(value);
    } finally {
      this.emitting = false;
      this.flushPendingRemovals();
      this.flushPendingEmits();
    }
  }

  /** 关闭流：进入终态，清除所有订阅者和缓冲。不可逆——后续 subscribe/emit 为 no-op。 */
  close(): void {
    this.closed = true;
    if (this.emitting) {
      // emit 期间 close：标记终态，emit 结束后 flushPendingRemovals 会清空 listeners
      for (const listener of this.listeners) this.pendingRemovals.push(listener);
      this.pendingEmits.length = 0; // 丢弃排队中的嵌套 emit
      return;
    }
    this.listeners.clear();
    this.pendingRemovals.length = 0;
    this.pendingEmits.length = 0;
  }

  private notifyListeners(value: T): void {
    for (const listener of this.listeners) {
      try {
        listener(value);
      } catch {
        // 单个监听器抛错不影响其它监听器与后续 emit。
      }
    }
  }

  private flushPendingRemovals(): void {
    if (this.pendingRemovals.length === 0) return;
    for (const listener of this.pendingRemovals) {
      this.listeners.delete(listener);
    }
    this.pendingRemovals.length = 0;
  }

  /** flush 嵌套 emit 期间排队的事件。close 后不再 flush。 */
  private flushPendingEmits(): void {
    if (this.closed) return;
    if (this.pendingEmits.length === 0) return;
    const queued = this.pendingEmits;
    this.pendingEmits = [];
    for (const value of queued) {
      this.emit(value);
    }
  }
}
