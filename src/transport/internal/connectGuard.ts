// SettleGuard：client connect 的 resolve/reject 互斥守卫。
// TCP/WS 的 connect() 在「连接成功」与「error」两个事件间用 settled 标志保证 Promise 只 settle 一次。
// 收敛为 trySettle()，消除重复的 `let settled = false; if (settled) return; settled = true;` 模式。

export class SettleGuard {
  private settled = false;

  /** 首次调用返回 true（允许 settle），后续调用返回 false。 */
  trySettle(): boolean {
    if (this.settled) return false;
    this.settled = true;
    return true;
  }
}
