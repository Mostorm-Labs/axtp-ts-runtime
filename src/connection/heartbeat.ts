// Heartbeat：心跳编排器（单一类 + 纯回调配置）。
// 归 Connection（连接语义）。
// D2: tick 和 timeout 分离——tick 保持固定节拍发送探测，收到 ack 只重置 timeout 计时器。
// 对端持续快速回 ack 不会拉长探测间隔。

export interface HeartbeatConfig {
  /** 探测间隔 ms（固定节拍，不受 ack 影响）。 */
  readonly intervalMs: number;
  /** 无响应超时 ms（每次 tick 后启动，收到 ack 取消）。 */
  readonly timeoutMs: number;
  /** 发出一次探测。 */
  readonly onTick: () => void;
  /** 超时触发（Connection 关闭连接）。 */
  readonly onTimeout: () => void;
}

export class Heartbeat {
  private tickTimer: ReturnType<typeof setTimeout> | undefined;
  private timeoutTimer: ReturnType<typeof setTimeout> | undefined;
  private running = false;

  constructor(private readonly config: HeartbeatConfig) {}

  start(): void {
    if (this.running) return;
    this.running = true;
    this.scheduleTick();
  }

  stop(): void {
    this.running = false;
    this.clearTimers();
  }

  /**
   * D2: 收到对端响应（HeartbeatAck / keepalive ack）——只取消 timeout 计时器。
   * tick 计时器保持固定节拍，不被 ack 推迟。
   */
  reset(): void {
    if (!this.running) return;
    // 只取消 timeout（tick 继续）
    if (this.timeoutTimer !== undefined) {
      clearTimeout(this.timeoutTimer);
      this.timeoutTimer = undefined;
    }
  }

  get isRunning(): boolean {
    return this.running;
  }

  /** D2: tick 保持固定节拍——每次 tick 后安排下一次 tick + 启动 timeout。 */
  private scheduleTick(): void {
    if (!this.running) return;
    this.tickTimer = setTimeout(() => {
      if (!this.running) return;
      this.config.onTick();
      // 启动 timeout 计时（ack 到达时由 reset 取消）
      this.scheduleTimeout();
      // 安排下一次 tick（固定间隔，不等 ack）
      this.scheduleTick();
    }, this.config.intervalMs);
  }

  private scheduleTimeout(): void {
    // 取消之前的 timeout（如果还没触发）
    if (this.timeoutTimer !== undefined) clearTimeout(this.timeoutTimer);
    this.timeoutTimer = setTimeout(() => {
      this.config.onTimeout();
      this.stop();
    }, this.config.timeoutMs);
  }

  private clearTimers(): void {
    if (this.tickTimer !== undefined) {
      clearTimeout(this.tickTimer);
      this.tickTimer = undefined;
    }
    if (this.timeoutTimer !== undefined) {
      clearTimeout(this.timeoutTimer);
      this.timeoutTimer = undefined;
    }
  }
}
