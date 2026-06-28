// Heartbeat：心跳编排器（单一类 + 纯回调配置）。
// 归 Connection（连接语义）。framed 在 FRAMING_READY 启动；WS 用原生 ping/pong（Connection 闭包注入）。
// 到达 intervalMs 触发 onTick（Connection 在回调里发 CONTROL Heartbeat 或 ws.ping）；
// 收到 ack/pong 调 reset() 重置计时；连续 timeoutMs 无响应触发 onTimeout（Connection 关闭连接）。

export interface HeartbeatConfig {
  /** 探测间隔 ms。 */
  readonly intervalMs: number;
  /** 无响应超时 ms（收到 ack/pong 前的最长等待）。 */
  readonly timeoutMs: number;
  /** 发出一次探测。 */
  readonly onTick: () => void;
  /** 连续超时触发（Connection 关闭连接）。 */
  readonly onTimeout: () => void;
}

export class Heartbeat {
  private tickTimer: ReturnType<typeof setTimeout> | undefined;
  private timeoutTimer: ReturnType<typeof setTimeout> | undefined;
  private running = false;
  /** 连续超时次数。达到阈值触发 onTimeout（默认 1 次 timeoutMs 无响应即触发）。 */
  private missedTicks = 0;
  private readonly maxMissedTicks = 1;
  private configValue: HeartbeatConfig;

  constructor(config: HeartbeatConfig) {
    this.configValue = config;
  }

  private get config(): HeartbeatConfig {
    return this.configValue;
  }

  start(): void {
    if (this.running) return;
    this.running = true;
    this.missedTicks = 0;
    this.scheduleTick();
  }

  stop(): void {
    this.running = false;
    this.clearTimers();
  }

  /** 收到对端响应（HeartbeatAck / pong）：重置计时。 */
  reset(): void {
    if (!this.running) return;
    this.missedTicks = 0;
    this.clearTimers();
    this.scheduleTick();
  }

  get isRunning(): boolean {
    return this.running;
  }

  /** 更新配置（如重新协商 intervalMs）。 */
  updateConfig(config: Partial<HeartbeatConfig>): void {
    this.configValue = { ...this.configValue, ...config };
    if (this.running) {
      this.clearTimers();
      this.scheduleTick();
    }
  }

  private scheduleTick(): void {
    if (!this.running) return;
    this.tickTimer = setTimeout(() => {
      this.config.onTick();
      this.scheduleTimeout();
    }, this.config.intervalMs);
  }

  private scheduleTimeout(): void {
    this.timeoutTimer = setTimeout(() => {
      this.missedTicks += 1;
      if (this.missedTicks >= this.maxMissedTicks) {
        this.config.onTimeout();
        this.stop();
      } else {
        // 继续等待下一个 tick
        this.scheduleTick();
      }
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

/** 默认配置工厂。 */
export function defaultHeartbeatConfig(
  intervalMs = 30000
): Omit<HeartbeatConfig, "onTick" | "onTimeout"> {
  return { intervalMs, timeoutMs: Math.max(intervalMs * 2, 10000) };
}
