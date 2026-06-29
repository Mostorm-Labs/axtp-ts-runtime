// Heartbeat：心跳编排器（单一类 + 纯回调配置）。
// 归 Connection（连接语义）。framed 在 FRAMING_READY 启动；WS 用原生 keepalive（Connection 闭包注入）。
// 到达 intervalMs 触发 onTick（Connection 在回调里发 CONTROL Heartbeat 或 sendKeepalive）；
// 收到 ack 调 reset() 重置计时；timeoutMs 无响应触发 onTimeout（Connection 关闭连接）。

export interface HeartbeatConfig {
  /** 探测间隔 ms。 */
  readonly intervalMs: number;
  /** 无响应超时 ms（收到 ack 前的最长等待）。 */
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
    this.scheduleTick();
  }

  stop(): void {
    this.running = false;
    this.clearTimers();
  }

  /** 收到对端响应（HeartbeatAck / keepalive ack）：重置计时。 */
  reset(): void {
    if (!this.running) return;
    this.clearTimers();
    this.scheduleTick();
  }

  get isRunning(): boolean {
    return this.running;
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
