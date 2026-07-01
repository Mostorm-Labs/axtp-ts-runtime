// Heartbeat：心跳编排器（endpoint 层，纯计时器 + 回调）。
// tick 与 timeout 分离——tick 保持固定节拍发送探测，收到 ack 只重置 timeout 计时器。
// 对端持续快速回 ack 不会拉长探测间隔。
// Endpoint 在 linkReady 时启动；framed onTick→core.sendHeartbeat，unframed onTick→transport.sendKeepalive。

export interface HeartbeatConfig {
  /** 探测间隔 ms（固定节拍，不受 ack 影响）。 */
  readonly intervalMs: number;
  /** 无响应超时 ms（每次 tick 后启动，收到 ack 取消）。 */
  readonly timeoutMs: number;
  /** 发出一次探测。 */
  readonly onTick: () => void;
  /** 超时触发（Endpoint 关闭连接）。 */
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
    this.scheduleTimeout();
  }

  stop(): void {
    this.running = false;
    this.clearTimers();
  }

  /** 收到对端响应（HeartbeatAck / keepalive ack）——只取消 timeout，tick 继续固定节拍。 */
  reset(): void {
    if (!this.running) return;
    if (this.timeoutTimer !== undefined) {
      clearTimeout(this.timeoutTimer);
      this.timeoutTimer = undefined;
    }
  }

  get isRunning(): boolean {
    return this.running;
  }

  private scheduleTick(): void {
    if (!this.running) return;
    this.tickTimer = setTimeout(() => {
      if (!this.running) return;
      this.config.onTick();
      this.scheduleTick();
    }, this.config.intervalMs);
  }

  private scheduleTimeout(): void {
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
