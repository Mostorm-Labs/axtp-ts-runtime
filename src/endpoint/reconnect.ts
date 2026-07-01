// Reconnect：传输重连策略 + 退避算法 + ReconnectCoordinator（endpoint 层）。
// 指数退避 + 抖动，成功后归零。coordinator 泛型化（transport 类型由调用方定，Endpoint 用 StreamTransport）。
// AxtpClient 用它编排：断连 → 退避 → transportFactory() → 新 Endpoint（broker/router 跨连接复用保 handler）。

import type { StreamTransport } from "../transport/contract.js";
import { AxtpError, ErrorCode } from "../types/error.js";

export interface ReconnectPolicy {
  enabled: boolean;
  /** 首次重连延迟 ms（默认 1000）。 */
  initialDelayMs?: number;
  /** 退避上限 ms（默认 30000）。 */
  maxDelayMs?: number;
  /** 最大尝试次数（默认 Infinity）。 */
  maxAttempts?: number;
  /** 指数因子（默认 2）。 */
  multiplier?: number;
  /** 是否抖动（默认 true，避免惊群）。 */
  jitter?: boolean;
  /** 成功后重置退避计数（默认 true）。 */
  resetBackoffOnSuccess?: boolean;
}

const defaultReconnectPolicy: Required<ReconnectPolicy> = {
  enabled: false,
  initialDelayMs: 1000,
  maxDelayMs: 30000,
  maxAttempts: Number.POSITIVE_INFINITY,
  multiplier: 2,
  jitter: true,
  resetBackoffOnSuccess: true
};

/** 计算第 attempt 次重连的延迟（全抖动）。 */
export function nextDelay(policy: Required<ReconnectPolicy>, attempt: number): number {
  const base = policy.initialDelayMs;
  const max = policy.maxDelayMs;
  const mult = policy.multiplier;
  const exp = Math.min(base * Math.pow(mult, attempt), max);
  if (!policy.jitter) return Math.max(1, exp);
  return Math.max(1, Math.floor(Math.random() * exp));
}

/** merge 用户 policy 与默认值。enabled=false 时返回 disabled policy。 */
export function resolvePolicy(policy?: ReconnectPolicy): Required<ReconnectPolicy> {
  if (policy === undefined) return { ...defaultReconnectPolicy };
  return { ...defaultReconnectPolicy, ...policy };
}

/**
 * 重连编排器：start() → schedule() → attempt() → onReconnected（交还 transport，置 active=false）
 *   → [Endpoint ready] → onSuccess()（reset attempts + 清 timer）。
 * active 在 attempt() 交还 transport 时即置 false，使 ready 前的再次断连能重新 start()。
 */
export class ReconnectCoordinator {
  private attempts = 0;
  private timer: ReturnType<typeof setTimeout> | undefined;
  private active = false;

  constructor(
    private readonly policy: Required<ReconnectPolicy>,
    private readonly transportFactory: () => Promise<StreamTransport>,
    private readonly onReconnected: (transport: StreamTransport) => void,
    private readonly onFailed: () => void,
    private readonly onError: (err: AxtpError) => void
  ) {}

  /** 开始重连编排。已 active 则忽略。 */
  start(): void {
    if (this.active) return;
    this.active = true;
    this.schedule();
  }

  /** 停止重连（用户主动关闭时）。清定时器，置 active=false。 */
  stop(): void {
    this.active = false;
    if (this.timer !== undefined) {
      clearTimeout(this.timer);
      this.timer = undefined;
    }
  }

  get attemptCount(): number {
    return this.attempts;
  }

  private schedule(): void {
    if (!this.active) return;
    if (this.attempts >= this.policy.maxAttempts) {
      this.active = false;
      this.onFailed();
      return;
    }
    const delay = nextDelay(this.policy, this.attempts);
    this.attempts += 1;
    this.timer = setTimeout(() => {
      this.attempt().catch((err) => {
        this.onError(
          err instanceof AxtpError
            ? err
            : new AxtpError(ErrorCode.TransportDisconnected, `reconnect attempt failed: ${err}`)
        );
        if (this.active) this.schedule();
      });
    }, delay);
  }

  private async attempt(): Promise<void> {
    const transport = await this.transportFactory();
    if (!this.active) {
      transport.close();
      return;
    }
    this.onReconnected(transport);
    this.active = false;
  }

  /** 新连接 ready 后调用：reset attempts + 清 timer，使下次断连的 start() 生效。 */
  onSuccess(): void {
    this.active = false;
    if (this.timer !== undefined) {
      clearTimeout(this.timer);
      this.timer = undefined;
    }
    if (this.policy.resetBackoffOnSuccess) {
      this.attempts = 0;
    }
  }
}
