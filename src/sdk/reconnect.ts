// ReconnectPolicy + 退避算法。
// Client 自动重连：全新 session（新 sid），迁移 handle/on 订阅，pending/stream 按 reject 失败。
// 指数退避 + 抖动，成功后归零。

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

export const defaultReconnectPolicy: Required<ReconnectPolicy> = {
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
  if (!policy.jitter) return exp;
  return Math.floor(Math.random() * exp);
}

export interface ReconnectInfo {
  readonly attempt: number;
  readonly totalDowntimeMs: number;
}

/** merge 用户 policy 与默认值。 */
export function resolvePolicy(policy?: ReconnectPolicy): Required<ReconnectPolicy> {
  if (policy === undefined) return { ...defaultReconnectPolicy };
  return { ...defaultReconnectPolicy, ...policy };
}
