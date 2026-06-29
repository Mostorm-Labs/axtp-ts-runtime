// ReconnectCoordinator：传输重连编排（退避 + transportFactory + 链路重建触发）。
// 失败时 emit onError 通知上层，成功时通过 onReconnected 回调重建 pipeline。
//
// 生命周期：start() → schedule() → attempt() → onReconnected(reset) → [链路ready] → notifySuccess()
//   重连成功后必须调 reset()（把 active 设回 false），否则下次断连 start() 不生效（#4b 修复）。

import type { ITransport, TransportFactory } from "../../transport/transport.js";
import { AxtpError, ErrorCode } from "../../types/error.js";
import { nextDelay, resolvePolicy, type ReconnectPolicy } from "./reconnect.js";

export class ReconnectCoordinator {
  private attempts = 0;
  private timer: ReturnType<typeof setTimeout> | undefined;
  private active = false;

  constructor(
    private readonly policy: ReturnType<typeof resolvePolicy>,
    private readonly transportFactory: TransportFactory,
    private readonly onReconnected: (transport: ITransport) => void,
    private readonly onFailed: () => void,
    private readonly onError: (err: AxtpError) => void
  ) {}

  static fromPolicy(
    policy: ReconnectPolicy | undefined,
    transportFactory: TransportFactory,
    callbacks: {
      onReconnected: (transport: ITransport) => void;
      onFailed: () => void;
      onError: (err: AxtpError) => void;
    }
  ): ReconnectCoordinator {
    return new ReconnectCoordinator(
      resolvePolicy(policy),
      transportFactory,
      callbacks.onReconnected,
      callbacks.onFailed,
      callbacks.onError
    );
  }

  /** 开始重连编排。若已 active 则忽略（防止重复触发）。 */
  start(): void {
    if (this.active) return;
    this.active = true;
    this.schedule();
  }

  /** 停止重连（用户主动关闭时）。清除定时器，设 active=false。 */
  stop(): void {
    this.active = false;
    if (this.timer !== undefined) {
      clearTimeout(this.timer);
      this.timer = undefined;
    }
  }

  /**
   * 重连成功后重置状态（#4b 修复）：把 active 设回 false，使下次断连的 start() 能生效。
   * 退避计数重置由 notifySuccess() 负责（在链路真正 ready 后调）。
   */
  reset(): void {
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
    if (!this.active) return;
    this.onReconnected(transport);
  }

  /** 重连链路建立成功后调用（Connection 在 onNegotiatedLinkReady/fireLinkReady 调此重置退避）。 */
  notifySuccess(): void {
    if (this.policy.resetBackoffOnSuccess) {
      this.attempts = 0;
    }
  }
}
