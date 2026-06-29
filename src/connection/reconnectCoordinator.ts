// ReconnectCoordinator：传输重连编排（退避 + transportFactory + 链路重建触发）。
// 失败时 emit onError 通知上层，成功时通过 onReconnected 回调重建 pipeline。

import type { ITransport, TransportFactory } from "../transport/transport.js";
import { AxtpError, ErrorCode } from "../types/error.js";
import { nextDelay, resolvePolicy, type ReconnectPolicy } from "./reconnect.js";

export class ReconnectCoordinator {
  private attempts = 0;
  private timer: ReturnType<typeof setTimeout> | undefined;
  private active = false;

  constructor(
    private readonly policy: ReturnType<typeof resolvePolicy>,
    private readonly transportFactory: TransportFactory,
    private readonly onReconnected: (transport: ITransport) => void,
    private readonly onSuccess: () => void,
    private readonly onFailed: () => void,
    private readonly onError: (err: AxtpError) => void
  ) {}

  static fromPolicy(
    policy: ReconnectPolicy | undefined,
    transportFactory: TransportFactory,
    callbacks: {
      onReconnected: (transport: ITransport) => void;
      onSuccess: () => void;
      onFailed: () => void;
      onError: (err: AxtpError) => void;
    }
  ): ReconnectCoordinator {
    return new ReconnectCoordinator(
      resolvePolicy(policy),
      transportFactory,
      callbacks.onReconnected,
      callbacks.onSuccess,
      callbacks.onFailed,
      callbacks.onError
    );
  }

  start(): void {
    if (this.active) return;
    this.active = true;
    this.schedule();
  }

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
    if (!this.active) return;
    this.onReconnected(transport);
  }

  /** 重连成功后调用（Connection 重建 pipeline 后调此重置退避）。 */
  notifySuccess(): void {
    if (this.policy.resetBackoffOnSuccess) {
      this.attempts = 0;
    }
  }
}
