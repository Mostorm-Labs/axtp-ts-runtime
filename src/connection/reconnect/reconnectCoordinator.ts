// ReconnectCoordinator：传输重连编排（退避 + transportFactory + 链路重建触发）。
// 失败时 emit onError 通知上层，成功时通过 onReconnected 回调重建 pipeline。
//
// 生命周期：start() → schedule() → attempt() → onReconnected（交还 transport，置 active=false）
//   → [链路 ready] → onSuccess()（reset attempts=0 + 清 timer）。
//   active 在 attempt() 交还 transport 时即置 false，使 link-ready 前的再次断连能重新 start()。

import type { ITransport, TransportFactory } from "../../transport/transport.js";
import { AxtpError, ErrorCode } from "../../types/error.js";
import { nextDelay, type resolvePolicy } from "./reconnect.js";

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
    // 交还 transport 后即置 idle：onReconnected 之后到 onSuccess（链路 ready）之间是异步握手窗口，
    // 若新 transport 在此期间断开，handleTransportClose→start() 必须能重新编排，否则因 active 仍为
    // true 而 start() 空转、timer 又是已触发的陈旧 id，连接会永久卡在 reconnecting。
    // attempts 已在 schedule() 自增，重入 schedule 的退避正确；onSuccess 仍负责 reset attempts + 清 timer。
    this.active = false;
  }

  /**
   * 重连链路建立成功后调用（Connection 在 onNegotiatedLinkReady/fireLinkReady 调此重置全部状态）。
   * 合并原 reset() + notifySuccess()：同时重置 active=false + attempts=0 + 清 timer，
   * 确保下次断连的 start() 能生效。
   */
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
