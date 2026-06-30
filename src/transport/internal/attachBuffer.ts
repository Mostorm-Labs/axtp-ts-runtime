// AttachedMessageBuffer：transport attach 前的消息缓冲。
// 真实 transport（TCP/WS）在 Connection 订阅 onMessage 前可能已有消息到达，缓冲之，attach 时 flush。
// 消除 TCP/WS 之间重复的 attached 标志 + buffered[] + flush 逻辑。

import type { Bytes } from "../../io/bytes.js";
import type { EventStream } from "../../types/events.js";

export class AttachedMessageBuffer {
  private attached = false;
  private readonly buffered: Bytes[] = [];

  constructor(private readonly onMessage: EventStream<Bytes>) {}

  /** 入站消息：attach 前缓冲，attach 后直投 onMessage。 */
  push(bytes: Bytes): void {
    if (!this.attached) {
      this.buffered.push(bytes);
      return;
    }
    this.onMessage.emit(bytes);
  }

  /** Connection 接管：停止缓冲，flush 已缓冲消息到 onMessage。幂等。 */
  attach(): void {
    if (this.attached) return;
    this.attached = true;
    const buffered = this.buffered.splice(0);
    for (const bytes of buffered) this.onMessage.emit(bytes);
  }
}
