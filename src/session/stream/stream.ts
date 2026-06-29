// Stream：应用层数据流句柄（双向）。
// 归 session 层（由 StreamManager 构造，依赖 StreamContext 同目录）。
// onChunk 接收对端数据（cursor 透传，Core 不解释，应用层按 cursorUnit 解释）。
// send 发送本地数据。close 通过 RPC（video.closeStream）或连接断开清理。

import type { Bytes } from "../../io/bytes.js";
import { AxtpError, ErrorCode } from "../../types/error.js";
import type { StreamContext } from "./streamRegistry.js";

export interface StreamStats {
  readonly chunks: number;
  readonly bytes: number;
}

type ChunkListener = (data: Bytes, cursor: bigint) => void;
type CloseListener = (reason?: string) => void;

export class Stream {
  private chunkListeners = new Set<ChunkListener>();
  private closeListeners = new Set<CloseListener>();
  private closed = false;

  constructor(
    private readonly ctx: StreamContext,
    private readonly sendFn: (streamId: number, data: Bytes, seqId: number) => void,
    private readonly closeFn: (streamId: number) => void
  ) {
    // 绑定到 StreamContext 的 handler，把入站数据转发给本 Stream 的监听器。
    ctx.handler = {
      onChunk: (data, cursor) => {
        for (const listener of this.chunkListeners) {
          try {
            listener(data, cursor);
          } catch {
            // 单个监听器抛错不影响其它
          }
        }
      },
      onClose: (reason) => {
        if (this.closed) return;
        this.closed = true;
        for (const listener of this.closeListeners) {
          try {
            listener(reason);
          } catch {
            // 忽略
          }
        }
      }
    };
  }

  /** 接收对端数据。cursor 透传（按 StreamContext/cursorUnit 解释）。 */
  onChunk(listener: ChunkListener): () => void {
    this.chunkListeners.add(listener);
    return () => this.chunkListeners.delete(listener);
  }

  /** 流关闭通知（对端关闭 / 连接断开 / 主动 close）。 */
  onClose(listener: CloseListener): () => void {
    this.closeListeners.add(listener);
    return () => this.closeListeners.delete(listener);
  }

  /** 发送数据（出站，双向）。 */
  send(data: Bytes): void {
    if (this.closed) throw new AxtpError(ErrorCode.StreamClosed, "stream closed");
    const seqId = this.ctx.nextLocalSeq;
    this.ctx.nextLocalSeq = (this.ctx.nextLocalSeq + 1) >>> 0;
    this.sendFn(this.ctx.streamId, data, seqId);
  }

  /** 主动关闭（会触发 onClose）。 */
  close(): void {
    if (this.closed) return;
    this.closeFn(this.ctx.streamId);
  }

  get streamId(): number {
    return this.ctx.streamId;
  }

  get isClosed(): boolean {
    return this.closed;
  }

  get stats(): StreamStats {
    return { chunks: this.ctx.chunks, bytes: this.ctx.bytes };
  }
}
