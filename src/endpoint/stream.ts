// Stream：应用层数据流句柄（双向），endpoint 层（由 StreamManager 构造）。
// onChunk 接收对端数据（cursor 透传）；send 发本地数据；close 触发清理。
// 公共类型由 sdk 层 re-export。

import type { Bytes } from "../io/bytes.js";
import { AxtpError, ErrorCode } from "../types/error.js";
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
    private readonly sendFn: (
      streamId: number,
      data: Bytes,
      seqId: number,
      cursor?: bigint
    ) => void,
    private readonly closeFn: (streamId: number) => void
  ) {
    ctx.handler = {
      onChunk: (data, cursor) => {
        for (const listener of this.chunkListeners) {
          try {
            listener(data, cursor);
          } catch {
            /* 单个监听器抛错不影响其它 */
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
            /* 忽略 */
          }
        }
      }
    };
  }

  /** 接收对端数据。cursor 透传。 */
  onChunk(listener: ChunkListener): () => void {
    this.chunkListeners.add(listener);
    return () => this.chunkListeners.delete(listener);
  }

  /** 流关闭通知。已关闭后注册会立即补发。 */
  onClose(listener: CloseListener): () => void {
    if (this.closed) {
      try {
        listener("already closed");
      } catch {
        /* 忽略 */
      }
      return () => {};
    }
    this.closeListeners.add(listener);
    return () => this.closeListeners.delete(listener);
  }

  /** 发送数据（出站，双向）。 */
  send(data: Bytes, cursor?: bigint): void {
    if (this.closed) throw new AxtpError(ErrorCode.StreamClosed, "stream closed");
    const seqId = this.ctx.nextLocalSeq;
    this.ctx.nextLocalSeq = (this.ctx.nextLocalSeq + 1) >>> 0;
    this.sendFn(this.ctx.streamId, data, seqId, cursor);
  }

  /** 主动关闭。 */
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
