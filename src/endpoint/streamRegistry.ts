// StreamRegistry：STREAM P0 路由（endpoint 层）。streamId -> StreamContext 路由。
// Core 不做严格 seq 校验（profile-specific，spec:251）。cursor 透传（Core 不解释）。
// teardown：abortAll 在断连/重连时释放所有 StreamContext（spec:253 MUST）。
// streamId 由 openStream 响应方(server)分配（非零 uint32）。

import type { Bytes } from "../io/bytes.js";
import type { StreamPayload } from "../protocol/model.js";
import { AxtpError, ErrorCode } from "../types/error.js";

export interface StreamChunkHandler {
  onChunk(data: Bytes, cursor: bigint): void;
  onClose(reason?: string): void;
}

export interface StreamContext {
  readonly streamId: number;
  direction: "send" | "receive";
  chunks: number;
  bytes: number;
  handler: StreamChunkHandler | undefined;
  nextLocalSeq: number;
  closed: boolean;
}

export class StreamRegistry {
  private readonly streams = new Map<number, StreamContext>();
  private nextStreamId = 1;

  /** 本地分配一个非零 streamId（server 端预分配）。 */
  allocate(): number {
    while (this.streams.has(this.nextStreamId) || this.nextStreamId === 0) {
      this.nextStreamId = this.nextStreamId >= 0xffffffff ? 1 : this.nextStreamId + 1;
    }
    const id = this.nextStreamId;
    this.nextStreamId = this.nextStreamId >= 0xffffffff ? 1 : this.nextStreamId + 1;
    return id;
  }

  /** 对端分配的 streamId，本地 adopt 建收流 context（receive 方）。 */
  adopt(streamId: number): StreamContext {
    if (streamId === 0) throw new AxtpError(ErrorCode.StreamIdInvalid, "streamId must be non-zero");
    if (this.streams.has(streamId)) {
      throw new AxtpError(ErrorCode.StreamAlreadyOpen, `stream ${streamId} already open`);
    }
    const ctx: StreamContext = {
      streamId,
      direction: "receive",
      chunks: 0,
      bytes: 0,
      handler: undefined,
      nextLocalSeq: 0,
      closed: false
    };
    this.streams.set(streamId, ctx);
    return ctx;
  }

  /** 入站 STREAM 数据：按 streamId 路由。Core 不丢包，仅统计。 */
  onData(payload: StreamPayload): void {
    const ctx = this.streams.get(payload.streamId);
    if (ctx === undefined) return;
    if (ctx.closed) return;
    ctx.chunks += 1;
    ctx.bytes += payload.data.length;
    ctx.handler?.onChunk(payload.data, payload.cursor);
  }

  close(streamId: number, reason?: string): void {
    const ctx = this.streams.get(streamId);
    if (ctx === undefined) return;
    ctx.closed = true;
    ctx.handler?.onClose(reason);
    this.streams.delete(streamId);
  }

  /** teardown：释放所有 StreamContext（spec:253 MUST，断连/重连时）。 */
  abortAll(reason = "connection closed"): void {
    for (const ctx of this.streams.values()) {
      ctx.closed = true;
      ctx.handler?.onClose(reason);
    }
    this.streams.clear();
  }
}
