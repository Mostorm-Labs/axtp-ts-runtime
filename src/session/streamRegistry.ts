// StreamRegistry：STREAM P0 路由（会话语义，归 Session）。
// streamId -> StreamContext 路由。Core 不做严格 seq 校验（profile-specific，spec:251）。
// cursor 透传（Core 不解释，由 Stream Context/cursorUnit 解释）。
// teardown：abortAll 在断连/重连时释放所有 StreamContext（spec:253 MUST）。
// streamId 由 openStream 响应方(server)分配（非零 uint32）。

import type { Bytes } from "../io/bytes.js";
import type { StreamPayload } from "../protocol/model.js";
import { AxtpError, ErrorCode } from "../types/error.js";

export interface StreamChunkHandler {
  onChunk(data: Bytes, cursor: bigint): void;
  onClose(reason?: string): void;
  onError(error: AxtpError): void;
}

export interface StreamContext {
  readonly streamId: number;
  /** 谁发起：本地 openStream 的发流方 / 对端 openStream 后本地 adopt 的收流方。 */
  direction: "send" | "receive";
  /** 期望的下一个 seqId（仅统计，不强制）。 */
  expectedSeq: number;
  hasSeq: boolean;
  chunks: number;
  bytes: number;
  handler: StreamChunkHandler | undefined;
  /** 本地下一个发送 seqId。 */
  nextLocalSeq: number;
  closed: boolean;
}

/** streamId 分配器：非零 uint32，避免与已知 streamId 冲突。 */
export class StreamIdAllocator {
  private next = 1;
  private readonly inUse = new Set<number>();

  allocate(): number {
    while (this.inUse.has(this.next) || this.next === 0) {
      this.next = this.next >= 0xffffffff ? 1 : this.next + 1;
    }
    const id = this.next;
    this.inUse.add(id);
    this.next = this.next >= 0xffffffff ? 1 : this.next + 1;
    return id;
  }

  release(id: number): void {
    this.inUse.delete(id);
  }

  markInUse(id: number): void {
    if (id !== 0) this.inUse.add(id);
  }
}

export class StreamRegistry {
  private readonly streams = new Map<number, StreamContext>();
  private readonly allocator = new StreamIdAllocator();

  /** 本地发起建流：分配 streamId，返回 context（send 方）。 */
  open(): StreamContext {
    const streamId = this.allocator.allocate();
    const ctx: StreamContext = {
      streamId,
      direction: "send",
      expectedSeq: 0,
      hasSeq: false,
      chunks: 0,
      bytes: 0,
      handler: undefined,
      nextLocalSeq: 0,
      closed: false
    };
    this.streams.set(streamId, ctx);
    return ctx;
  }

  /** 对端分配的 streamId，本地 adopt 建收流 context（receive 方）。 */
  adopt(streamId: number): StreamContext {
    if (streamId === 0) throw new AxtpError(ErrorCode.StreamIdInvalid, "streamId must be non-zero");
    if (this.streams.has(streamId))
      throw new AxtpError(ErrorCode.StreamAlreadyOpen, `stream ${streamId} already open`);
    this.allocator.markInUse(streamId);
    const ctx: StreamContext = {
      streamId,
      direction: "receive",
      expectedSeq: 0,
      hasSeq: false,
      chunks: 0,
      bytes: 0,
      handler: undefined,
      nextLocalSeq: 0,
      closed: false
    };
    this.streams.set(streamId, ctx);
    return ctx;
  }

  /** 入站 STREAM 数据：按 streamId 路由（Core 不丢包，仅统计 seq）。 */
  onData(payload: StreamPayload): void {
    const ctx = this.streams.get(payload.streamId);
    if (ctx === undefined) return; // 未知 stream，丢弃（不强制报错）
    if (ctx.closed) return;
    // seq 统计（不强制，profile-specific）
    if (ctx.hasSeq) {
      if (payload.seqId === ctx.expectedSeq - 1) {
        // duplicate（仅统计，不丢）
      }
    }
    ctx.hasSeq = true;
    ctx.expectedSeq = (payload.seqId + 1) >>> 0;
    ctx.chunks += 1;
    ctx.bytes += payload.data.length;
    ctx.handler?.onChunk(payload.data, payload.cursor);
  }

  /** 注册 chunk handler。 */
  setHandler(streamId: number, handler: StreamChunkHandler): void {
    const ctx = this.streams.get(streamId);
    if (ctx !== undefined) ctx.handler = handler;
  }

  /** 关闭单个流。 */
  close(streamId: number, reason?: string): void {
    const ctx = this.streams.get(streamId);
    if (ctx === undefined) return;
    ctx.closed = true;
    ctx.handler?.onClose(reason);
    this.streams.delete(streamId);
    this.allocator.release(streamId);
  }

  /** teardown：释放所有 StreamContext（spec:253 MUST，断连/重连时）。 */
  abortAll(reason = "connection closed"): void {
    for (const ctx of this.streams.values()) {
      ctx.closed = true;
      ctx.handler?.onClose(reason);
    }
    this.streams.clear();
  }

  has(streamId: number): boolean {
    return this.streams.has(streamId);
  }

  get(streamId: number): StreamContext | undefined {
    return this.streams.get(streamId);
  }

  get count(): number {
    return this.streams.size;
  }
}
