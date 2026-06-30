// StreamManager：STREAM 数据流管理（单一职责：建流/数据/teardown）。
// 持有 StreamRegistry（streamId 路由 + abortAll）。
// openStream 走 RPC（video.openStream），onStream 注册建流 handler。
// 通过 SessionIO 发送 STREAM 数据帧。

import type { StreamPayload } from "../../protocol/model.js";
import { AxtpError, ErrorCode } from "../../types/error.js";
import type { SessionIO } from "../types.js";
import { Stream } from "./stream.js";
import { StreamRegistry, type StreamContext } from "./streamRegistry.js";

export class StreamManager {
  private readonly registry = new StreamRegistry();

  constructor(private readonly io: SessionIO) {}

  /** 入站 STREAM 数据：按 streamId 路由。 */
  onData(payload: StreamPayload): void {
    this.registry.onData(payload);
  }

  /** 发起建流：调用 openStream RPC 拿 streamId，本地建 send 方 Stream。 */
  async openStream(
    callMethod: (method: string, params: unknown) => Promise<unknown>,
    method: string,
    params: unknown
  ): Promise<{ streamId: number; response: unknown; stream: Stream }> {
    const result = await callMethod(method, params);
    const streamId = this.extractStreamId(result, "openStream response missing streamId");
    const sendCtx = this.registry.adopt(streamId);
    sendCtx.direction = "send";
    const stream = this.makeStream(sendCtx);
    return { streamId, response: result, stream };
  }

  /**
   * 包装建流 handler：handler 接收 Stream（receive 方），可 send/onChunk。
   * handler 返回 result（含 streamId），StreamManager 据此注册 receive context。
   */
  wrapStreamHandler(
    handler: (params: unknown, stream: Stream) => Promise<unknown> | unknown
  ): (ctx: unknown, params: unknown) => Promise<{ result: unknown; stream: Stream }> {
    return async (_ctx, params) => {
      // 预分配 streamId，建 receive context + Stream，传给 handler
      const streamId = this.registry.allocate();
      const recvCtx = this.registry.adopt(streamId);
      recvCtx.direction = "receive";
      const stream = this.makeStream(recvCtx);
      try {
        const result = await handler(params, stream);
        // 校验 handler 返回的 result 必须含匹配的 streamId
        const resultStreamId = this.extractStreamId(result, "onStream handler must return streamId");
        if (resultStreamId !== streamId) {
          throw new AxtpError(ErrorCode.StreamIdInvalid, "onStream handler streamId mismatch");
        }
        return { result, stream };
      } catch (err) {
        // handler 抛错 / result 缺 streamId / streamId 不匹配：释放已 adopt 的 context，避免泄漏。
        this.registry.close(streamId, "onStream handler error");
        throw err;
      }
    };
  }

  /** 从 RPC result 提取 streamId 并校验。 */
  private extractStreamId(result: unknown, msg: string): number {
    const streamId = (result as { streamId?: number }).streamId;
    if (typeof streamId !== "number" || streamId === 0) {
      throw new AxtpError(ErrorCode.StreamIdInvalid, msg);
    }
    return streamId;
  }

  /** 构造 Stream 对象。 */
  private makeStream(ctx: StreamContext): Stream {
    return new Stream(
      ctx,
      (streamId, data, seqId, cursor) => {
        this.io.sendStream(streamId, data, seqId, cursor);
      },
      (streamId) => this.registry.close(streamId, "local close")
    );
  }

  /** teardown：释放所有 StreamContext。 */
  abortAll(reason: string): void {
    this.registry.abortAll(reason);
  }
}
