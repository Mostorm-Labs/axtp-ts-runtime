// StreamManager：STREAM 数据流管理（endpoint 层，建流/数据/teardown）。
// 持 StreamRegistry（streamId 路由 + abortAll）。openStream 走 RPC（video.openStream）。
// 出站 STREAM 数据经 sendStream 回调（Endpoint 绑定 core.sendStream），取代旧 SessionIO。

import type { StreamPayload } from "../protocol/model.js";
import { AxtpError, ErrorCode } from "../types/error.js";
import { Stream } from "./stream.js";
import { StreamRegistry, type StreamContext } from "./streamRegistry.js";

export class StreamManager {
  private readonly registry = new StreamRegistry();

  constructor(private readonly sendStream: (payload: StreamPayload) => void) {}

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

  /** 包装建流 handler：handler 接收 Stream（receive 方），可 send/onChunk；返回含 streamId 的 result。 */
  wrapStreamHandler(
    handler: (params: unknown, stream: Stream) => Promise<unknown> | unknown
  ): (ctx: unknown, params: unknown) => Promise<unknown> {
    return async (_ctx, params) => {
      const streamId = this.registry.allocate();
      const recvCtx = this.registry.adopt(streamId);
      recvCtx.direction = "receive";
      const stream = this.makeStream(recvCtx);
      try {
        const result = await handler(params, stream);
        const resultStreamId = this.extractStreamId(
          result,
          "onStream handler must return streamId"
        );
        if (resultStreamId !== streamId) {
          throw new AxtpError(ErrorCode.StreamIdInvalid, "onStream handler streamId mismatch");
        }
        return result;
      } catch (err) {
        this.registry.close(streamId, "onStream handler error");
        throw err;
      }
    };
  }

  private extractStreamId(result: unknown, msg: string): number {
    const streamId = (result as { streamId?: number }).streamId;
    if (typeof streamId !== "number" || streamId === 0) {
      throw new AxtpError(ErrorCode.StreamIdInvalid, msg);
    }
    return streamId;
  }

  private makeStream(ctx: StreamContext): Stream {
    return new Stream(
      ctx,
      (streamId, data, seqId, cursor) =>
        this.sendStream({ streamId, seqId, cursor: cursor ?? 0n, data }),
      (streamId) => this.registry.close(streamId, "local close")
    );
  }

  /** teardown：释放所有 StreamContext（断连/关闭时）。 */
  abortAll(reason: string): void {
    this.registry.abortAll(reason);
  }
}
