// StreamManager：STREAM 数据流管理（单一职责：建流/数据/teardown）。
// 持有 StreamRegistry（streamId 路由 + abortAll）。
// openStream 走 RPC（video.openStream），onStream 注册建流 handler。
// 通过 SessionIO 发送 STREAM 数据帧。

import { ErrorCode } from "../../protocol/generated/axtp_ids_generated.js";
import type { StreamPayload } from "../../protocol/model.js";
import { AxtpError } from "../../types/error.js";
import type { SessionIO } from "../handshake/handshakeOrchestrator.js";
import { Stream } from "./stream.js";
import { StreamRegistry, type StreamContext } from "./streamRegistry.js";

export class StreamManager {
  readonly registry = new StreamRegistry();

  constructor(private readonly io: SessionIO & { sendStream: (p: StreamPayload) => void }) {}

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
    const streamId = (result as { streamId?: number }).streamId;
    if (typeof streamId !== "number" || streamId === 0) {
      throw new AxtpError(ErrorCode.StreamIdInvalid, "openStream response missing streamId");
    }
    const sendCtx = this.registry.adopt(streamId);
    sendCtx.direction = "send";
    const stream = this.makeStream(sendCtx);
    return { streamId, response: result, stream };
  }

  /** 注册建流 handler（server 端）。返回的 result 含 streamId，handler 用它建 receive context。 */
  /**
   * 包装建流 handler：handler 执行返回 result（含 streamId），
   * 建 receive context + Stream，通过 onStreamCreated 回调把 Stream 交给调用方。
   */
  wrapStreamHandler(
    handler: (params: unknown) => Promise<unknown> | unknown,
    onStreamCreated: (stream: Stream) => void
  ): (ctx: unknown, params: unknown) => Promise<unknown> {
    return async (_ctx, params) => {
      const result = await handler(params);
      const streamId = (result as { streamId?: number }).streamId;
      if (typeof streamId !== "number" || streamId === 0) {
        throw new AxtpError(ErrorCode.StreamIdInvalid, "onStream handler must return streamId");
      }
      const recvCtx = this.registry.adopt(streamId);
      recvCtx.direction = "receive";
      const stream = this.makeStream(recvCtx);
      onStreamCreated(stream);
      return result;
    };
  }

  /** 构造 Stream 对象。 */
  private makeStream(ctx: StreamContext): Stream {
    return new Stream(
      ctx,
      (streamId, data, seqId) => {
        this.io.sendStream({ streamId, seqId, cursor: 0n, data });
      },
      (streamId) => this.registry.close(streamId, "local close")
    );
  }

  /** teardown：释放所有 StreamContext。 */
  abortAll(reason: string): void {
    this.registry.abortAll(reason);
  }
}
