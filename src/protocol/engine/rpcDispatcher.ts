// RpcDispatcher：request id -> Promise resolver 表。
// 事件驱动：响应到达即 resolve（无需 poll）。原子 request()：建表+发送+等待+超时清理一气呵成。
// 断连/超时：rejectAll 让所有 pending call 失败（ConnectionClosed）。
// requestId 为 uint32，从 1 递增、回绕。

import { ErrorCode } from "../../protocol/generated/axtp_ids_generated.js";
import { AxtpError } from "../../types/error.js";
import type { RpcPayload } from "../model.js";

interface PendingEntry {
  readonly resolve: (payload: RpcPayload) => void;
  readonly reject: (error: AxtpError) => void;
  readonly timer: ReturnType<typeof setTimeout>;
}

export interface RequestResult {
  readonly requestId: number;
  readonly promise: Promise<RpcPayload>;
}

const kMaxRequestId = 0xffffffff;

export class RpcDispatcher {
  private readonly pending = new Map<number, PendingEntry>();
  private nextRequestId = 1;

  /**
   * 原子发起请求：分配 requestId、建表、调用 send、返回 Promise。
   * 超时自动 reject 并清理。调用方在响应到达时调 resolve()。
   */
  request(
    send: (requestId: number) => void,
    timeoutMs: number,
    onTimeout?: () => void
  ): RequestResult {
    const requestId = this.allocateRequestId();
    let entry: PendingEntry;
    const promise = new Promise<RpcPayload>((resolve, reject) => {
      const timer = setTimeout(() => {
        if (this.pending.has(requestId)) {
          this.pending.delete(requestId);
          reject(
            new AxtpError(
              ErrorCode.RpcResponseTimeout,
              `request ${requestId} timed out`,
              undefined,
              requestId
            )
          );
          onTimeout?.();
        }
      }, timeoutMs);
      entry = { resolve, reject, timer };
      this.pending.set(requestId, entry);
    });
    // 发送必须在建表之后（避免响应早于建表到达）。
    try {
      send(requestId);
    } catch (err) {
      this.cancel(requestId);
      throw err;
    }
    return { requestId, promise };
  }

  /** 响应到达：匹配 requestId 并 resolve。 */
  resolve(payload: RpcPayload): boolean {
    const entry = this.pending.get(payload.requestId);
    if (entry === undefined) return false;
    clearTimeout(entry.timer);
    this.pending.delete(payload.requestId);
    entry.resolve(payload);
    return true;
  }

  /** 主动取消单个请求。 */
  cancel(requestId: number): void {
    const entry = this.pending.get(requestId);
    if (entry === undefined) return;
    clearTimeout(entry.timer);
    this.pending.delete(requestId);
  }

  /** 断连/关闭：reject 所有 pending（ConnectionClosed）。 */
  rejectAll(error: AxtpError): void {
    for (const entry of this.pending.values()) {
      clearTimeout(entry.timer);
      entry.reject(error);
    }
    this.pending.clear();
  }

  /** 当前 pending 数量。 */
  size(): number {
    return this.pending.size;
  }

  hasPending(requestId: number): boolean {
    return this.pending.has(requestId);
  }

  private allocateRequestId(): number {
    const id = this.nextRequestId;
    this.nextRequestId = this.nextRequestId >= kMaxRequestId ? 1 : this.nextRequestId + 1;
    return id;
  }
}
