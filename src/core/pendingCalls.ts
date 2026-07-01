// PendingCalls：出站 RPC 请求的 requestId→Promise 关联表（Core 的出站跟踪）。
// 事件驱动：响应到达即 resolve，无需 poll。request() 原子：分配 id + 建表 + 发送 + 超时清理。
// 断连/超时由 rejectAll 让所有 pending 失败。requestId 为 uint32，从 1 递增、回绕。

import type { ResponsePayload } from "../protocol/model.js";
import { AxtpError, ErrorCode } from "../types/error.js";

interface PendingEntry {
  readonly resolve: (payload: ResponsePayload) => void;
  readonly reject: (error: AxtpError) => void;
  readonly timer: ReturnType<typeof setTimeout>;
}

export interface RequestResult {
  readonly requestId: number;
  readonly promise: Promise<ResponsePayload>;
}

const kMaxRequestId = 0xffffffff;

export class PendingCalls {
  private readonly pending = new Map<number, PendingEntry>();
  private nextRequestId = 1;

  /**
   * 原子发起请求：分配 requestId、建 pending 表、调用 send、返回 Promise。
   * 超时自动 reject 并清理。响应到达时调 resolve()。
   * send 必须在建表之后调用（避免响应早于建表到达）；send 抛错则回滚 entry 并 rethrow。
   */
  request(send: (requestId: number) => void, timeoutMs: number): RequestResult {
    const requestId = this.allocateRequestId();
    // 先建 resolver 容器，避免依赖 Promise executor 同步执行的隐式契约。
    const resolver: { resolve: (p: ResponsePayload) => void; reject: (e: AxtpError) => void } = {
      resolve: () => {},
      reject: () => {}
    };
    const promise = new Promise<ResponsePayload>((resolve, reject) => {
      resolver.resolve = resolve;
      resolver.reject = reject;
    });
    const timer = setTimeout(() => {
      if (this.pending.has(requestId)) {
        this.pending.delete(requestId);
        resolver.reject(
          new AxtpError(
            ErrorCode.RpcResponseTimeout,
            `request ${requestId} timed out`,
            undefined,
            requestId
          )
        );
      }
    }, timeoutMs);
    this.pending.set(requestId, { resolve: resolver.resolve, reject: resolver.reject, timer });
    try {
      send(requestId);
    } catch (err) {
      this.cancel(requestId);
      throw err;
    }
    return { requestId, promise };
  }

  /** 响应到达：按 requestId 匹配并 resolve。未知 id 为 no-op。 */
  resolve(payload: ResponsePayload): void {
    const entry = this.pending.get(payload.requestId);
    if (entry === undefined) return;
    clearTimeout(entry.timer);
    this.pending.delete(payload.requestId);
    entry.resolve(payload);
  }

  /** 断连/关闭：reject 所有 pending 并清定时器。 */
  rejectAll(error: AxtpError): void {
    for (const entry of this.pending.values()) {
      clearTimeout(entry.timer);
      entry.reject(error);
    }
    this.pending.clear();
  }

  private cancel(requestId: number): void {
    const entry = this.pending.get(requestId);
    if (entry === undefined) return;
    clearTimeout(entry.timer);
    this.pending.delete(requestId);
  }

  private allocateRequestId(): number {
    const id = this.nextRequestId;
    this.nextRequestId = this.nextRequestId >= kMaxRequestId ? 1 : this.nextRequestId + 1;
    return id;
  }
}
