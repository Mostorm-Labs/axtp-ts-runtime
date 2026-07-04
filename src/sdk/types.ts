// sdk/types.ts：SDK 公共类型（CallContext / CallOptions / Stream 重导出）。

export type { CallContext } from "../broker/context.js";
export type { Stream } from "../endpoint/stream.js";

export type CallOfflinePolicy = "fail-fast" | "wait-ready" | "queue";
export type QueuedCallCoalescePrevious = "resolve-with-next" | "reject" | "drop";

export interface CallOptions {
  timeoutMs?: number;
  offlinePolicy?: CallOfflinePolicy;
  coalesceKey?: string;
  coalescePrevious?: QueuedCallCoalescePrevious;
}
