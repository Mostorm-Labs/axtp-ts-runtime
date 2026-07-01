// 测试共享 helper：EventStream 的 once（首事件 Promise）。
import type { EventStream } from "../../src/types/events.js";

/** 订阅 EventStream，resolve 到首个 emit 值（用于 await ready/connect 等）。 */
export function once<T>(stream: EventStream<T>): Promise<T> {
  return new Promise<T>((resolve) => stream.subscribe((v) => resolve(v)));
}
