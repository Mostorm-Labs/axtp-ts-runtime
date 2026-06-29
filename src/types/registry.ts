// Layer 1 registry：运行时 name<->id 双向查找（O(1)）+ eventMasks 计算。
// 类型（MethodName/EventName 等）从 generated 的单一事实源 keyof typeof 推导。
// handler 表按 name 索引——规避 method id 与 event id 共享同一数字空间
// （0x0901 既是 audio.getAlgorithmConfig 方法又是 audio.algorithmConfigChanged 事件）。

import type { EventName, MethodName } from "../protocol/generated/registry.js";
import { EVENT_REGISTRY, METHOD_REGISTRY } from "../protocol/generated/registry.js";
import { hexToBytes } from "../io/bytes.js";

export {
  EVENT_REGISTRY,
  METHOD_REGISTRY,
  type EventId,
  type EventName,
  type EventPayload,
  type EventStatus,
  type MethodId,
  type MethodName,
  type MethodRequest,
  type MethodResponse,
  type MethodStatus
} from "../protocol/generated/registry.js";

/** 运行时 name<->id 双向查找表（O(1)）。 */
class RegistryIndex {
  private readonly methodNameToId = new Map<MethodName, number>();
  private readonly methodIdToName = new Map<number, MethodName>();
  private readonly eventNameToId = new Map<EventName, number>();
  private readonly eventIdToName = new Map<number, EventName>();

  constructor() {
    for (const name of Object.keys(METHOD_REGISTRY) as MethodName[]) {
      const entry = METHOD_REGISTRY[name];
      this.methodNameToId.set(name, entry.id);
      this.methodIdToName.set(entry.id, name);
    }
    for (const name of Object.keys(EVENT_REGISTRY) as EventName[]) {
      const entry = EVENT_REGISTRY[name];
      this.eventNameToId.set(name, entry.id);
      this.eventIdToName.set(entry.id, name);
    }
  }

  methodId(name: string): number | undefined {
    return this.methodNameToId.get(name as MethodName);
  }

  methodName(id: number): MethodName | undefined {
    return this.methodIdToName.get(id);
  }

  eventId(name: string): number | undefined {
    return this.eventNameToId.get(name as EventName);
  }

  eventName(id: number): EventName | undefined {
    return this.eventIdToName.get(id);
  }
}

/** 单例运行时索引。 */
export const registry = new RegistryIndex();

/**
 * eventMasks 编码：domainId(1B) + maskLen(1B) + bitmask(maskLen B)。
 * bit0 映射到该 domain 中 registry bitOffset=0 的 event。
 * 空/缺失 mask = 不订阅。每个 domain 一个 entry，按 domainId 排序输出。
 */
export function computeEventMasks(eventNames: readonly EventName[]): string {
  if (eventNames.length === 0) return "";
  const byDomain = new Map<number, number[]>();
  for (const name of eventNames) {
    const entry = EVENT_REGISTRY[name];
    if (entry === undefined) continue;
    const domainId = (entry.id >> 8) & 0xff;
    let bits = byDomain.get(domainId);
    if (bits === undefined) {
      bits = [];
      byDomain.set(domainId, bits);
    }
    bits.push(entry.bitOffset);
  }
  const sortedDomains = [...byDomain.keys()].sort((a, b) => a - b);
  let hex = "";
  for (const domainId of sortedDomains) {
    const bits = byDomain.get(domainId);
    if (bits === undefined) continue;
    const maxBit = Math.max(...bits);
    const maskLen = Math.max(1, Math.ceil((maxBit + 1) / 8));
    const mask = new Uint8Array(maskLen);
    for (const bit of bits) {
      const byteIndex = Math.floor(bit / 8);
      const bitIndex = bit % 8;
      mask[maskLen - 1 - byteIndex] |= 1 << bitIndex;
    }
    hex += domainId.toString(16).padStart(2, "0");
    hex += maskLen.toString(16).padStart(2, "0");
    for (const b of mask) hex += b.toString(16).padStart(2, "0");
  }
  return hex;
}

/** 判断某事件名是否被 eventMasks（hex 串）命中订阅。 */
export function isEventSubscribed(eventName: EventName, eventMasksHex: string): boolean {
  if (!eventMasksHex) return false;
  const bytes = hexToBytes(eventMasksHex);
  const entry = EVENT_REGISTRY[eventName];
  if (entry === undefined) return false;
  const domainId = (entry.id >> 8) & 0xff;
  for (let i = 0; i + 2 < bytes.length; ) {
    const dom = bytes[i++];
    const maskLen = bytes[i++];
    const mask = bytes.slice(i, i + maskLen);
    i += maskLen;
    if (dom === domainId) {
      const byteIndex = Math.floor(entry.bitOffset / 8);
      const bitIndex = entry.bitOffset % 8;
      const maskByte = mask[mask.length - 1 - byteIndex] ?? 0;
      return (maskByte & (1 << bitIndex)) !== 0;
    }
  }
  return false;
}
