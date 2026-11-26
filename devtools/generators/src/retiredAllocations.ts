import { GeneratorError } from "./errors.js";
import { normalizeId } from "./util.js";

export interface RetiredAllocation {
  kind: "method" | "event";
  id: number;
  domain: string;
  bitOffset: number;
}

function reject(file: string, message: string): never {
  throw new GeneratorError({ code: "AXTP-GEN-1004", file, field: "retiredAllocations", message });
}

export function readRetiredAllocations(raw: unknown, file: string): RetiredAllocation[] {
  if (raw === undefined) return [];
  if (!Array.isArray(raw)) reject(file, "retired allocations must be a list");
  return raw.map((item) => {
    if (item?.kind !== "method" && item?.kind !== "event") reject(file, "invalid retired allocation kind");
    const id = normalizeId(item.id, "retired allocation ID");
    if (id < 1 || id > 0xffff || typeof item.domain !== "string" || !item.domain ||
        !Number.isInteger(item.bitOffset) || item.bitOffset < 0) {
      reject(file, "invalid retired allocation ID, domain or bitOffset");
    }
    return { kind: item.kind, id, domain: item.domain, bitOffset: item.bitOffset };
  });
}

export function retiredBits(
  allocations: RetiredAllocation[] | undefined,
  kind: RetiredAllocation["kind"],
  liveIds: Iterable<number>,
  file: string
): Array<{ name: string; domain: string; bitOffset: number }> {
  const ids = new Set(liveIds);
  const bits = [];
  for (const { kind: allocationKind, id, domain, bitOffset } of allocations ?? []) {
    if (allocationKind !== kind) continue;
    if (ids.has(id)) reject(file, `duplicate or reused retired ${kind} ID: ${id}`);
    ids.add(id);
    bits.push({ name: `retired ${kind} ${id}`, domain, bitOffset });
  }
  return bits;
}
