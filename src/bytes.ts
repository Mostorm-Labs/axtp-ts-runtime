export type Bytes = Uint8Array;

const encoder = new TextEncoder();
const decoder = new TextDecoder();

export function toBytes(value: Bytes | ArrayLike<number> | string): Bytes {
  if (typeof value === "string") return encoder.encode(value);
  if (value instanceof Uint8Array) return value.slice();
  return Uint8Array.from(value);
}

export function bytesToText(bytes: Bytes): string {
  return decoder.decode(bytes);
}

export function concatBytes(chunks: readonly Bytes[]): Bytes {
  const size = chunks.reduce((total, chunk) => total + chunk.length, 0);
  const out = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.length;
  }
  return out;
}

export function bytesEqual(lhs: Bytes, rhs: Bytes): boolean {
  if (lhs.length !== rhs.length) return false;
  for (let index = 0; index < lhs.length; index += 1) {
    if (lhs[index] !== rhs[index]) return false;
  }
  return true;
}

export function hexToBytes(hex: string): Bytes {
  const normalized = hex.replace(/\s+/g, "");
  if (normalized.length % 2 !== 0) {
    throw new Error("hex string must have an even length");
  }
  const out = new Uint8Array(normalized.length / 2);
  for (let index = 0; index < out.length; index += 1) {
    out[index] = Number.parseInt(normalized.slice(index * 2, index * 2 + 2), 16);
  }
  return out;
}

export function bytesToHex(bytes: Bytes): string {
  return [...bytes].map((byte) => byte.toString(16).padStart(2, "0")).join("");
}
