import type { Bytes } from "./bytes.js";

export interface ByteSink {
  onBytes(bytes: Bytes): void;
}

export interface ByteWriterSink {
  writeBytes(bytes: Bytes): void;
}

export class ByteWriter {
  private readonly values: number[] = [];

  writeU8(value: number): void {
    this.values.push(value & 0xff);
  }

  writeU16(value: number): void {
    this.values.push((value >> 8) & 0xff, value & 0xff);
  }

  writeU32(value: number): void {
    this.values.push((value >> 24) & 0xff, (value >> 16) & 0xff, (value >> 8) & 0xff, value & 0xff);
  }

  writeU64(value: bigint): void {
    for (let shift = 56n; shift >= 0n; shift -= 8n) {
      this.values.push(Number((value >> shift) & 0xffn));
    }
  }

  writeBytes(bytes: Bytes): void {
    for (const byte of bytes) this.values.push(byte);
  }

  bytes(): Bytes {
    return Uint8Array.from(this.values);
  }

  takeBytes(): Bytes {
    const out = this.bytes();
    this.clear();
    return out;
  }

  clear(): void {
    this.values.length = 0;
  }
}

export class ByteReader {
  private cursor = 0;

  constructor(private readonly data: Bytes) {}

  readU8(): number | undefined {
    if (!this.hasRemaining(1)) return undefined;
    return this.data[this.cursor++];
  }

  readU16(): number | undefined {
    if (!this.hasRemaining(2)) return undefined;
    const value = (this.data[this.cursor] << 8) | this.data[this.cursor + 1];
    this.cursor += 2;
    return value & 0xffff;
  }

  readU32(): number | undefined {
    if (!this.hasRemaining(4)) return undefined;
    const value =
      (this.data[this.cursor] << 24) |
      (this.data[this.cursor + 1] << 16) |
      (this.data[this.cursor + 2] << 8) |
      this.data[this.cursor + 3];
    this.cursor += 4;
    return value >>> 0;
  }

  readU64(): bigint | undefined {
    if (!this.hasRemaining(8)) return undefined;
    let value = 0n;
    for (let shift = 56n; shift >= 0n; shift -= 8n) {
      value |= BigInt(this.data[this.cursor++]) << shift;
    }
    return value;
  }

  readBytes(size: number): Bytes | undefined {
    if (size < 0) return undefined;
    if (!this.hasRemaining(size)) return undefined;
    const out = this.data.slice(this.cursor, this.cursor + size);
    this.cursor += size;
    return out;
  }

  hasRemaining(count: number): boolean {
    return count <= this.remaining();
  }

  offset(): number {
    return this.cursor;
  }

  remaining(): number {
    return this.data.length - this.cursor;
  }

  empty(): boolean {
    return this.remaining() === 0;
  }

  // ===== strict 变体：缓冲不足时抛错，返回非可选类型 =====
  // 用于已知缓冲足够的场景（如已校验长度的 header 解析），消除调用方的非空断言。

  readU8Strict(): number {
    const v = this.readU8();
    if (v === undefined) throw new Error("ByteReader: unexpected EOF reading u8");
    return v;
  }

  readU16Strict(): number {
    const v = this.readU16();
    if (v === undefined) throw new Error("ByteReader: unexpected EOF reading u16");
    return v;
  }

  readU32Strict(): number {
    const v = this.readU32();
    if (v === undefined) throw new Error("ByteReader: unexpected EOF reading u32");
    return v;
  }

  readU64Strict(): bigint {
    const v = this.readU64();
    if (v === undefined) throw new Error("ByteReader: unexpected EOF reading u64");
    return v;
  }

  readBytesStrict(size: number): Bytes {
    const v = this.readBytes(size);
    if (v === undefined) throw new Error("ByteReader: unexpected EOF reading bytes");
    return v;
  }
}

export function crc16CcittFalse(bytes: Bytes): number {
  let crc = 0xffff;
  for (const byte of bytes) {
    crc ^= byte << 8;
    for (let bit = 0; bit < 8; bit += 1) {
      crc = (crc & 0x8000) !== 0 ? ((crc << 1) ^ 0x1021) & 0xffff : (crc << 1) & 0xffff;
    }
  }
  return crc;
}
