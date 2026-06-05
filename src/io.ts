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
    this.values.push(value & 0xff, (value >> 8) & 0xff);
  }

  writeU32(value: number): void {
    this.values.push(value & 0xff, (value >> 8) & 0xff, (value >> 16) & 0xff, (value >> 24) & 0xff);
  }

  writeU64(value: bigint): void {
    for (let shift = 0n; shift < 64n; shift += 8n) {
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
    const value = this.data[this.cursor] | (this.data[this.cursor + 1] << 8);
    this.cursor += 2;
    return value;
  }

  readU32(): number | undefined {
    if (!this.hasRemaining(4)) return undefined;
    const value =
      this.data[this.cursor] |
      (this.data[this.cursor + 1] << 8) |
      (this.data[this.cursor + 2] << 16) |
      (this.data[this.cursor + 3] << 24);
    this.cursor += 4;
    return value >>> 0;
  }

  readU64(): bigint | undefined {
    if (!this.hasRemaining(8)) return undefined;
    let value = 0n;
    for (let shift = 0n; shift < 64n; shift += 8n) {
      value |= BigInt(this.data[this.cursor++]) << shift;
    }
    return value;
  }

  readBytes(size: number): Bytes | undefined {
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
