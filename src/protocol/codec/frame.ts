// Frame codec：Standard Frame（12B header + payload + 2B CRC16-CCITT-FALSE）。
// 入站：FrameDecoder（magic resync + 8 项校验）-> MessageReassembler（按 messageId 重组分片）。
// 出站：MessageFragmenter（按 maxFrameSize 分片）-> FrameEncoder。
//
// spec 20-core.md:50 分发前 MUST 校验 8 项：
// magic / version / payloadType / PayloadLength+14<=maxFrameSize / FrameCount>=1 /
// FrameIndex<FrameCount / CRC16 / 完整 payload。
// MessageId 严禁用于 RPC 匹配或 STREAM 排序（spec:52）。

import { concatBytes, type Bytes } from "../../io/bytes.js";
import { ByteReader, ByteWriter, crc16CcittFalse } from "../../io/io.js";
import { PayloadType } from "../../protocol/generated/axtp_ids_generated.js";
import type { Frame, Message } from "../model.js";

export const kMagic0 = 0x41;
export const kMagic1 = 0x58;
export const kAxtpVersion1 = 0x01;
export const kStandardFrameHeaderSize = 12;
export const kStandardFrameCrcSize = 2;

function isPayloadType(value: number): value is PayloadType {
  return value === PayloadType.Control || value === PayloadType.Rpc || value === PayloadType.Stream;
}

/** 入站第一级：字节流 -> Frame（magic resync + 校验）。 */
export class FrameDecoder {
  private buffer: Bytes = new Uint8Array();

  constructor(
    private readonly next: { onFrame(frame: Frame): void },
    private maxFrameSizeValue: number = 4096
  ) {}

  onBytes(bytes: Bytes): void {
    this.buffer = concatBytes([this.buffer, bytes]);
    this.parseLoop();
  }

  setMaxFrameSize(size: number): void {
    this.maxFrameSizeValue = size;
  }

  private consume(count: number): void {
    this.buffer = this.buffer.slice(count);
  }

  /** magic resync：丢弃直到找到 0x41 0x58。 */
  private resyncToMagic(): void {
    for (let i = 0; i + 1 < this.buffer.length; i += 1) {
      if (this.buffer[i] === kMagic0 && this.buffer[i + 1] === kMagic1) {
        if (i > 0) this.consume(i);
        return;
      }
    }
    if (this.buffer.length === 0) return;
    // 保留可能作为 magic 首字节的尾部 1 字节
    const keep = this.buffer[this.buffer.length - 1] === kMagic0 ? 1 : 0;
    this.consume(this.buffer.length - keep);
  }

  private parseLoop(): void {
    while (true) {
      this.resyncToMagic();
      if (this.buffer.length < kStandardFrameHeaderSize + kStandardFrameCrcSize) return;

      const headerBytes = this.buffer.slice(0, kStandardFrameHeaderSize);
      const reader = new ByteReader(headerBytes);
      reader.readU8(); // magic0
      reader.readU8(); // magic1
      const version = reader.readU8()!;
      const payloadType = reader.readU8()!;
      const payloadLength = reader.readU16()!;
      const sourceId = reader.readU8()!;
      const destinationId = reader.readU8()!;
      const messageId = reader.readU16()!;
      const frameIndex = reader.readU8()!;
      const frameCount = reader.readU8()!;

      // 校验 ②version ③payloadType ⑤FrameCount ⑥FrameIndex ④PayloadLength+14<=maxFrameSize
      if (
        version !== kAxtpVersion1 ||
        !isPayloadType(payloadType) ||
        frameCount === 0 ||
        frameIndex >= frameCount
      ) {
        this.consume(1);
        continue;
      }
      if (
        payloadLength + kStandardFrameHeaderSize + kStandardFrameCrcSize >
        this.maxFrameSizeValue
      ) {
        this.consume(1);
        continue;
      }

      const totalSize = kStandardFrameHeaderSize + payloadLength + kStandardFrameCrcSize;
      if (this.buffer.length < totalSize) return; // ⑧完整 payload 尚未到达

      const frameBytes = this.buffer.slice(0, totalSize);
      const footerReader = new ByteReader(frameBytes.slice(totalSize - kStandardFrameCrcSize));
      const expectedCrc = footerReader.readU16()!;
      // ⑦CRC16-CCITT-FALSE 覆盖 header+payload，不含 CRC 自身
      const actualCrc = crc16CcittFalse(frameBytes.slice(0, totalSize - kStandardFrameCrcSize));
      if (expectedCrc !== actualCrc) {
        this.consume(1);
        continue;
      }

      this.consume(totalSize);
      this.next.onFrame({
        header: {
          version,
          payloadType,
          payloadLength,
          sourceId,
          destinationId,
          messageId,
          frameIndex,
          frameCount
        },
        payload: frameBytes.slice(
          kStandardFrameHeaderSize,
          kStandardFrameHeaderSize + payloadLength
        ),
        crc16: expectedCrc
      });
    }
  }
}

/** 入站第二级：按 messageId 重组分片 message。 */
export class MessageReassembler {
  private readonly assemblies = new Map<
    number,
    {
      payloadType: PayloadType;
      frameCount: number;
      totalSize: number;
      fragments: Array<Bytes | undefined>;
    }
  >();

  constructor(
    private readonly next: { onMessage(message: Message): void },
    private readonly maxMessageSize: number = 1024 * 1024
  ) {}

  onFrame(frame: Frame): void {
    if (frame.header.frameCount === 1) {
      if (frame.header.frameIndex !== 0) return;
      this.next.onMessage({
        messageId: frame.header.messageId,
        payloadType: frame.header.payloadType,
        body: frame.payload
      });
      return;
    }

    let assembly = this.assemblies.get(frame.header.messageId);
    if (assembly === undefined) {
      assembly = {
        payloadType: frame.header.payloadType,
        frameCount: frame.header.frameCount,
        totalSize: 0,
        fragments: new Array(frame.header.frameCount).fill(undefined)
      };
      this.assemblies.set(frame.header.messageId, assembly);
    }

    if (
      assembly.payloadType !== frame.header.payloadType ||
      assembly.frameCount !== frame.header.frameCount ||
      frame.header.frameIndex >= assembly.fragments.length
    ) {
      this.assemblies.delete(frame.header.messageId);
      return;
    }

    if (assembly.fragments[frame.header.frameIndex] !== undefined) return;
    assembly.totalSize += frame.payload.length;
    if (assembly.totalSize > this.maxMessageSize) {
      this.assemblies.delete(frame.header.messageId);
      return;
    }
    assembly.fragments[frame.header.frameIndex] = frame.payload;

    if (assembly.fragments.some((f) => f === undefined)) return;
    this.assemblies.delete(frame.header.messageId);
    this.next.onMessage({
      messageId: frame.header.messageId,
      payloadType: assembly.payloadType,
      body: concatBytes(assembly.fragments as Bytes[])
    });
  }
}

/** 出站第一级：message -> frames（按 maxFrameSize 分片）。 */
export class MessageFragmenter {
  private nextMessageId = 1;

  constructor(private maxFrameSize: number = 4096) {}

  setMaxFrameSize(size: number): void {
    this.maxFrameSize = size;
  }

  getMaxFrameSize(): number {
    return this.maxFrameSize;
  }

  fragment(message: Message): Frame[] {
    const capacity = this.payloadCapacity();
    const messageId = this.takeMessageId();
    if (capacity === 0 || message.body.length === 0) {
      return [this.makeFrame(message, messageId, 0, 1, new Uint8Array())];
    }
    const frameCount = Math.ceil(message.body.length / capacity);
    if (frameCount > 255) {
      throw new Error("AXTP message requires more than 255 fragments");
    }
    const frames: Frame[] = [];
    for (let i = 0; i < frameCount; i += 1) {
      const offset = i * capacity;
      const payload = message.body.slice(offset, Math.min(message.body.length, offset + capacity));
      frames.push(this.makeFrame(message, messageId, i, frameCount, payload));
    }
    return frames;
  }

  private payloadCapacity(): number {
    if (this.maxFrameSize <= kStandardFrameHeaderSize + kStandardFrameCrcSize) return 0;
    return this.maxFrameSize - kStandardFrameHeaderSize - kStandardFrameCrcSize;
  }

  private takeMessageId(): number {
    const id = this.nextMessageId;
    this.nextMessageId += 1;
    if (this.nextMessageId > 0xffff) this.nextMessageId = 1;
    return id;
  }

  private makeFrame(
    message: Message,
    messageId: number,
    frameIndex: number,
    frameCount: number,
    payload: Bytes
  ): Frame {
    return {
      header: {
        version: kAxtpVersion1,
        payloadType: message.payloadType,
        payloadLength: payload.length,
        sourceId: 0,
        destinationId: 0,
        messageId,
        frameIndex,
        frameCount
      },
      payload,
      crc16: 0
    };
  }
}

/** 出站第二级：frame -> 字节（12B header + payload + 2B CRC）。 */
export class FrameEncoder {
  encode(frame: Frame): Bytes {
    const writer = new ByteWriter();
    writer.writeU8(kMagic0);
    writer.writeU8(kMagic1);
    writer.writeU8(frame.header.version);
    writer.writeU8(frame.header.payloadType);
    writer.writeU16(frame.payload.length);
    writer.writeU8(frame.header.sourceId);
    writer.writeU8(frame.header.destinationId);
    writer.writeU16(frame.header.messageId);
    writer.writeU8(frame.header.frameIndex);
    writer.writeU8(frame.header.frameCount);
    writer.writeBytes(frame.payload);
    const partial = writer.bytes();
    writer.writeU16(crc16CcittFalse(partial));
    return writer.takeBytes();
  }
}

/** 便捷：把一个完整 message 编码为单个 frame 的字节（未分片）。 */
export function encodeSingleFrame(message: Message): Bytes {
  const fragmenter = new MessageFragmenter();
  return new FrameEncoder().encode(fragmenter.fragment(message)[0]);
}
