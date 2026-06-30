// codec 健壮性单测：A1/A2/A3/A4（帧错误上报、assembly timeout、streamId 校验、ByteReader size 校验）。
import { describe, expect, it, vi } from "vitest";
import { ByteReader } from "../../src/io/io.js";
import {
  FrameDecoder,
  FrameEncoder,
  MessageFragmenter,
  MessageReassembler
} from "../../src/protocol/codec/frame.js";
import { decodeStream } from "../../src/protocol/codec/stream.js";
import type { Frame } from "../../src/protocol/model.js";
import { PayloadType } from "../../src/protocol/model.js";
import { ErrorCode, type AxtpError } from "../../src/types/error.js";

/** 编一个合法的 RPC 帧（body=[1,2,3]），返回 wire 字节。 */
function encodeGoodFrame(): Uint8Array {
  const frames = new MessageFragmenter(4096).fragment({
    messageId: 0,
    payloadType: PayloadType.Rpc,
    body: new Uint8Array([1, 2, 3])
  });
  return new FrameEncoder().encode(frames[0]);
}

/** 收集 onError 的 sink。 */
function errorSink(): {
  onFrame: (f: Frame) => void;
  onError: (e: AxtpError) => void;
  errors: AxtpError[];
} {
  const errors: AxtpError[] = [];
  return { onFrame: () => {}, onError: (e) => errors.push(e), errors };
}

// ===== A3: STREAM streamId 非零校验 =====
describe("A3: decodeStream streamId 校验", () => {
  it("streamId=0 返回 undefined（spec:251 MUST 校验 streamId != 0）", () => {
    expect(decodeStream(new Uint8Array(16))).toBeUndefined();
  });

  it("streamId!=0 正常解码", () => {
    const body = new Uint8Array(16);
    body[3] = 1; // streamId = 1 (big-endian)
    expect(decodeStream(body)?.streamId).toBe(1);
  });

  it("header 不足 16B 返回 undefined", () => {
    expect(decodeStream(new Uint8Array(10))).toBeUndefined();
  });
});

// ===== A4: ByteReader 负 size 防御 =====
describe("A4: ByteReader size 校验", () => {
  it("readBytes 负 size 返回 undefined", () => {
    expect(new ByteReader(new Uint8Array([1, 2, 3])).readBytes(-1)).toBeUndefined();
  });

  it("readBytesStrict 负 size 抛错", () => {
    expect(() => new ByteReader(new Uint8Array([1])).readBytesStrict(-1)).toThrow();
  });
});

// ===== A1: FrameDecoder 校验失败上报 onError =====
describe("A1: FrameDecoder 校验失败上报", () => {
  it("坏 version -> FrameVersionUnsupported（先于 CRC 检查）", () => {
    const sink = errorSink();
    const bytes = encodeGoodFrame();
    bytes[2] = 0x02; // version 字节
    new FrameDecoder(sink, 4096).onBytes(bytes);
    expect(sink.errors.some((e) => e.code === ErrorCode.FrameVersionUnsupported)).toBe(true);
  });

  it("坏 payloadType -> FramePayloadTypeInvalid", () => {
    const sink = errorSink();
    const bytes = encodeGoodFrame();
    bytes[3] = 0x09; // 非法 payloadType
    new FrameDecoder(sink, 4096).onBytes(bytes);
    expect(sink.errors.some((e) => e.code === ErrorCode.FramePayloadTypeInvalid)).toBe(true);
  });

  it("坏 CRC -> FrameCrcError（header 合法、payload 被改）", () => {
    const sink = errorSink();
    const bytes = encodeGoodFrame();
    bytes[12] ^= 0xff; // payload 首字节（header 12B 不变，CRC 失配）
    new FrameDecoder(sink, 4096).onBytes(bytes);
    expect(sink.errors.some((e) => e.code === ErrorCode.FrameCrcError)).toBe(true);
  });

  it("payloadLength 超限 -> FrameTooLarge", () => {
    const sink = errorSink();
    // maxFrameSize=14（仅 header+CRC，无 payload 容量）：payloadLength=3 -> 3+14>14
    new FrameDecoder(sink, 14).onBytes(encodeGoodFrame());
    expect(sink.errors.some((e) => e.code === ErrorCode.FrameTooLarge)).toBe(true);
  });

  it("正常帧不上报错误", () => {
    const sink = errorSink();
    new FrameDecoder(sink, 4096).onBytes(encodeGoodFrame());
    expect(sink.errors).toHaveLength(0);
  });
});

// ===== A2: MessageReassembler 超时上报 =====
describe("A2: MessageReassembler 超时上报 FrameReassemblyTimeout", () => {
  it("未完成 assembly 超过 timeout -> 上报", () => {
    vi.useFakeTimers();
    try {
      const sink = {
        onMessage: () => {},
        onError: (e: AxtpError) => sink.errors.push(e),
        errors: [] as AxtpError[]
      };
      // 构造参数：(next, maxMessageSize, assemblyTimeoutMs=1000)
      const reassembler = new MessageReassembler(sink, 1024 * 1024, 1000);
      const frames = new MessageFragmenter(64).fragment({
        messageId: 0,
        payloadType: PayloadType.Stream,
        body: new Uint8Array(100) // > 64，必分片（frameCount>=2）
      });
      reassembler.onFrame(frames[0]); // 只收第一片，assembly 未完成
      expect(sink.errors).toHaveLength(0);
      vi.advanceTimersByTime(1500); // 超过 timeout
      reassembler.onFrame(frames[0]); // 再次 onFrame 触发惰性 evictExpired
      expect(sink.errors.some((e) => e.code === ErrorCode.FrameReassemblyTimeout)).toBe(true);
    } finally {
      vi.useRealTimers();
    }
  });
});
