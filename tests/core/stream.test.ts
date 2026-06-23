import { describe, expect, it } from "vitest";
import {
  ErrorCode,
  StreamRegistry,
  streamPayload,
  type StreamInfo,
  type StreamPayload,
  type StreamSink
} from "../../src/index.js";

class RecordingStreamSink implements StreamSink {
  readonly opened: StreamInfo[] = [];
  readonly chunks: StreamPayload[] = [];
  readonly closed: StreamInfo[] = [];

  onStreamOpened(info: StreamInfo): void {
    this.opened.push({ ...info });
  }

  onStreamChunk(_info: StreamInfo, stream: StreamPayload): void {
    this.chunks.push(stream);
  }

  onStreamClosed(info: StreamInfo): void {
    this.closed.push({ ...info });
  }
}

function chunk(streamId: number, seqId: number, cursor: bigint, bytes: number): StreamPayload {
  return streamPayload({
    streamId,
    seqId,
    cursor,
    data: Uint8Array.from({ length: bytes }, () => seqId + 1)
  });
}

describe("stream registry", () => {
  it("tracks stream lifecycle, chunk stats, and sequence anomalies", () => {
    const sink = new RecordingStreamSink();
    const registry = new StreamRegistry({ streamSink: sink });
    const info: StreamInfo = {
      streamId: 0x10,
      kind: "file",
      source: "firmware.bin",
      streamProfile: "file.transfer",
      cursorUnit: "offsetBytes",
      payloadFormat: "binary",
      metadata: { sha256: "abc" }
    };

    expect(registry.registerStream(info, { rejectDuplicateKindSource: true })).toBe(ErrorCode.Success);
    expect(registry.hasStream(0x10)).toBe(true);
    expect(registry.hasOpenStream("file", "firmware.bin")).toBe(true);
    expect(registry.activeStreamCount()).toBe(1);
    expect(sink.opened).toEqual([info]);

    expect(registry.registerStream(info, { rejectDuplicateKindSource: true })).toBe(ErrorCode.StreamAlreadyOpen);

    registry.handleStream(chunk(0x10, 0, 0n, 3));
    registry.handleStream(chunk(0x10, 2, 3n, 5));
    registry.handleStream(chunk(0x10, 2, 3n, 7));
    registry.handleStream(chunk(0x99, 0, 0n, 11));

    expect(registry.stats()).toEqual({
      chunks: 3,
      bytes: 15,
      unknownChunks: 1,
      seqGaps: 1,
      duplicateSeq: 1
    });
    expect(sink.chunks).toHaveLength(3);

    expect(registry.close(0x10)).toEqual(info);
    expect(sink.closed).toEqual([info]);
    expect(registry.activeStreamCount()).toBe(0);
    expect(registry.close(0x10)).toBeUndefined();
  });
});
