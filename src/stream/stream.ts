import { ErrorCode } from "../core/protocol/generated/axtp_ids_generated.js";
import type { StreamPayload } from "../core/protocol/model/model.js";

export type StreamMetadata = Record<string, unknown>;

export interface StreamInfo {
  streamId: number;
  kind: string;
  source: string;
  streamProfile: string;
  cursorUnit: string;
  payloadFormat: string;
  metadata: StreamMetadata;
}

export interface ActiveStream {
  streamId: number;
  kind: string;
  source: string;
  streamProfile: string;
}

export interface StreamStats {
  chunks: number;
  bytes: number;
  unknownChunks: number;
  seqGaps: number;
  duplicateSeq: number;
}

export interface StreamRegisterOptions {
  rejectDuplicateKindSource?: boolean;
}

export interface StreamSink {
  onStreamOpened(info: StreamInfo): void;
  onStreamChunk(info: StreamInfo, stream: StreamPayload): void;
  onStreamClosed(info: StreamInfo): void;
}

export interface StreamCoreOptions {
  streamSink?: StreamSink;
}

interface StreamContext {
  info: StreamInfo;
  expectedSeq: number;
  hasSeq: boolean;
  chunks: number;
  bytes: number;
}

export function toHexU32(value: number): string {
  return `0x${(value >>> 0).toString(16).toUpperCase().padStart(8, "0")}`;
}

export class StreamRegistry {
  private readonly streams = new Map<number, StreamContext>();
  private readonly statsValue: StreamStats = {
    chunks: 0,
    bytes: 0,
    unknownChunks: 0,
    seqGaps: 0,
    duplicateSeq: 0
  };

  constructor(private readonly options: StreamCoreOptions = {}) {}

  static shouldLogChunkCount(count: number): boolean {
    return count <= 50 || count % 100 === 0;
  }

  hasOpenStream(kind: string, source: string): boolean {
    for (const context of this.streams.values()) {
      if (context.info.kind === kind && context.info.source === source) return true;
    }
    return false;
  }

  hasStream(streamId: number): boolean {
    return this.streams.has(streamId);
  }

  findStream(streamId: number): StreamInfo | undefined {
    const context = this.streams.get(streamId);
    return context === undefined ? undefined : cloneInfo(context.info);
  }

  registerStream(info: StreamInfo, options: StreamRegisterOptions = {}): ErrorCode {
    if (info.streamId === 0) return ErrorCode.StreamIdInvalid;
    if (info.kind.length === 0) return ErrorCode.StreamPayloadInvalid;
    if (this.streams.has(info.streamId)) return ErrorCode.StreamAlreadyOpen;
    if (options.rejectDuplicateKindSource ?? true) {
      for (const context of this.streams.values()) {
        if (context.info.kind === info.kind && context.info.source === info.source) {
          return ErrorCode.StreamAlreadyOpen;
        }
      }
    }

    const stored = cloneInfo(info);
    this.streams.set(stored.streamId, {
      info: stored,
      expectedSeq: 0,
      hasSeq: false,
      chunks: 0,
      bytes: 0
    });
    this.options.streamSink?.onStreamOpened(cloneInfo(stored));
    return ErrorCode.Success;
  }

  close(streamId: number): StreamInfo | undefined {
    const context = this.streams.get(streamId);
    if (context === undefined) return undefined;
    this.streams.delete(streamId);
    const info = cloneInfo(context.info);
    this.options.streamSink?.onStreamClosed(info);
    return info;
  }

  handleStream(stream: StreamPayload): void {
    const context = this.streams.get(stream.streamId);
    if (context === undefined) {
      this.statsValue.unknownChunks += 1;
      return;
    }

    if (context.hasSeq) {
      if (stream.seqId === context.expectedSeq - 1) {
        this.statsValue.duplicateSeq += 1;
      } else if (stream.seqId !== context.expectedSeq) {
        this.statsValue.seqGaps += 1;
      }
    }
    context.hasSeq = true;
    context.expectedSeq = (stream.seqId + 1) >>> 0;
    context.chunks += 1;
    context.bytes += stream.data.length;
    this.statsValue.chunks += 1;
    this.statsValue.bytes += stream.data.length;
    this.options.streamSink?.onStreamChunk(cloneInfo(context.info), stream);
  }

  stats(): StreamStats {
    return { ...this.statsValue };
  }

  activeStreamCount(): number {
    return this.streams.size;
  }

  activeStreamsSnapshot(): ActiveStream[] {
    return [...this.streams.values()].map((context) => ({
      streamId: context.info.streamId,
      kind: context.info.kind,
      source: context.info.source,
      streamProfile: context.info.streamProfile
    }));
  }
}

function cloneInfo(info: StreamInfo): StreamInfo {
  return {
    ...info,
    metadata: { ...info.metadata }
  };
}
