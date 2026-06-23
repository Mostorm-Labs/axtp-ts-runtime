import { bytesToText, toBytes } from "../../core/support/io/bytes.js";
import {
  ErrorCode,
  MethodId,
  RpcEncoding
} from "../../core/protocol/generated/axtp_ids_generated.js";
import type { StreamPayload } from "../../core/protocol/model/model.js";
import {
  BasicBroker,
  BrokerResult,
  type RawRpcHandler,
  type RpcRequestView,
  type RpcResponseData
} from "../../core/runtime/broker/broker.js";
import {
  StreamRegistry,
  type ActiveStream,
  type StreamInfo,
  type StreamSink
} from "../../stream/stream.js";

export enum MediaKind {
  Video = "video",
  Audio = "audio"
}

export enum OpenMode {
  ReceiverPull = "receiver-pull",
  ProducerOpen = "producer-open",
  Both = "both"
}

export interface MediaHostOptions {
  acceptVideo?: boolean;
  acceptAudio?: boolean;
  openMode?: OpenMode;
  source?: string;
  audioFormat?: string;
  audioSampleRate?: number;
  audioChannels?: number;
  streamSink?: MediaStreamSink;
}

export interface MediaStreamStats {
  videoChunks: number;
  audioChunks: number;
  videoBytes: number;
  audioBytes: number;
  unknownChunks: number;
  seqGaps: number;
  duplicateSeq: number;
}

export interface MediaStreamInfo {
  kind: MediaKind;
  streamId: number;
  source: string;
  codec: string;
  streamProfile: string;
  cursorUnit: string;
  width: number;
  height: number;
  sampleRate: number;
  channels: number;
  metadata: Record<string, unknown>;
}

export interface ActiveMediaStream {
  kind: MediaKind;
  streamId: number;
  source: string;
}

export interface MediaStreamSink {
  onStreamOpened(info: MediaStreamInfo): void;
  onStreamChunk(kind: MediaKind, stream: StreamPayload): void;
  onStreamClosed(kind: MediaKind, streamId: number): void;
}

export interface OpenStreamResult {
  status: ErrorCode;
  body: Record<string, unknown>;
}

export function receiverPullEnabled(mode: OpenMode): boolean {
  return mode === OpenMode.ReceiverPull || mode === OpenMode.Both;
}

export function producerOpenEnabled(mode: OpenMode): boolean {
  return mode === OpenMode.ProducerOpen || mode === OpenMode.Both;
}

export class MediaStreamRegistry implements StreamSink {
  private readonly options: Required<Omit<MediaHostOptions, "streamSink">> & Pick<MediaHostOptions, "streamSink">;
  private readonly streams = new StreamRegistry({ streamSink: this });
  private readonly mediaStats: MediaStreamStats = {
    videoChunks: 0,
    audioChunks: 0,
    videoBytes: 0,
    audioBytes: 0,
    unknownChunks: 0,
    seqGaps: 0,
    duplicateSeq: 0
  };
  private nextVideoStreamId = 0x1001;
  private nextAudioStreamId = 0x2001;

  constructor(options: MediaHostOptions = {}) {
    this.options = {
      acceptVideo: options.acceptVideo ?? true,
      acceptAudio: options.acceptAudio ?? true,
      openMode: options.openMode ?? OpenMode.ReceiverPull,
      source: options.source ?? "wireless_cast",
      audioFormat: options.audioFormat ?? "adts",
      audioSampleRate: options.audioSampleRate ?? 48000,
      audioChannels: options.audioChannels ?? 1,
      streamSink: options.streamSink
    };
  }

  openMode(): OpenMode {
    return this.options.openMode;
  }

  receiverPullEnabled(): boolean {
    return receiverPullEnabled(this.options.openMode);
  }

  producerOpenEnabled(): boolean {
    return producerOpenEnabled(this.options.openMode);
  }

  mediaEnabled(kind: MediaKind): boolean {
    return kind === MediaKind.Video ? this.options.acceptVideo : this.options.acceptAudio;
  }

  sourceFor(kind: MediaKind): string {
    if (this.options.source.length === 0 || this.options.source === "wireless_cast") {
      return kind === MediaKind.Video ? "wireless_cast_video" : "wireless_cast_audio";
    }
    return this.options.source;
  }

  audioSampleRate(): number {
    return this.options.audioSampleRate === 0 ? 48000 : this.options.audioSampleRate;
  }

  audioChannels(): number {
    return this.options.audioChannels === 0 ? 1 : this.options.audioChannels;
  }

  static kindName(kind: MediaKind): string {
    return kind;
  }

  hasOpenStream(kind: MediaKind, source: string): boolean {
    return this.streams.hasOpenStream(kind, source);
  }

  acceptProducerOpen(kind: MediaKind, paramsText: string): OpenStreamResult {
    if (!this.producerOpenEnabled()) {
      return this.error(ErrorCode.RpcParamInvalid);
    }
    if (!this.mediaEnabled(kind)) {
      return this.error(ErrorCode.NotSupported);
    }
    const params = parseObject(paramsText);
    if (params === undefined) return this.error(ErrorCode.RpcParamInvalid);

    const source = jsonStringOr(params, "source", this.sourceFor(kind));
    const peerRole = jsonStringOr(params, "peerRole", "receiver");
    const syncGroupId = jsonStringOr(params, "syncGroupId", "");
    const castSessionId = jsonStringOr(params, "castSessionId", "");
    const maxDataSize = jsonU32Or(params, "maxDataSize", 0);

    if (kind === MediaKind.Video) {
      const codec = jsonStringOr(params, "codec", "h264");
      if (codec !== "h264") return this.error(ErrorCode.MediaCodecUnsupported);
      return this.openAccepted(
        kind,
        this.allocateStreamId(kind),
        source,
        peerRole,
        "h264",
        "media.video",
        "timestampUs",
        syncGroupId,
        castSessionId,
        maxDataSize,
        { codecFormat: "annexb", parameterSetsInKeyFrame: true }
      );
    }

    const codec = jsonStringOr(params, "codec", "aac");
    if (codec !== "aac") return this.error(ErrorCode.MediaCodecUnsupported);
    const transportFormat = jsonStringOr(params, "transportFormat", this.options.audioFormat || "adts");
    if (transportFormat !== "adts") return this.error(ErrorCode.MediaCodecUnsupported);
    return this.openAccepted(
      kind,
      this.allocateStreamId(kind),
      source,
      peerRole,
      "aac",
      "media.audio",
      "timestampUs",
      syncGroupId,
      castSessionId,
      maxDataSize,
      {
        transportFormat,
        sampleRate: jsonU32Or(params, "sampleRate", this.audioSampleRate()),
        channels: jsonU32Or(params, "channels", this.audioChannels())
      }
    );
  }

  registerPulledOpen(kind: MediaKind, responseText: string): OpenStreamResult {
    if (!this.mediaEnabled(kind)) return this.error(ErrorCode.NotSupported);
    const result = parseObject(responseText);
    if (result === undefined) return this.error(ErrorCode.RpcPayloadInvalid);
    const streamId = jsonU32Or(result, "streamId", 0);
    if (streamId === 0) return this.error(ErrorCode.RpcPayloadInvalid);
    const source = jsonStringOr(result, "source", this.sourceFor(kind));
    const peerRole = jsonStringOr(result, "peerRole", "transmitter");

    if (kind === MediaKind.Video) {
      const codec = jsonStringOr(result, "codec", "h264");
      if (codec !== "h264") return this.error(ErrorCode.MediaCodecUnsupported);
      return this.openAccepted(
        kind,
        streamId,
        source,
        peerRole,
        codec,
        jsonStringOr(result, "streamProfile", "media.video"),
        jsonStringOr(result, "cursorUnit", "timestampUs"),
        jsonStringOr(result, "syncGroupId", ""),
        jsonStringOr(result, "castSessionId", ""),
        jsonU32Or(result, "maxDataSize", 0),
        result
      );
    }

    const codec = jsonStringOr(result, "codec", "aac");
    if (codec !== "aac") return this.error(ErrorCode.MediaCodecUnsupported);
    const transportFormat = jsonStringOr(result, "transportFormat", "adts");
    if (transportFormat !== "adts") return this.error(ErrorCode.MediaCodecUnsupported);
    return this.openAccepted(
      kind,
      streamId,
      source,
      peerRole,
      codec,
      jsonStringOr(result, "streamProfile", "media.audio"),
      jsonStringOr(result, "cursorUnit", "timestampUs"),
      jsonStringOr(result, "syncGroupId", ""),
      jsonStringOr(result, "castSessionId", ""),
      jsonU32Or(result, "maxDataSize", 0),
      {
        ...result,
        sampleRate: jsonU32Or(result, "sampleRate", this.audioSampleRate()),
        channels: jsonU32Or(result, "channels", this.audioChannels())
      }
    );
  }

  close(kind: MediaKind, paramsText: string): OpenStreamResult {
    const params = parseObject(paramsText);
    if (params === undefined) return this.error(ErrorCode.RpcParamInvalid);
    const streamId = jsonU32Or(params, "streamId", 0);
    if (streamId === 0) return this.error(ErrorCode.RpcParamMissing);

    let alreadyClosed = true;
    const info = this.streams.findStream(streamId);
    if (info !== undefined) {
      alreadyClosed = false;
      if (kindFromStreamInfo(info) !== kind) return this.error(ErrorCode.StreamNotFound);
      this.streams.close(streamId);
    }
    return { status: ErrorCode.Success, body: { streamId, state: "closed", alreadyClosed } };
  }

  closeLocal(kind: MediaKind, streamId: number): OpenStreamResult {
    return this.close(kind, JSON.stringify({ streamId, peerRole: "transmitter" }));
  }

  handleStream(stream: StreamPayload): void {
    this.streams.handleStream(stream);
    const stats = this.streams.stats();
    this.mediaStats.unknownChunks = stats.unknownChunks;
    this.mediaStats.seqGaps = stats.seqGaps;
    this.mediaStats.duplicateSeq = stats.duplicateSeq;
  }

  stats(): MediaStreamStats {
    const stats = this.streams.stats();
    return {
      ...this.mediaStats,
      unknownChunks: stats.unknownChunks,
      seqGaps: stats.seqGaps,
      duplicateSeq: stats.duplicateSeq
    };
  }

  activeStreamCount(): number {
    return this.streams.activeStreamCount();
  }

  activeStreamsSnapshot(): ActiveMediaStream[] {
    return this.streams.activeStreamsSnapshot().map((stream: ActiveStream) => ({
      kind: kindFromName(stream.kind),
      streamId: stream.streamId,
      source: stream.source
    }));
  }

  onStreamOpened(info: StreamInfo): void {
    this.options.streamSink?.onStreamOpened(toMediaInfo(info));
  }

  onStreamChunk(info: StreamInfo, stream: StreamPayload): void {
    const kind = kindFromStreamInfo(info);
    if (kind === MediaKind.Video) {
      this.mediaStats.videoChunks += 1;
      this.mediaStats.videoBytes += stream.data.length;
    } else {
      this.mediaStats.audioChunks += 1;
      this.mediaStats.audioBytes += stream.data.length;
    }
    this.options.streamSink?.onStreamChunk(kind, stream);
  }

  onStreamClosed(info: StreamInfo): void {
    this.options.streamSink?.onStreamClosed(kindFromStreamInfo(info), info.streamId);
  }

  private openAccepted(
    kind: MediaKind,
    streamId: number,
    source: string,
    peerRole: string,
    codec: string,
    streamProfile: string,
    cursorUnit: string,
    syncGroupId: string,
    castSessionId: string,
    maxDataSize: number,
    extra: Record<string, unknown>
  ): OpenStreamResult {
    const body: Record<string, unknown> = {
      streamId,
      state: "streaming",
      source,
      peerRole,
      codec,
      streamProfile,
      cursorUnit
    };
    if (syncGroupId.length > 0) body.syncGroupId = syncGroupId;
    if (castSessionId.length > 0) body.castSessionId = castSessionId;
    if (maxDataSize !== 0) body.maxDataSize = maxDataSize;
    Object.assign(body, extra);

    const status = this.streams.registerStream(
      {
        streamId,
        kind,
        source: String(body.source ?? ""),
        payloadFormat: String(body.codec ?? ""),
        streamProfile: String(body.streamProfile ?? ""),
        cursorUnit: String(body.cursorUnit ?? ""),
        metadata: { ...body }
      },
      { rejectDuplicateKindSource: true }
    );
    if (status !== ErrorCode.Success) return this.error(status);
    return { status: ErrorCode.Success, body };
  }

  private error(status: ErrorCode): OpenStreamResult {
    return { status, body: {} };
  }

  private allocateStreamId(kind: MediaKind): number {
    if (kind === MediaKind.Video) return this.nextVideoStreamId++;
    return this.nextAudioStreamId++;
  }
}

export function installMediaHostHandlers(broker: BasicBroker, registry: MediaStreamRegistry): void {
  const handler = (kind: MediaKind, open: boolean): RawRpcHandler => {
    return (_context, request: RpcRequestView): RpcResponseData => {
      const result = open
        ? registry.acceptProducerOpen(kind, bytesToText(request.body))
        : registry.close(kind, bytesToText(request.body));
      return makeResponse(result);
    };
  };
  broker.registerRawMethod(MethodId.VideoOpenStream, handler(MediaKind.Video, true));
  broker.registerRawMethod(MethodId.AudioOpenStream, handler(MediaKind.Audio, true));
  broker.registerRawMethod(MethodId.VideoCloseStream, handler(MediaKind.Video, false));
  broker.registerRawMethod(MethodId.AudioCloseStream, handler(MediaKind.Audio, false));
  broker.registerStreamHandler((_context, stream) => {
    registry.handleStream(stream);
  });
}

function makeResponse(result: OpenStreamResult): RpcResponseData {
  return {
    encoding: RpcEncoding.Json,
    body: result.status === ErrorCode.Success ? toBytes(JSON.stringify(result.body)) : new Uint8Array(),
    overrideEncoding: true,
    statusCode: result.status,
    overrideStatus: true
  };
}

function parseObject(text: string): Record<string, unknown> | undefined {
  if (text.length === 0) return {};
  try {
    const parsed = JSON.parse(text) as unknown;
    if (parsed !== null && typeof parsed === "object" && !Array.isArray(parsed)) {
      return parsed as Record<string, unknown>;
    }
  } catch {
    return undefined;
  }
  return undefined;
}

function jsonStringOr(object: Record<string, unknown>, name: string, fallback: string): string {
  const value = object[name];
  return typeof value === "string" ? value : fallback;
}

function jsonU32Or(object: Record<string, unknown>, name: string, fallback: number): number {
  const value = object[name];
  if (typeof value !== "number" || !Number.isInteger(value) || value < 0 || value > 0xffffffff) {
    return fallback;
  }
  return value >>> 0;
}

function kindFromName(kind: string): MediaKind {
  return kind === MediaKind.Audio ? MediaKind.Audio : MediaKind.Video;
}

function kindFromStreamInfo(info: StreamInfo): MediaKind {
  return kindFromName(info.kind);
}

function toMediaInfo(info: StreamInfo): MediaStreamInfo {
  return {
    kind: kindFromStreamInfo(info),
    streamId: info.streamId,
    source: info.source,
    codec: info.payloadFormat,
    streamProfile: info.streamProfile,
    cursorUnit: info.cursorUnit,
    width: jsonU32Or(info.metadata, "width", jsonU32Or(info.metadata, "codedWidth", 0)),
    height: jsonU32Or(info.metadata, "height", jsonU32Or(info.metadata, "codedHeight", 0)),
    sampleRate: jsonU32Or(info.metadata, "sampleRate", 0),
    channels: jsonU32Or(info.metadata, "channels", 0),
    metadata: { ...info.metadata }
  };
}
