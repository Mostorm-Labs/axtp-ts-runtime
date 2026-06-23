import { describe, expect, it } from "vitest";
import {
  BasicBroker,
  BrokerTaskType,
  ErrorCode,
  MediaKind,
  MediaStreamRegistry,
  MethodId,
  OpenMode,
  RpcBodyEncoding,
  RpcEncoding,
  RpcOp,
  SourceProtocol,
  installMediaHostHandlers,
  rpcPayload,
  streamPayload,
  toBytes,
  bytesToText,
  type MediaStreamInfo,
  type StreamPayload,
  type MediaStreamSink
} from "../../src/index.js";

class RecordingMediaSink implements MediaStreamSink {
  readonly opened: MediaStreamInfo[] = [];
  readonly chunks: StreamPayload[] = [];
  readonly closed: Array<{ kind: MediaKind; streamId: number }> = [];

  onStreamOpened(info: MediaStreamInfo): void {
    this.opened.push({ ...info });
  }

  onStreamChunk(_kind: MediaKind, stream: StreamPayload): void {
    this.chunks.push(stream);
  }

  onStreamClosed(kind: MediaKind, streamId: number): void {
    this.closed.push({ kind, streamId });
  }
}

describe("media profile", () => {
  it("opens producer video streams through broker handlers and routes chunks", async () => {
    const sink = new RecordingMediaSink();
    const broker = new BasicBroker();
    const registry = new MediaStreamRegistry({
      openMode: OpenMode.ProducerOpen,
      streamSink: sink
    });
    installMediaHostHandlers(broker, registry);

    broker.submit({
      type: BrokerTaskType.RpcRequest,
      rpc: rpcPayload({
        encoding: RpcEncoding.Json,
        op: RpcOp.Request,
        requestId: 77,
        methodOrEventId: MethodId.VideoOpenStream,
        bodyEncoding: RpcBodyEncoding.None,
        meta: { sourceProtocol: SourceProtocol.JsonRpc, sessionId: 0, requestId: 77, jsonSid: "", jsonMethodOrEventName: "video.openStream" },
        body: toBytes('{"source":"wireless_cast_video","peerRole":"receiver","codec":"h264"}')
      })
    });
    await broker.poll();

    const openResult = broker.pollResult()?.rpc;
    expect(openResult?.statusCode).toBe(ErrorCode.Success);
    const openBody = JSON.parse(bytesToText(openResult?.body ?? new Uint8Array())) as Record<string, unknown>;
    expect(openBody.streamId).toBe(0x1001);
    expect(openBody.codec).toBe("h264");
    expect(openBody.codecFormat).toBe("annexb");
    expect(sink.opened[0]?.kind).toBe(MediaKind.Video);

    broker.submit({
      type: BrokerTaskType.StreamData,
      rpc: rpcPayload(),
      stream: streamPayload({
        streamId: 0x1001,
        seqId: 0,
        cursor: 1000n,
        data: Uint8Array.of(0x00, 0x00, 0x01, 0x67, 0x42)
      })
    });
    await broker.poll();
    expect(broker.pollResult()).toBeUndefined();
    expect(registry.stats().videoChunks).toBe(1);
    expect(registry.stats().videoBytes).toBe(5);
    expect(sink.chunks[0]?.streamId).toBe(0x1001);

    broker.submit({
      type: BrokerTaskType.RpcRequest,
      rpc: rpcPayload({
        encoding: RpcEncoding.Json,
        op: RpcOp.Request,
        requestId: 78,
        methodOrEventId: MethodId.VideoCloseStream,
        bodyEncoding: RpcBodyEncoding.None,
        body: toBytes('{"streamId":4097,"peerRole":"transmitter"}')
      })
    });
    await broker.poll();

    const closeResult = broker.pollResult()?.rpc;
    expect(closeResult?.statusCode).toBe(ErrorCode.Success);
    expect(sink.closed).toEqual([{ kind: MediaKind.Video, streamId: 0x1001 }]);
    expect(registry.activeStreamCount()).toBe(0);
  });
});
