import { describe, expect, it } from "vitest";
import {
  AxtpClient,
  AxtpCore,
  AxtpEndpoint,
  AxtpWireMode,
  BasicBroker,
  ByteReader,
  ByteWriter,
  ErrorCode,
  EventId,
  FrameDecoder,
  InboundProcessor,
  JsonRpcEncoder,
  MethodId,
  MockTransport,
  OutboundProcessor,
  PayloadType,
  RpcBodyEncoding,
  RpcEncoding,
  RpcOp,
  SourceProtocol,
  TransportKind,
  WebSocketJsonRpcAdapter,
  bytesEqual,
  bytesToText,
  crc16CcittFalse,
  rpcPayload,
  toBytes,
  type Bytes,
  type ControlPayload,
  type Frame,
  type PayloadSink,
  type RpcPayload,
  type StreamPayload
} from "./index.js";
import { ControlOpcode } from "./generated/axtp_ids_generated.js";

class CapturePayloadSink implements PayloadSink {
  readonly controls: ControlPayload[] = [];
  readonly rpcs: RpcPayload[] = [];
  readonly streams: StreamPayload[] = [];

  onControl(payload: ControlPayload): void {
    this.controls.push(payload);
  }

  onRpc(payload: RpcPayload): void {
    this.rpcs.push(payload);
  }

  onStream(payload: StreamPayload): void {
    this.streams.push(payload);
  }
}

function encodeRpc(payload: RpcPayload, maxFrameSize = 4096): Bytes {
  const chunks: Bytes[] = [];
  const outbound = new OutboundProcessor({ writeBytes: (bytes) => chunks.push(bytes) }, maxFrameSize);
  outbound.sendRpcRequest(payload);
  return concatForTest(chunks);
}

function encodeControl(opcode: ControlOpcode, controlId: number): Bytes {
  const chunks: Bytes[] = [];
  const outbound = new OutboundProcessor({ writeBytes: (bytes) => chunks.push(bytes) });
  outbound.sendControl({
    opcode,
    controlId,
    statusCode: ErrorCode.Success,
    meta: {
      sourceProtocol: SourceProtocol.AxtpV1,
      sessionId: 0,
      requestId: 0,
      jsonSid: "",
      jsonMethodOrEventName: ""
    },
    body: new Uint8Array()
  });
  return concatForTest(chunks);
}

function concatForTest(chunks: Bytes[]): Bytes {
  const out = new Uint8Array(chunks.reduce((sum, chunk) => sum + chunk.length, 0));
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.length;
  }
  return out;
}

function splitFrames(bytes: Bytes): Bytes[] {
  const frames: Bytes[] = [];
  let offset = 0;
  while (offset < bytes.length) {
    const length = bytes[offset + 4] | (bytes[offset + 5] << 8);
    const total = 12 + length + 2;
    frames.push(bytes.slice(offset, offset + total));
    offset += total;
  }
  return frames;
}

async function settle(): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, 0));
}

describe("model IO", () => {
  it("uses little-endian byte IO and CRC16-CCITT-FALSE", () => {
    const writer = new ByteWriter();
    writer.writeU8(0x12);
    writer.writeU16(0x3456);
    writer.writeU32(0x789abcde);
    writer.writeU64(0x1122334455667788n);
    expect([...writer.bytes()]).toEqual([
      0x12, 0x56, 0x34, 0xde, 0xbc, 0x9a, 0x78, 0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11
    ]);

    const reader = new ByteReader(writer.bytes());
    expect(reader.readU8()).toBe(0x12);
    expect(reader.readU16()).toBe(0x3456);
    expect(reader.readU32()).toBe(0x789abcde);
    expect(reader.readU64()).toBe(0x1122334455667788n);
    expect(reader.empty()).toBe(true);
    expect(reader.readU8()).toBeUndefined();
    expect(reader.offset()).toBe(15);
    expect(crc16CcittFalse(toBytes("123456789"))).toBe(0x29b1);
  });
});

describe("framed binary pipeline", () => {
  it("decodes split input, concatenated frames, resync noise, and drops CRC-invalid frames", () => {
    const first = encodeRpc(rpcPayload({
      encoding: RpcEncoding.Tlv,
      op: RpcOp.Request,
      requestId: 7,
      methodOrEventId: MethodId.AudioGetAlgorithmConfig,
      bodyEncoding: RpcBodyEncoding.Tlv8,
      body: Uint8Array.of(0xaa)
    }));
    const sink = new CapturePayloadSink();
    const inbound = new InboundProcessor(sink);
    inbound.onBytes(first.slice(0, 6));
    expect(sink.rpcs).toHaveLength(0);
    inbound.onBytes(first.slice(6));
    expect(sink.rpcs[0].requestId).toBe(7);

    const second = encodeRpc(rpcPayload({
      encoding: RpcEncoding.Tlv,
      op: RpcOp.Request,
      requestId: 8,
      methodOrEventId: MethodId.AudioGetAlgorithmConfig,
      bodyEncoding: RpcBodyEncoding.Tlv8
    }));
    const third = encodeRpc(rpcPayload({
      encoding: RpcEncoding.Tlv,
      op: RpcOp.Request,
      requestId: 9,
      methodOrEventId: MethodId.AudioGetAlgorithmConfig,
      bodyEncoding: RpcBodyEncoding.Tlv8
    }));
    inbound.onBytes(concatForTest([second, third]));
    expect(sink.rpcs.map((item) => item.requestId)).toEqual([7, 8, 9]);

    inbound.onBytes(concatForTest([Uint8Array.of(0, 0x41, 0, 0x99), second]));
    expect(sink.rpcs.at(-1)?.requestId).toBe(8);

    const invalid = third.slice();
    invalid[invalid.length - 1] ^= 0xff;
    inbound.onBytes(invalid);
    expect(sink.rpcs).toHaveLength(4);
  });

  it("round-trips fragmented messages and wraps message ids", () => {
    const body = Uint8Array.from({ length: 40 }, (_, index) => index);
    const encoded = encodeRpc(rpcPayload({
      encoding: RpcEncoding.Tlv,
      op: RpcOp.Request,
      requestId: 43,
      methodOrEventId: MethodId.AudioGetAlgorithmConfig,
      bodyEncoding: RpcBodyEncoding.RawBytes,
      body
    }), 24);
    const frames = splitFrames(encoded);
    expect(frames.length).toBeGreaterThan(1);

    const sink = new CapturePayloadSink();
    const inbound = new InboundProcessor(sink);
    for (const frame of [...frames].reverse()) {
      inbound.onBytes(frame);
    }
    expect(sink.rpcs).toHaveLength(1);
    expect(bytesEqual(sink.rpcs[0].body, body)).toBe(true);

    const frameSink: Frame[] = [];
    const decoder = new FrameDecoder({ onFrame: (frame) => frameSink.push(frame) });
    const outbound = new OutboundProcessor({ writeBytes: (bytes) => decoder.onBytes(bytes) });
    const fragmenter = (outbound as unknown as { messageFragmenter: { nextMessageId: number } }).messageFragmenter;
    fragmenter.nextMessageId = 0xffff;
    outbound.sendRpcRequest(rpcPayload({ op: RpcOp.Request, requestId: 1, methodOrEventId: MethodId.AudioGetAlgorithmConfig }));
    outbound.sendRpcRequest(rpcPayload({ op: RpcOp.Request, requestId: 2, methodOrEventId: MethodId.AudioGetAlgorithmConfig }));
    expect(frameSink[0].header.messageId).toBe(0xffff);
    expect(frameSink[1].header.messageId).toBe(1);
  });
});

describe("core and endpoint", () => {
  it("handles control session and pending RPC responses", () => {
    const core = new AxtpCore();
    core.byteSink.onBytes(encodeControl(ControlOpcode.Open, 1));
    expect(core.controlSessionOpen()).toBe(true);
    const openResponse = core.tryPopOutboundBytes();
    expect(openResponse).toBeDefined();
    const openSink = new CapturePayloadSink();
    new InboundProcessor(openSink).onBytes(openResponse!);
    expect(openSink.controls[0].opcode).toBe(ControlOpcode.Accept);

    core.byteSink.onBytes(encodeControl(ControlOpcode.Ping, 2));
    const pingResponse = core.tryPopOutboundBytes();
    const pingSink = new CapturePayloadSink();
    new InboundProcessor(pingSink).onBytes(pingResponse!);
    expect(pingSink.controls[0].opcode).toBe(ControlOpcode.Pong);

    core.byteSink.onBytes(encodeControl(ControlOpcode.Close, 3));
    const closeResponse = core.tryPopOutboundBytes();
    const closeSink = new CapturePayloadSink();
    new InboundProcessor(closeSink).onBytes(closeResponse!);
    expect(closeSink.controls[0].opcode).toBe(ControlOpcode.CloseAck);
    expect(core.controlSessionOpen()).toBe(false);

    core.expectRpcResponse(55);
    core.byteSink.onBytes(encodeRpc(rpcPayload({
      encoding: RpcEncoding.Tlv,
      op: RpcOp.RequestResponse,
      requestId: 55,
      methodOrEventId: MethodId.AudioGetAlgorithmConfig,
      bodyEncoding: RpcBodyEncoding.Tlv8,
      body: Uint8Array.of(1)
    })));
    expect(core.tryTakeRpcResponse(55)?.body[0]).toBe(1);
  });

  it("runs endpoint + broker + mock transport request/response flow", async () => {
    const broker = new BasicBroker();
    const endpoint = new AxtpEndpoint(broker);
    const transport = new MockTransport();
    endpoint.attachTransport(transport);
    broker.registerMethod(MethodId.AudioGetAlgorithmConfig, (request) => {
      expect(request.requestId).toBe(900);
      return Uint8Array.of(0x77);
    });

    transport.injectIncoming(encodeRpc(rpcPayload({
      encoding: RpcEncoding.Tlv,
      op: RpcOp.Request,
      requestId: 900,
      methodOrEventId: MethodId.AudioGetAlgorithmConfig,
      bodyEncoding: RpcBodyEncoding.Tlv8
    })));
    await endpoint.poll();
    const outgoing = transport.tryPopOutgoing();
    expect(outgoing).toBeDefined();
    const sink = new CapturePayloadSink();
    new InboundProcessor(sink).onBytes(outgoing!);
    expect(sink.rpcs[0].op).toBe(RpcOp.RequestResponse);
    expect(sink.rpcs[0].body[0]).toBe(0x77);
  });
});

describe("websocket json rpc", () => {
  it("handles identify gating, requests, errors, batches, and events", async () => {
    const broker = new BasicBroker();
    const endpoint = new AxtpEndpoint(broker);
    broker.registerMethod(MethodId.AudioGetAlgorithmConfig, () => toBytes('{"ok":true}'));
    broker.registerMethod(MethodId.AudioSetAlgorithmConfig, (request) => {
      expect(bytesToText(request.body)).toContain("noiseSuppression");
      return new Uint8Array();
    });
    broker.registerMethod(MethodId.AudioGetAlgorithmCapabilities, () => Uint8Array.of(0xb1));

    const transport = new MockTransport({
      kind: TransportKind.Mock,
      wireMode: AxtpWireMode.WebSocketJsonRpc,
      defaultRpcEncoding: RpcEncoding.Json,
      messageOriented: true,
      supportsTextMessage: true,
      supportsBinaryMessage: false,
      preferredFrameSize: 4096
    });
    endpoint.attachTransport(transport);
    const adapter = new WebSocketJsonRpcAdapter(endpoint, transport);
    transport.bind(adapter);

    transport.injectIncoming(toBytes('{"sid":"","op":7,"d":{"id":700,"method":"audio.getAlgorithmConfig","params":{}}}'));
    await settle();
    let response = JSON.parse(bytesToText(transport.tryPopOutgoing()!));
    expect(response.d.status.code).toBe(ErrorCode.ControlOpenRequired);

    transport.injectIncoming(toBytes('{"sid":"","op":2,"d":{"rpcVersion":1}}'));
    await settle();
    response = JSON.parse(bytesToText(transport.tryPopOutgoing()!));
    expect(response.op).toBe(RpcOp.Identified);
    const sid = response.sid as string;

    transport.injectIncoming(toBytes(`{"sid":"${sid}","op":7,"d":{"id":701,"method":"audio.getAlgorithmConfig","params":{}}}`));
    await settle();
    response = JSON.parse(bytesToText(transport.tryPopOutgoing()!));
    expect(response.d.status.ok).toBe(true);
    expect(response.d.result.ok).toBe(true);

    transport.injectIncoming(toBytes(`{"sid":"${sid}","op":7,"d":{"id":702,"method":"audio.setAlgorithmConfig","params":{"noiseSuppression":{"enabled":true}}}}`));
    await settle();
    response = JSON.parse(bytesToText(transport.tryPopOutgoing()!));
    expect(response.d.status.ok).toBe(true);
    expect(response.d.result).toBeUndefined();

    transport.injectIncoming(toBytes(`{"sid":"${sid}","op":7,"d":{"id":703,"method":"audio.unknown","params":{}}}`));
    await settle();
    response = JSON.parse(bytesToText(transport.tryPopOutgoing()!));
    expect(response.d.status.code).toBe(ErrorCode.RpcMethodNotFound);

    transport.injectIncoming(toBytes(`{"sid":"${sid}","op":7,"d":{"id":704,"method":"audio.getAlgorithmCapabilities","params":{}}}`));
    await settle();
    response = JSON.parse(bytesToText(transport.tryPopOutgoing()!));
    expect(response.d.status.code).toBe(ErrorCode.RpcBodyDecodeFailed);

    transport.injectIncoming(toBytes(`{"sid":"${sid}","op":9,"d":{"id":705,"requests":[]}}`));
    await settle();
    response = JSON.parse(bytesToText(transport.tryPopOutgoing()!));
    expect(response.op).toBe(RpcOp.RequestBatchResponse);
    expect(response.d.status.code).toBe(ErrorCode.RpcBatchUnsupported);

    await adapter.sendEvent(rpcPayload({
      op: RpcOp.Event,
      methodOrEventId: EventId.AudioAlgorithmConfigChanged,
      meta: {
        sourceProtocol: SourceProtocol.JsonRpc,
        sessionId: 0,
        requestId: 0,
        jsonSid: sid,
        jsonMethodOrEventName: ""
      },
      body: toBytes('{"reason":"manual"}')
    }));
    response = JSON.parse(bytesToText(transport.tryPopOutgoing()!));
    expect(response.d.event).toBe("audio.algorithmConfigChanged");
    expect(response.d.data.reason).toBe("manual");

    const hello = new JsonRpcEncoder().encode(JsonRpcEncoder.makeHello());
    expect(JSON.parse(bytesToText(hello)).op).toBe(RpcOp.Hello);
  });
});

describe("sdk dynamic calls", () => {
  it("supports local JSON/TLV/raw calls and unavailable/timeout errors", async () => {
    const client = new AxtpClient({ timeoutMs: 5 });
    client.registry().addMethod(0x90010001, "vendor.echo");
    client.registerMethod(MethodId.AudioGetAlgorithmConfig, () => toBytes('{"ok":true}'));
    client.registerMethod(MethodId.AudioSetAlgorithmConfig, (request) => {
      expect(request.encoding).toBe(RpcEncoding.Tlv);
      return new Uint8Array();
    });
    client.registerMethod(0x90010001, (request) => request.body);

    await expect(client.callJson("audio.getAlgorithmConfig", "{}")).resolves.toBe('{"ok":true}');
    await expect(client.callTlv("audio.setAlgorithmConfig", Uint8Array.of(1, 2))).resolves.toEqual(new Uint8Array());
    await expect(client.callRawBytes(0x90010001, Uint8Array.of(0xca, 0xfe))).resolves.toEqual(Uint8Array.of(0xca, 0xfe));

    const unavailable = await new AxtpClient().callRaw(rpcPayload({ methodOrEventId: MethodId.AudioGetAlgorithmConfig }));
    expect(unavailable.statusCode).toBe(ErrorCode.Unavailable);

    const timeoutClient = new AxtpClient({ timeoutMs: 2 });
    await timeoutClient.attachTransport(new MockTransport());
    const timeout = await timeoutClient.callRaw(rpcPayload({ methodOrEventId: MethodId.AudioGetAlgorithmConfig }));
    expect(timeout.statusCode).toBe(ErrorCode.RpcResponseTimeout);
  });
});
