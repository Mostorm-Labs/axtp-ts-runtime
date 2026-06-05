import { bytesToText, concatBytes, toBytes, type Bytes } from "./bytes.js";
import { ByteReader, ByteWriter, crc16CcittFalse, type ByteSink, type ByteWriterSink } from "./io.js";
import {
  ControlOpcode,
  ErrorCode,
  PayloadType,
  RpcBodyEncoding,
  RpcEncoding,
  RpcOp
} from "./generated/axtp_ids_generated.js";
import { RegistryLookup } from "./generated/registry_generated.js";
import {
  SourceProtocol,
  controlPayload,
  defaultPayloadMeta,
  kAxtpStandardMagic0,
  kAxtpStandardMagic1,
  kAxtpVersion1,
  kBinaryRpcHeaderSize,
  kControlPayloadHeaderSize,
  kStandardFrameCrcSize,
  kStandardFrameHeaderSize,
  kStreamPayloadHeaderSize,
  rpcPayload,
  streamPayload,
  type ControlPayload,
  type Frame,
  type Message,
  type RpcPayload,
  type StreamPayload
} from "./model.js";
import { AxtpWireMode } from "./transport.js";

export interface PayloadSink {
  onControl(payload: ControlPayload): void;
  onRpc(payload: RpcPayload): void;
  onStream(payload: StreamPayload): void;
}

function isPayloadType(value: number): value is PayloadType {
  return value === PayloadType.Control || value === PayloadType.Rpc || value === PayloadType.Stream;
}

export class FrameDecoder implements ByteSink {
  private buffer: Bytes = new Uint8Array();

  constructor(private readonly next: { onFrame(frame: Frame): void }) {}

  onBytes(bytes: Bytes): void {
    this.buffer = concatBytes([this.buffer, bytes]);
    this.parseLoop();
  }

  private consume(count: number): void {
    this.buffer = this.buffer.slice(count);
  }

  private resyncToMagic(): void {
    for (let index = 0; index + 1 < this.buffer.length; index += 1) {
      if (this.buffer[index] === kAxtpStandardMagic0 && this.buffer[index + 1] === kAxtpStandardMagic1) {
        if (index > 0) this.consume(index);
        return;
      }
    }
    if (this.buffer.length === 0) return;
    const keep = this.buffer[this.buffer.length - 1] === kAxtpStandardMagic0 ? 1 : 0;
    this.consume(this.buffer.length - keep);
  }

  private parseLoop(): void {
    while (true) {
      this.resyncToMagic();
      if (this.buffer.length < kStandardFrameHeaderSize + kStandardFrameCrcSize) return;

      const headerBytes = this.buffer.slice(0, kStandardFrameHeaderSize);
      const reader = new ByteReader(headerBytes);
      reader.readU8();
      reader.readU8();
      const version = reader.readU8();
      const payloadType = reader.readU8();
      const payloadLength = reader.readU16();
      const sourceId = reader.readU8();
      const destinationId = reader.readU8();
      const messageId = reader.readU16();
      const frameIndex = reader.readU8();
      const frameCount = reader.readU8();

      if (
        version === undefined ||
        payloadType === undefined ||
        payloadLength === undefined ||
        sourceId === undefined ||
        destinationId === undefined ||
        messageId === undefined ||
        frameIndex === undefined ||
        frameCount === undefined
      ) {
        return;
      }

      if (version !== kAxtpVersion1 || !isPayloadType(payloadType) || frameCount === 0 || frameIndex >= frameCount) {
        this.consume(1);
        continue;
      }

      const totalSize = kStandardFrameHeaderSize + payloadLength + kStandardFrameCrcSize;
      if (this.buffer.length < totalSize) return;

      const frameBytes = this.buffer.slice(0, totalSize);
      const footerReader = new ByteReader(frameBytes.slice(totalSize - kStandardFrameCrcSize));
      const expectedCrc = footerReader.readU16();
      const actualCrc = crc16CcittFalse(frameBytes.slice(0, totalSize - kStandardFrameCrcSize));
      if (expectedCrc === undefined || expectedCrc !== actualCrc) {
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
        payload: frameBytes.slice(kStandardFrameHeaderSize, kStandardFrameHeaderSize + payloadLength),
        crc16: expectedCrc
      });
    }
  }
}

export class FrameEncoder {
  encode(frame: Frame): Bytes {
    const writer = new ByteWriter();
    writer.writeU8(kAxtpStandardMagic0);
    writer.writeU8(kAxtpStandardMagic1);
    writer.writeU8(frame.header.version);
    writer.writeU8(frame.header.payloadType);
    writer.writeU16(frame.payload.length);
    writer.writeU8(frame.header.sourceId);
    writer.writeU8(frame.header.destinationId);
    writer.writeU16(frame.header.messageId);
    writer.writeU8(frame.header.frameIndex);
    writer.writeU8(frame.header.frameCount);
    writer.writeBytes(frame.payload);
    writer.writeU16(crc16CcittFalse(writer.bytes()));
    return writer.takeBytes();
  }
}

export class MessageReassembler {
  private readonly assemblies = new Map<number, {
    payloadType: PayloadType;
    frameCount: number;
    totalSize: number;
    fragments: Array<Bytes | undefined>;
  }>();

  constructor(
    private readonly next: { onMessage(message: Message): void },
    private readonly maxMessageSize = 1024 * 1024
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
        fragments: Array.from<Bytes | undefined>({ length: frame.header.frameCount }).fill(undefined)
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

    if (assembly.fragments.some((fragment) => fragment === undefined)) return;
    this.assemblies.delete(frame.header.messageId);
    this.next.onMessage({
      messageId: frame.header.messageId,
      payloadType: assembly.payloadType,
      body: concatBytes(assembly.fragments as Bytes[])
    });
  }
}

export class MessageFragmenter {
  private nextMessageId = 1;

  constructor(private maxFrameSizeValue = 4096) {}

  setMaxFrameSize(maxFrameSize: number): void {
    this.maxFrameSizeValue = maxFrameSize;
  }

  maxFrameSize(): number {
    return this.maxFrameSizeValue;
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
    for (let index = 0; index < frameCount; index += 1) {
      const offset = index * capacity;
      const payload = message.body.slice(offset, Math.min(message.body.length, offset + capacity));
      frames.push(this.makeFrame(message, messageId, index, frameCount, payload));
    }
    return frames;
  }

  private payloadCapacity(): number {
    if (this.maxFrameSizeValue <= kStandardFrameHeaderSize + kStandardFrameCrcSize) return 0;
    return this.maxFrameSizeValue - kStandardFrameHeaderSize - kStandardFrameCrcSize;
  }

  private takeMessageId(): number {
    const id = this.nextMessageId;
    this.nextMessageId += 1;
    if (this.nextMessageId > 0xffff) this.nextMessageId = 1;
    return id;
  }

  private makeFrame(message: Message, messageId: number, frameIndex: number, frameCount: number, payload: Bytes): Frame {
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

export class PayloadDecoder {
  constructor(private readonly next: PayloadSink) {}

  onMessage(message: Message): void {
    switch (message.payloadType) {
      case PayloadType.Control:
        this.decodeControl(message);
        break;
      case PayloadType.Rpc:
        this.decodeRpc(message);
        break;
      case PayloadType.Stream:
        this.decodeStream(message);
        break;
    }
  }

  private decodeControl(message: Message): void {
    if (message.body.length < kControlPayloadHeaderSize) return;
    const reader = new ByteReader(message.body);
    const opcode = reader.readU8();
    const controlId = reader.readU16();
    const statusCode = reader.readU16();
    const body = reader.readBytes(reader.remaining());
    if (opcode === undefined || controlId === undefined || statusCode === undefined || body === undefined) return;
    this.next.onControl(controlPayload({
      opcode: opcode as ControlOpcode,
      controlId,
      statusCode: statusCode as ErrorCode,
      body
    }));
  }

  private decodeRpc(message: Message): void {
    if (message.body.length < kBinaryRpcHeaderSize) return;
    const reader = new ByteReader(message.body);
    const encoding = reader.readU8();
    const op = reader.readU8();
    const requestId = reader.readU32();
    const methodOrEventId = reader.readU16();
    const statusCode = reader.readU16();
    const bodyEncoding = reader.readU8();
    const body = reader.readBytes(reader.remaining());
    if (
      encoding === undefined ||
      op === undefined ||
      requestId === undefined ||
      methodOrEventId === undefined ||
      statusCode === undefined ||
      bodyEncoding === undefined ||
      body === undefined
    ) {
      return;
    }
    this.next.onRpc(rpcPayload({
      encoding: encoding as RpcEncoding,
      op: op as RpcOp,
      requestId,
      methodOrEventId,
      statusCode: statusCode as ErrorCode,
      bodyEncoding: bodyEncoding as RpcBodyEncoding,
      meta: { ...defaultPayloadMeta(), requestId },
      body
    }));
  }

  private decodeStream(message: Message): void {
    if (message.body.length < kStreamPayloadHeaderSize) return;
    const reader = new ByteReader(message.body);
    const streamId = reader.readU32();
    const seqId = reader.readU32();
    const cursor = reader.readU64();
    const data = reader.readBytes(reader.remaining());
    if (streamId === undefined || seqId === undefined || cursor === undefined || data === undefined) return;
    this.next.onStream(streamPayload({ streamId, seqId, cursor, data }));
  }
}

export class PayloadEncoder {
  encodeControl(payload: ControlPayload): Message {
    const writer = new ByteWriter();
    writer.writeU8(payload.opcode);
    writer.writeU16(payload.controlId);
    writer.writeU16(payload.statusCode);
    writer.writeBytes(payload.body);
    return { messageId: 0, payloadType: PayloadType.Control, body: writer.takeBytes() };
  }

  encodeRpc(payload: RpcPayload): Message {
    const writer = new ByteWriter();
    writer.writeU8(payload.encoding);
    writer.writeU8(payload.op);
    writer.writeU32(payload.requestId);
    writer.writeU16(payload.methodOrEventId);
    writer.writeU16(payload.statusCode);
    writer.writeU8(payload.bodyEncoding);
    writer.writeBytes(payload.body);
    return { messageId: 0, payloadType: PayloadType.Rpc, body: writer.takeBytes() };
  }

  encodeStream(payload: StreamPayload): Message {
    const writer = new ByteWriter();
    writer.writeU32(payload.streamId);
    writer.writeU32(payload.seqId);
    writer.writeU64(payload.cursor);
    writer.writeBytes(payload.data);
    return { messageId: 0, payloadType: PayloadType.Stream, body: writer.takeBytes() };
  }
}

export class JsonRpcDecoder implements ByteSink {
  constructor(private readonly sink: PayloadSink) {}

  onBytes(bytes: Bytes): void {
    try {
      const object = JSON.parse(bytesToText(bytes)) as JsonObject;
      const op = parseOp(object);
      const d = asObject(object.d);

      if (op === RpcOp.Request) {
        this.decodeRequest(object, d);
        return;
      }
      if (op === RpcOp.Event) {
        this.decodeEvent(object, d);
        return;
      }
      if (op === RpcOp.Identify || op === RpcOp.Reidentify) {
        this.decodeSessionRpc(object, d, op);
        return;
      }
      if (op === RpcOp.RequestBatch) {
        this.decodeBatch(object, d);
      }
    } catch {
      // Invalid JSON-RPC messages are dropped here; the adapter can emit protocol errors.
    }
  }

  private decodeRequest(object: JsonObject, d: JsonObject): void {
    if (typeof d.method !== "string") throw new Error("missing method");
    const methodId = RegistryLookup.methodIdByName(d.method);
    if (methodId === undefined) {
      this.sink.onRpc(rpcPayload({
        encoding: RpcEncoding.Json,
        op: RpcOp.RequestResponse,
        requestId: parseRequestId(d),
        statusCode: ErrorCode.RpcMethodNotFound,
        bodyEncoding: RpcBodyEncoding.RawBytes,
        meta: {
          ...defaultPayloadMeta(),
          sourceProtocol: SourceProtocol.JsonRpc,
          jsonSid: parseSid(object),
          jsonMethodOrEventName: d.method
        }
      }));
      return;
    }

    const requestId = parseRequestId(d);
    this.sink.onRpc(rpcPayload({
      encoding: RpcEncoding.Json,
      op: RpcOp.Request,
      requestId,
      methodOrEventId: methodId,
      bodyEncoding: RpcBodyEncoding.RawBytes,
      meta: {
        ...defaultPayloadMeta(),
        sourceProtocol: SourceProtocol.JsonRpc,
        requestId,
        jsonSid: parseSid(object),
        jsonMethodOrEventName: d.method
      },
      body: d.params === undefined ? new Uint8Array() : jsonToBytes(d.params)
    }));
  }

  private decodeEvent(object: JsonObject, d: JsonObject): void {
    if (typeof d.event !== "string") throw new Error("missing event");
    const eventId = RegistryLookup.eventIdByName(d.event);
    if (eventId === undefined) return;
    this.sink.onRpc(rpcPayload({
      encoding: RpcEncoding.Json,
      op: RpcOp.Event,
      requestId: 0,
      methodOrEventId: eventId,
      bodyEncoding: RpcBodyEncoding.RawBytes,
      meta: {
        ...defaultPayloadMeta(),
        sourceProtocol: SourceProtocol.JsonRpc,
        jsonSid: parseSid(object),
        jsonMethodOrEventName: d.event
      },
      body: d.data === undefined ? new Uint8Array() : jsonToBytes(d.data)
    }));
  }

  private decodeSessionRpc(object: JsonObject, d: JsonObject, op: RpcOp): void {
    this.sink.onRpc(rpcPayload({
      encoding: RpcEncoding.Json,
      op,
      bodyEncoding: RpcBodyEncoding.RawBytes,
      meta: {
        ...defaultPayloadMeta(),
        sourceProtocol: SourceProtocol.JsonRpc,
        jsonSid: parseSid(object)
      },
      body: jsonToBytes(d)
    }));
  }

  private decodeBatch(object: JsonObject, d: JsonObject): void {
    const requestId = parseRequestId(d);
    this.sink.onRpc(rpcPayload({
      encoding: RpcEncoding.Json,
      op: RpcOp.RequestBatchResponse,
      requestId,
      statusCode: ErrorCode.RpcBatchUnsupported,
      bodyEncoding: RpcBodyEncoding.RawBytes,
      meta: {
        ...defaultPayloadMeta(),
        sourceProtocol: SourceProtocol.JsonRpc,
        requestId,
        jsonSid: parseSid(object)
      },
      body: jsonToBytes(d)
    }));
  }
}

export class JsonRpcEncoder {
  encode(payload: RpcPayload): Bytes {
    switch (payload.op) {
      case RpcOp.Hello:
        return toBytes(JSON.stringify({ sid: "", op: RpcOp.Hello, d: { axtpVersion: "1.0.0", rpcVersion: 1 } }));
      case RpcOp.Identified:
        return toBytes(JSON.stringify({ sid: payload.meta.jsonSid, op: RpcOp.Identified, d: { negotiatedRpcVersion: 1 } }));
      case RpcOp.Event:
        return toBytes(JSON.stringify(this.serializeEvent(payload)));
      case RpcOp.RequestBatchResponse:
        return toBytes(JSON.stringify(this.serializeBatchResponse(payload)));
      default:
        return toBytes(JSON.stringify(this.serializeResponse(payload)));
    }
  }

  static makeHello(): RpcPayload {
    return rpcPayload({
      encoding: RpcEncoding.Json,
      op: RpcOp.Hello,
      bodyEncoding: RpcBodyEncoding.RawBytes,
      meta: { ...defaultPayloadMeta(), sourceProtocol: SourceProtocol.JsonRpc }
    });
  }

  static makeIdentified(sid: string): RpcPayload {
    return rpcPayload({
      encoding: RpcEncoding.Json,
      op: RpcOp.Identified,
      bodyEncoding: RpcBodyEncoding.RawBytes,
      meta: { ...defaultPayloadMeta(), sourceProtocol: SourceProtocol.JsonRpc, jsonSid: sid }
    });
  }

  private serializeResponse(payload: RpcPayload): JsonObject {
    let statusCode = payload.statusCode;
    const result = bytesToJson(payload.body);
    if (statusCode === ErrorCode.Success && payload.body.length > 0 && result === undefined) {
      statusCode = ErrorCode.RpcBodyDecodeFailed;
    }
    const d: JsonObject = {
      id: payload.requestId,
      status: statusObject(statusCode)
    };
    if (statusCode === ErrorCode.Success && result !== undefined) {
      d.result = result;
    }
    return { sid: payload.meta.jsonSid, op: RpcOp.RequestResponse, d };
  }

  private serializeBatchResponse(payload: RpcPayload): JsonObject {
    return {
      sid: payload.meta.jsonSid,
      op: RpcOp.RequestBatchResponse,
      d: { id: payload.requestId, status: statusObject(payload.statusCode) }
    };
  }

  private serializeEvent(payload: RpcPayload): JsonObject {
    const eventName = payload.meta.jsonMethodOrEventName || RegistryLookup.eventById(payload.methodOrEventId)?.name || "";
    const d: JsonObject = { event: eventName };
    const data = bytesToJson(payload.body);
    if (data !== undefined) d.data = data;
    return { sid: payload.meta.jsonSid, op: RpcOp.Event, d };
  }
}

export class InboundProcessor implements ByteSink {
  private readonly payloadDecoder: PayloadDecoder;
  private readonly messageReassembler: MessageReassembler;
  private readonly frameDecoder: FrameDecoder;
  private readonly jsonRpcDecoder: JsonRpcDecoder;
  private wireMode = AxtpWireMode.FramedBinary;

  constructor(private readonly sink: PayloadSink) {
    this.payloadDecoder = new PayloadDecoder(this.sink);
    this.messageReassembler = new MessageReassembler(this.payloadDecoder);
    this.frameDecoder = new FrameDecoder(this.messageReassembler);
    this.jsonRpcDecoder = new JsonRpcDecoder(this.sink);
  }

  onBytes(bytes: Bytes): void {
    if (this.wireMode === AxtpWireMode.WebSocketJsonRpc) {
      this.jsonRpcDecoder.onBytes(bytes);
      return;
    }
    this.frameDecoder.onBytes(bytes);
  }

  setWireMode(wireMode: AxtpWireMode): void {
    this.wireMode = wireMode;
  }
}

export class OutboundProcessor {
  private readonly payloadEncoder = new PayloadEncoder();
  private readonly messageFragmenter: MessageFragmenter;
  private readonly frameEncoder = new FrameEncoder();
  private readonly jsonRpcEncoder = new JsonRpcEncoder();
  private wireMode = AxtpWireMode.FramedBinary;

  constructor(private readonly writer: ByteWriterSink, maxFrameSize = 4096) {
    this.messageFragmenter = new MessageFragmenter(maxFrameSize);
  }

  sendControl(payload: ControlPayload): void {
    if (this.wireMode === AxtpWireMode.WebSocketJsonRpc) return;
    this.sendMessage(this.payloadEncoder.encodeControl(payload));
  }

  sendRpcRequest(payload: RpcPayload): void {
    this.sendRpc(payload);
  }

  sendRpcResponse(payload: RpcPayload): void {
    this.sendRpc(payload);
  }

  sendRpcError(payload: RpcPayload): void {
    this.sendRpc(payload);
  }

  sendEvent(payload: RpcPayload): void {
    this.sendRpc(payload);
  }

  sendStream(payload: StreamPayload): void {
    if (this.wireMode === AxtpWireMode.WebSocketJsonRpc) return;
    this.sendMessage(this.payloadEncoder.encodeStream(payload));
  }

  sendRpc(payload: RpcPayload): void {
    if (this.wireMode === AxtpWireMode.WebSocketJsonRpc) {
      this.writer.writeBytes(this.jsonRpcEncoder.encode(payload));
      return;
    }
    this.sendMessage(this.payloadEncoder.encodeRpc(payload));
  }

  setWireMode(wireMode: AxtpWireMode): void {
    this.wireMode = wireMode;
  }

  setMaxFrameSize(maxFrameSize: number): void {
    this.messageFragmenter.setMaxFrameSize(maxFrameSize);
  }

  private sendMessage(message: Message): void {
    for (const frame of this.messageFragmenter.fragment(message)) {
      this.writer.writeBytes(this.frameEncoder.encode(frame));
    }
  }
}

type JsonValue = null | boolean | number | string | JsonValue[] | JsonObject;
type JsonObject = { [key: string]: JsonValue | undefined };

function asObject(value: JsonValue | undefined): JsonObject {
  if (value === null || typeof value !== "object" || Array.isArray(value)) throw new Error("expected object");
  return value;
}

function parseOp(object: JsonObject): RpcOp {
  if (typeof object.op !== "number" || !Number.isInteger(object.op) || object.op < 0 || object.op > 0xff) {
    throw new Error("invalid op");
  }
  return object.op as RpcOp;
}

function parseSid(object: JsonObject): string {
  return typeof object.sid === "string" ? object.sid : "";
}

function parseRequestId(d: JsonObject): number {
  if (typeof d.id !== "number" || !Number.isInteger(d.id) || d.id <= 0 || d.id > 0xffffffff) {
    throw new Error("invalid id");
  }
  return d.id;
}

function jsonToBytes(value: JsonValue | undefined): Bytes {
  return toBytes(JSON.stringify(value));
}

function bytesToJson(bytes: Bytes): JsonValue | undefined {
  if (bytes.length === 0) return undefined;
  try {
    return JSON.parse(bytesToText(bytes)) as JsonValue;
  } catch {
    return undefined;
  }
}

function statusObject(code: ErrorCode): JsonObject {
  const status: JsonObject = { ok: code === ErrorCode.Success, code };
  if (code !== ErrorCode.Success) {
    status.msg = RegistryLookup.errorByCode(code)?.name ?? "UNKNOWN_ERROR";
  }
  return status;
}
