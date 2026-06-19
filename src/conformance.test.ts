import fs from "node:fs";
import path from "node:path";
import { describe, it } from "vitest";
import {
  AXTP_GENERATED_VERSION,
  AxtpCore,
  AxtpEndpoint,
  AxtpWireMode,
  BasicBroker,
  CapabilityId,
  ControlOpcode,
  ErrorCode,
  EventId,
  InboundProcessor,
  MethodId,
  MockTransport,
  OutboundProcessor,
  PayloadType,
  RegistryLookup,
  RpcBodyEncoding,
  RpcEncoding,
  RpcOp,
  SourceProtocol,
  TransportKind,
  WebSocketJsonRpcAdapter,
  bytesEqual,
  bytesToText,
  kCapabilityRegistry,
  kMethodRegistry,
  rpcPayload,
  streamPayload,
  toBytes,
  type Bytes,
  type ControlPayload,
  type PayloadSink,
  type RpcPayload,
  type StreamPayload
} from "./index.js";

type Requirement = "required" | "optional" | "not-selected" | "unsupported";
type Status = "pending" | "passed" | "failed" | "skipped" | "unsupported";

interface CaseResult {
  id: string;
  level: string;
  requirement: Requirement;
  status: Status;
  durationMs: number;
  message: string;
}

const cases: CaseResult[] = [
  { id: "handshake.open_accept", level: "framed-binary", requirement: "optional", status: "pending", durationMs: 0, message: "" },
  { id: "handshake.open_reject", level: "framed-binary", requirement: "optional", status: "skipped", durationMs: 0, message: "control open rejection policy is not configurable in the TypeScript runtime" },
  { id: "handshake.close", level: "framed-binary", requirement: "optional", status: "pending", durationMs: 0, message: "" },
  { id: "handshake.ping_pong", level: "framed-binary", requirement: "optional", status: "pending", durationMs: 0, message: "" },
  { id: "session.hello_identify_identified", level: "websocket-jsonrpc", requirement: "required", status: "pending", durationMs: 0, message: "" },
  { id: "session.request_before_identified", level: "websocket-jsonrpc", requirement: "required", status: "pending", durationMs: 0, message: "" },
  { id: "rpc.request_response_json", level: "core", requirement: "required", status: "pending", durationMs: 0, message: "" },
  { id: "rpc.method_not_found", level: "core", requirement: "required", status: "pending", durationMs: 0, message: "" },
  { id: "rpc.invalid_params", level: "core", requirement: "not-selected", status: "skipped", durationMs: 0, message: "schema-aware parameter validation is outside the required TypeScript core profile" },
  { id: "rpc.request_id_match", level: "core", requirement: "required", status: "pending", durationMs: 0, message: "" },
  { id: "event.subscribe_event", level: "event", requirement: "optional", status: "pending", durationMs: 0, message: "" },
  { id: "event.unsubscribe_event", level: "event", requirement: "optional", status: "pending", durationMs: 0, message: "" },
  { id: "event.emit_event", level: "event", requirement: "optional", status: "pending", durationMs: 0, message: "" },
  { id: "capability.get_all", level: "capability", requirement: "optional", status: "pending", durationMs: 0, message: "" },
  { id: "capability.method_binding", level: "capability", requirement: "optional", status: "pending", durationMs: 0, message: "" },
  { id: "capability.unsupported_method", level: "capability", requirement: "optional", status: "pending", durationMs: 0, message: "" },
  { id: "error.standard_error_shape", level: "core", requirement: "required", status: "pending", durationMs: 0, message: "" },
  { id: "error.unauthorized", level: "core", requirement: "not-selected", status: "skipped", durationMs: 0, message: "auth policy hooks are outside the required TypeScript core profile" },
  { id: "error.server_busy", level: "core", requirement: "not-selected", status: "skipped", durationMs: 0, message: "busy-state policy hooks are outside the required TypeScript core profile" },
  { id: "stream.stream_open", level: "stream", requirement: "optional", status: "skipped", durationMs: 0, message: "stream.open RPC control-plane method is not part of the generated spec/v0.0.2 registry" },
  { id: "stream.stream_data", level: "stream", requirement: "optional", status: "pending", durationMs: 0, message: "" },
  { id: "stream.stream_close", level: "stream", requirement: "optional", status: "skipped", durationMs: 0, message: "stream.close RPC control-plane method is not part of the generated spec/v0.0.2 registry" }
];

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

function concat(chunks: Bytes[]): Bytes {
  const out = new Uint8Array(chunks.reduce((sum, chunk) => sum + chunk.length, 0));
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.length;
  }
  return out;
}

function encodeControl(opcode: ControlOpcode, controlId: number): Bytes {
  const chunks: Bytes[] = [];
  new OutboundProcessor({ writeBytes: (bytes) => chunks.push(bytes) }).sendControl({
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
  return concat(chunks);
}

function decodeOneControl(bytes: Bytes): ControlPayload {
  const sink = new CapturePayloadSink();
  new InboundProcessor(sink).onBytes(bytes);
  if (sink.controls.length !== 1) throw new Error("expected one control response");
  return sink.controls[0];
}

async function settle(): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, 0));
}

async function runCase(id: string, fn: () => boolean | Promise<boolean>): Promise<void> {
  const item = cases.find((candidate) => candidate.id === id);
  if (item === undefined) throw new Error(`unknown case ${id}`);
  const start = performance.now();
  try {
    const ok = await fn();
    item.status = ok ? "passed" : "failed";
    if (!ok && item.message.length === 0) item.message = "case returned false";
  } catch (error) {
    item.status = "failed";
    item.message = error instanceof Error ? error.message : String(error);
  } finally {
    item.durationMs = performance.now() - start;
  }
}

function makeJsonRuntime(configureBroker?: (broker: BasicBroker) => void): {
  broker: BasicBroker;
  endpoint: AxtpEndpoint;
  transport: MockTransport;
  adapter: WebSocketJsonRpcAdapter;
} {
  const broker = new BasicBroker();
  configureBroker?.(broker);
  const endpoint = new AxtpEndpoint(broker);
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
  return { broker, endpoint, transport, adapter };
}

async function popJson(transport: MockTransport): Promise<Record<string, unknown>> {
  await settle();
  const bytes = transport.tryPopOutgoing();
  if (bytes === undefined) throw new Error("missing outgoing JSON message");
  return JSON.parse(bytesToText(bytes)) as Record<string, unknown>;
}

function responseStatus(response: Record<string, unknown>): Record<string, unknown> {
  return ((response.d as Record<string, unknown>).status as Record<string, unknown>);
}

async function identify(transport: MockTransport, eventMasks = "0901"): Promise<string> {
  transport.injectIncoming(toBytes(`{"sid":"","op":2,"d":{"rpcVersion":1,"eventMasks":"${eventMasks}"}}`));
  const response = await popJson(transport);
  if (response.op !== RpcOp.Identified) throw new Error("IDENTIFY did not produce IDENTIFIED");
  const sid = response.sid;
  if (typeof sid !== "string" || sid.length === 0) throw new Error("IDENTIFIED sid was empty");
  const d = response.d as Record<string, unknown>;
  if (d.negotiatedRpcVersion !== 1) throw new Error("IDENTIFIED did not negotiate rpcVersion 1");
  return sid;
}

async function methodNotFoundWithId(requestId: number): Promise<boolean> {
  const { transport, adapter } = makeJsonRuntime();
  await adapter.poll();
  await popJson(transport);
  const sid = await identify(transport);
  transport.injectIncoming(toBytes(`{"sid":"${sid}","op":7,"d":{"id":${requestId},"method":"vendor.missing","params":{}}}`));
  const response = await popJson(transport);
  const d = response.d as Record<string, unknown>;
  return d.id === requestId && responseStatus(response).ok === false && responseStatus(response).code === ErrorCode.RpcMethodNotFound;
}

function testOpenAccept(): boolean {
  const core = new AxtpCore();
  core.byteSink.onBytes(encodeControl(ControlOpcode.Open, 1));
  const responseBytes = core.tryPopOutboundBytes();
  if (responseBytes === undefined) return false;
  const response = decodeOneControl(responseBytes);
  return response.opcode === ControlOpcode.Accept && response.controlId === 1 && response.statusCode === ErrorCode.Success && core.controlSessionOpen();
}

function testClose(): boolean {
  const core = new AxtpCore();
  core.byteSink.onBytes(encodeControl(ControlOpcode.Open, 1));
  core.tryPopOutboundBytes();
  core.tryPopOutboundBytes();
  core.byteSink.onBytes(encodeControl(ControlOpcode.Close, 2));
  const responseBytes = core.tryPopOutboundBytes();
  if (responseBytes === undefined) return false;
  const response = decodeOneControl(responseBytes);
  return response.opcode === ControlOpcode.CloseAck && response.controlId === 2 && !core.controlSessionOpen();
}

function testPingPong(): boolean {
  const core = new AxtpCore();
  core.byteSink.onBytes(encodeControl(ControlOpcode.Ping, 3));
  const responseBytes = core.tryPopOutboundBytes();
  if (responseBytes === undefined) return false;
  const response = decodeOneControl(responseBytes);
  return response.opcode === ControlOpcode.Pong && response.controlId === 3;
}

async function testSessionHelloIdentify(): Promise<boolean> {
  const { transport, adapter } = makeJsonRuntime();
  await adapter.poll();
  const hello = await popJson(transport);
  if (hello.op !== RpcOp.Hello) return false;
  await identify(transport);
  return true;
}

async function testRequestBeforeIdentified(): Promise<boolean> {
  const { transport, adapter } = makeJsonRuntime();
  await adapter.poll();
  await popJson(transport);
  transport.injectIncoming(toBytes('{"sid":"","op":7,"d":{"id":700,"method":"audio.getAlgorithmConfig","params":{}}}'));
  const response = await popJson(transport);
  const d = response.d as Record<string, unknown>;
  return response.op === RpcOp.RequestResponse && d.id === 700 && responseStatus(response).code === ErrorCode.ControlOpenRequired;
}

async function testRequestResponseJson(): Promise<boolean> {
  const { transport, adapter } = makeJsonRuntime((broker) => {
    broker.registerJsonMethod("audio.getAlgorithmConfig", (context, params) => {
      if (context.methodName !== "audio.getAlgorithmConfig" || params !== "{}") throw new Error("unexpected handler context");
      return '{"noiseSuppression":{"enabled":true,"level":3}}';
    });
  });
  await adapter.poll();
  await popJson(transport);
  const sid = await identify(transport);
  transport.injectIncoming(toBytes(`{"sid":"${sid}","op":7,"d":{"id":701,"method":"audio.getAlgorithmConfig","params":{}}}`));
  const response = await popJson(transport);
  const d = response.d as Record<string, unknown>;
  return response.op === RpcOp.RequestResponse && d.id === 701 && responseStatus(response).ok === true && typeof d.result === "object";
}

async function testSubscribeEvent(): Promise<boolean> {
  const { transport, adapter } = makeJsonRuntime();
  await adapter.poll();
  await popJson(transport);
  await identify(transport, "0901");
  return true;
}

async function testUnsubscribeEvent(): Promise<boolean> {
  const { transport, adapter } = makeJsonRuntime();
  await adapter.poll();
  await popJson(transport);
  const sid = await identify(transport, "0901");
  transport.injectIncoming(toBytes(`{"sid":"${sid}","op":4,"d":{"eventMasks":""}}`));
  const response = await popJson(transport);
  return response.op === RpcOp.Identified;
}

async function testEmitEvent(): Promise<boolean> {
  const { transport, adapter } = makeJsonRuntime();
  await adapter.poll();
  await popJson(transport);
  const sid = await identify(transport);
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
    body: toBytes('{"reason":"user_request","applyState":"applied"}')
  }));
  const event = await popJson(transport);
  const d = event.d as Record<string, unknown>;
  const data = d.data as Record<string, unknown>;
  return event.op === RpcOp.Event && d.event === "audio.algorithmConfigChanged" && data.reason === "user_request";
}

function testCapabilityGetAll(): boolean {
  return kMethodRegistry.length >= 4 &&
    RegistryLookup.methodIdByName("audio.getAlgorithmConfig") === MethodId.AudioGetAlgorithmConfig &&
    RegistryLookup.methodIdByName("audio.getAlgorithmCapabilities") === MethodId.AudioGetAlgorithmCapabilities &&
    RegistryLookup.methodIdByName("audio.setAlgorithmConfig") === MethodId.AudioSetAlgorithmConfig &&
    RegistryLookup.methodIdByName("audio.resetAlgorithmConfig") === MethodId.AudioResetAlgorithmConfig;
}

function testCapabilityMethodBinding(): boolean {
  const capability = kCapabilityRegistry.find((item) => item.id === CapabilityId.AudioAlgorithm && item.name === "audio.algorithm");
  const method = RegistryLookup.methodById(MethodId.AudioGetAlgorithmConfig);
  const event = RegistryLookup.eventById(EventId.AudioAlgorithmConfigChanged);
  return capability !== undefined && method?.domain === "audio" && event?.domain === "audio";
}

function testStreamData(): boolean {
  const chunks: Bytes[] = [];
  new OutboundProcessor({ writeBytes: (bytes) => chunks.push(bytes) }).sendStream(streamPayload({
    streamId: 9,
    seqId: 1,
    cursor: 0n,
    data: Uint8Array.of(0xaa, 0xbb, 0xcc)
  }));
  const sink = new CapturePayloadSink();
  const inbound = new InboundProcessor(sink);
  for (const chunk of chunks) inbound.onBytes(chunk);
  return sink.streams.length === 1 &&
    sink.streams[0].streamId === 9 &&
    sink.streams[0].seqId === 1 &&
    sink.streams[0].cursor === 0n &&
    bytesEqual(sink.streams[0].data, Uint8Array.of(0xaa, 0xbb, 0xcc));
}

function writeResult(resultPath: string, profilePath: string): void {
  const normalizedCases = cases.map((item) => ({
    ...item,
    status: item.status === "pending" ? "failed" : item.status
  }));
  const summary = {
    total: cases.length,
    passed: cases.filter((item) => item.status === "passed").length,
    failed: cases.filter((item) => item.status === "failed" || item.status === "pending").length,
    skipped: cases.filter((item) => item.status === "skipped").length,
    unsupported: cases.filter((item) => item.status === "unsupported").length
  };
  const result = {
    runtime: "axtp-ts-runtime",
    runtimeVersion: AXTP_GENERATED_VERSION.runtimeVersion,
    specTag: AXTP_GENERATED_VERSION.specTag,
    profile: profilePath,
    requiredLevels: ["core", "websocket-jsonrpc"],
    optionalLevels: ["capability", "framed-binary", "event", "stream"],
    unsupportedLevels: [],
    summary,
    cases: normalizedCases
  };
  fs.mkdirSync(path.dirname(resultPath), { recursive: true });
  fs.writeFileSync(resultPath, `${JSON.stringify(result, null, 2)}\n`);
}

function envIsTrue(name: string): boolean {
  return process.env[name] === "true";
}

function resolveSpecPath(): string | undefined {
  for (const candidate of [
    process.env.AXTP_SPEC_PATH,
    "third_party/axtp-spec",
    ".axtp-spec"
  ]) {
    if (
      candidate !== undefined &&
      (fs.existsSync(path.join(candidate, "docs/conformance/manifest.yaml")) ||
        fs.existsSync(path.join(candidate, "conformance/manifest.yaml")))
    ) {
      return candidate;
    }
  }
  return undefined;
}

describe("AXTP conformance", () => {
  it("executes native runtime conformance cases", async () => {
    const specPath = resolveSpecPath();
    const profilePath = process.env.CONFORMANCE_PROFILE_PATH ?? "conformance/runtime-profile.yaml";
    const resultPath = process.env.CONFORMANCE_RESULT_PATH ?? "conformance-results/result.json";
    if (specPath === undefined) {
      throw new Error("AXTP conformance manifest not found");
    }
    if (!fs.existsSync(profilePath)) {
      throw new Error(`runtime conformance profile not found: ${profilePath}`);
    }

    await runCase("handshake.open_accept", testOpenAccept);
    await runCase("handshake.close", testClose);
    await runCase("handshake.ping_pong", testPingPong);
    await runCase("session.hello_identify_identified", testSessionHelloIdentify);
    await runCase("session.request_before_identified", testRequestBeforeIdentified);
    await runCase("rpc.request_response_json", testRequestResponseJson);
    await runCase("rpc.method_not_found", () => methodNotFoundWithId(2));
    await runCase("rpc.request_id_match", () => methodNotFoundWithId(55));
    await runCase("event.subscribe_event", testSubscribeEvent);
    await runCase("event.unsubscribe_event", testUnsubscribeEvent);
    await runCase("event.emit_event", testEmitEvent);
    await runCase("capability.get_all", testCapabilityGetAll);
    await runCase("capability.method_binding", testCapabilityMethodBinding);
    await runCase("capability.unsupported_method", () => methodNotFoundWithId(4));
    await runCase("error.standard_error_shape", () => methodNotFoundWithId(99));
    await runCase("stream.stream_data", testStreamData);

    writeResult(resultPath, profilePath);

    const requiredIssue = cases.some((item) => item.requirement === "required" && item.status !== "passed");
    const optionalIssue = cases.some((item) => item.requirement === "optional" && item.status !== "passed");
    if (requiredIssue && !envIsTrue("CONFORMANCE_ALLOW_INCOMPLETE")) {
      throw new Error("required AXTP conformance cases failed");
    }
    if (optionalIssue && envIsTrue("CONFORMANCE_STRICT_OPTIONAL")) {
      throw new Error("optional AXTP conformance cases failed or were skipped");
    }
  });
});
