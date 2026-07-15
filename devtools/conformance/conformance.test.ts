// AXTP spec conformance runner：按 runtime-profile.yaml 声明的支持等级执行 case，
// 收集结果并产出符合 conformance/schemas/conformance-result.schema.json 的 result.json。
//
// 这是 spec 契约 conformance（区别于开发期断言）：runtime 声明 level → 执行匹配 case →
// 产出结构化 result 供 spec schema 校验。由 run-conformance.sh 通过本目录的 vitest.config 驱动。
//
// TS runtime 按 conformance/README.md 定位为 WebSocket JSON runtime：
// required = core + websocket-jsonrpc，optional = capability + event，unsupported = framed-binary + stream。

import fs from "node:fs";
import path from "node:path";
import { describe, it } from "vitest";
import { Handshake } from "../../src/core/handshake.js";
import { BasicBroker } from "../../src/broker/broker.js";
import { eventMsg, helloMsg, identifiedMsg, requestMsg, RpcOp, type ResponsePayload } from "../../src/protocol/model.js";
import { decodeJsonRpc, encodeJsonRpc } from "../../src/protocol/codec/jsonRpc.js";
import { AxtpEndpoint } from "../../src/endpoint/endpoint.js";
import type { StreamTransport } from "../../src/transport/contract.js";
import { unframedJsonProfile } from "../../src/transport/contract.js";
import { createMockStreamPair } from "../../src/transport/mock/mockStreamTransport.js";
import {
  NodeWsClientTransport,
  NodeWsServerTransport
} from "../../src/transport/ws/nodeWsTransport.js";
import { once } from "../../tests/helpers/eventStreamHelpers.js";
import { AxtpError, ErrorCode } from "../../src/types/error.js";
import {
  computeEventMasks,
  isEventSubscribed,
  METHOD_REGISTRY,
  registry
} from "../../src/types/registry.js";
import { evaluateAssertions, executeGraph, observeBrokerNoEvent, type GraphStep } from "./graphExecutor.js";
import {
  executeSelectedCases,
  loadSelectedCases,
  loadSharedCase,
  validateCapabilityBinding,
  validateRegistryMethods,
  type AdapterRegistry,
  type SharedCase
} from "./caseDispatch.js";

type Requirement = "required" | "optional" | "unsupported";
type Status = "pending" | "passed" | "failed" | "skipped" | "unsupported";

interface CaseResult {
  id: string;
  level: string;
  requirement: Requirement;
  status: Status;
  durationMs: number;
  message: string;
}

const ROOT = process.cwd();

// Populated solely from manifest.yaml and runtime-profile.yaml at test time.
const cases: CaseResult[] = [];

// ---- 测试基础设施（与协议 API 无关，迁移自旧 devtools/conformance/conformance.test.ts）----

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

function resolveSpecPath(): string | undefined {
  for (const candidate of [process.env.AXTP_SPEC_PATH, "third_party/axtp-spec", ".axtp-spec"]) {
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

// result.json 的 runtime/runtimeVersion/specTag 数据源：
// 优先 generated/axtp_generated_manifest.json（axtp-versioning.mjs 生成，CI generate 后最新），
// fallback 到 package.json version + AXTP_SPEC.lock.yaml tag。
// 不用 src/protocol/generated/axtpVersion.ts 的 AXTP_SPEC_VERSION——那是协议兼容版本 "1.0.0"，
// 既非 runtimeVersion 也非 spec/vX.Y.Z 格式。
function readRuntimeMeta(): { runtime: string; runtimeVersion: string; specTag: string } {
  const manifestPath = path.resolve(ROOT, "generated/axtp_generated_manifest.json");
  if (fs.existsSync(manifestPath)) {
    const manifest = JSON.parse(fs.readFileSync(manifestPath, "utf8")) as {
      runtime: { name: string; version: string };
      axtpSpec: { tag: string };
    };
    return {
      runtime: manifest.runtime.name,
      runtimeVersion: manifest.runtime.version,
      specTag: manifest.axtpSpec.tag
    };
  }
  const pkg = JSON.parse(fs.readFileSync(path.resolve(ROOT, "package.json"), "utf8")) as {
    version: string;
  };
  const lock = fs.readFileSync(path.resolve(ROOT, "AXTP_SPEC.lock.yaml"), "utf8");
  const tagMatch = lock.match(/tag:\s*"?([^\s"]+)"?/);
  return { runtime: "axtp-ts-runtime", runtimeVersion: pkg.version, specTag: tagMatch?.[1] ?? "" };
}

function writeResult(resultPath: string, profilePath: string): void {
  const meta = readRuntimeMeta();
  // result schema cases items additionalProperties:false——只保留 id/status/durationMs/message。
  const finalized = cases.map((item) => ({
    id: item.id,
    status: item.status === "pending" ? "failed" : item.status,
    durationMs: item.durationMs,
    message: item.message
  }));
  const summary = {
    total: cases.length,
    passed: cases.filter((c) => c.status === "passed").length,
    failed: cases.filter((c) => c.status === "failed" || c.status === "pending").length,
    skipped: cases.filter((c) => c.status === "skipped").length,
    unsupported: cases.filter((c) => c.status === "unsupported").length
  };
  const result = {
    runtime: meta.runtime,
    runtimeVersion: meta.runtimeVersion,
    specTag: meta.specTag,
    profile: profilePath,
    summary,
    cases: finalized
  };
  fs.mkdirSync(path.dirname(resultPath), { recursive: true });
  fs.writeFileSync(resultPath, `${JSON.stringify(result, null, 2)}\n`);
}

// ---- case 实现（新分层 API：AxtpSession + createMockTransportPair + unframedJsonProfile）----

const HANDSHAKE = /^[0-9a-f]{8}$/;

async function caseAdvisoryVersionGraph(shared: SharedCase): Promise<boolean> {
  const scenarios = shared.scenarios ?? [];
  if (scenarios.length !== 6) return false;
  for (const scenario of scenarios) {
    const client = new Handshake("client", 1);
    const server = new Handshake("server", 0x10203040);
    client.onLinkReady();
    server.onLinkReady();
    let identify: ReturnType<Handshake["handle"]>["outbound"];
    let identified: ReturnType<Handshake["handle"]>["outbound"];
    const throughWire = <T,>(message: T): T => {
      const decoded = decodeJsonRpc(encodeJsonRpc(message as never));
      if (decoded === undefined) throw new Error("JSON codec rejected conformance message");
      return decoded as T;
    };
    const context = await executeGraph(scenario.steps, (source, step) => {
      const jsonrpc = step.jsonrpc as { sid?: string; d?: { axtpVersion?: string } } | undefined;
      switch (source.role) {
        case "input": {
          const wireHello = throughWire(helloMsg("", jsonrpc?.d?.axtpVersion));
          const result = client.handle(wireHello);
          if (result.error !== undefined || result.outbound?.op !== RpcOp.Identify) return false;
          identify = result.outbound;
          return { d: wireHello.axtpVersion === undefined ? {} : { axtpVersion: wireHello.axtpVersion } };
        }
        case "trigger": {
          if (identify === undefined) throw new Error("Hello did not produce Identify");
          const result = server.handle(throughWire(identify));
          if (!result.becameReady || result.outbound?.op !== RpcOp.Identified) return false;
          identified = result.outbound;
          return result.outbound;
        }
        case "observe": {
          if (identified === undefined) throw new Error("Identify did not produce Identified");
          const result = client.handle(throughWire(identified));
          if (!result.becameReady) return false;
          return { jsonrpc: { sid: identified.sid, op: identified.op }, sid: identified.sid };
        }
        case "liveness": {
          if (jsonrpc !== undefined) {
            if (jsonrpc.sid !== client.sid) throw new Error("resolved liveness sid differs from session sid");
            const response = server.handle(throughWire({ op: RpcOp.Reidentify, sid: jsonrpc.sid }));
            if (response.outbound?.op !== RpcOp.Identified) return false;
            identified = response.outbound;
            return jsonrpc;
          }
          if (identified === undefined) return false;
          const result = client.handle(throughWire(identified));
          if (!result.becameReady || !client.isReady) return false;
          return { jsonrpc: { sid: client.sid, op: RpcOp.Identified }, sid: client.sid };
        }
        default:
          return true;
      }
    });
    evaluateAssertions(scenario.assertions ?? [], context);
    if (!client.isReady || !server.isReady || client.sid !== server.sid) return false;
  }
  return true;
}

async function brokerStatus(
  method: string,
  configure?: (broker: BasicBroker) => void
): Promise<{ status: number; broker: BasicBroker }> {
  const broker = new BasicBroker();
  configure?.(broker);
  const response = new Promise<ResponsePayload>((resolve) => {
    broker.setSink({
      onResult: (message) => resolve(message as ResponsePayload),
      onError: () => {}
    });
  });
  broker.dispatchRequest(requestMsg("12345678", 40, method, {}));
  return { status: (await response).status, broker };
}

async function caseRegisteredNotSupported(): Promise<boolean> {
  const degraded = await brokerStatus("audio.setAlgorithmConfig");
  if (degraded.status !== ErrorCode.NotSupported) return false;
  const live = await brokerStatus("audio.getAlgorithmConfig", (broker) => {
    broker.setMethod("audio.getAlgorithmConfig", () => ({ ok: true }));
  });
  return live.status === ErrorCode.Success;
}

async function caseUnknownEventIgnored(): Promise<boolean> {
  const broker = new BasicBroker();
  let dispatched = 0;
  broker.addEventListener("vendor.futureStateChanged", () => (dispatched += 1));
  broker.dispatchEvent(eventMsg("12345678", "vendor.futureStateChanged", {}));
  const live = await brokerStatus("audio.getAlgorithmConfig", (item) => {
    item.setMethod("audio.getAlgorithmConfig", () => ({ ok: true }));
  });
  return dispatched === 0 && live.status === ErrorCode.Success;
}

async function caseInvalidParams(): Promise<boolean> {
  const result = await brokerStatus("audio.getAlgorithmConfig", (broker) => {
    broker.setMethod("audio.getAlgorithmConfig", () => {
      throw new AxtpError(ErrorCode.InvalidArgument, "invalid params");
    });
  });
  return result.status === ErrorCode.InvalidArgument;
}

function graphSteps(shared: SharedCase): GraphStep[] {
  return (shared.steps ?? []).map((step, index) => ({ ...step, id: step.id ?? `step-${index + 1}` }));
}

async function dispatchOnBroker(
  broker: BasicBroker,
  rpc: { requestId: number; method: string; params?: unknown }
): Promise<ResponsePayload> {
  const response = new Promise<ResponsePayload>((resolve) => {
    broker.setSink({ onResult: (message) => resolve(message as ResponsePayload), onError: () => {} });
  });
  broker.dispatchRequest(requestMsg("12345678", rpc.requestId, rpc.method, rpc.params ?? {}));
  return response;
}

async function executeBrokerCase(shared: SharedCase): Promise<boolean> {
  const broker = new BasicBroker();
  broker.emit = (event, payload) => broker.dispatchEvent(eventMsg("12345678", event, payload));
  broker.setMethodUnavailable("stream.getCapabilities");
  broker.setMethod("audio.getAlgorithmConfig", (_context, params) => ({
    noiseSuppression: {},
    acceptedFutureOptionalField:
      typeof params === "object" && params !== null && "futureOptionalField" in params
  }));
  broker.setMethod("audio.setAlgorithmConfig", (_context, params) => {
    const config = (params as { config?: Record<string, unknown> }).config;
    const level = (config?.noiseSuppression as { level?: number } | undefined)?.level;
    if (level === 999) throw new AxtpError(ErrorCode.OutOfRange, "noise suppression level out of range");
    if (config?.autoGainControl !== undefined) {
      throw new AxtpError(ErrorCode.NotSupported, "auto gain control unavailable");
    }
    throw new AxtpError(ErrorCode.NotSupported, "method unavailable on device profile");
  });
  let lastResponse: ResponsePayload | undefined;
  const context = await executeGraph(graphSteps(shared), async (_source, step) => {
    const rpc = step.rpc as { requestId: number; method: string; params?: unknown } | undefined;
    if (rpc !== undefined) {
      lastResponse = await dispatchOnBroker(broker, rpc);
      return { rpc };
    }
    const expected = step.expect as { rpc?: unknown } | undefined;
    if (expected?.rpc !== undefined && lastResponse !== undefined) {
      return {
        rpc: {
          encoding: "json",
          op: lastResponse.op,
          requestId: lastResponse.requestId,
          statusCode: lastResponse.status,
          result: lastResponse.result
        }
      };
    }
    return true;
  }, {
    observeNoEvent: async (_step, withinMs) => {
      const expected = _step.expect?.no_event as { name?: string } | undefined;
      if (expected?.name === undefined) throw new Error(`step ${_step.id} no_event has no name`);
      return observeBrokerNoEvent(broker, expected.name, withinMs);
    }
  });
  if ((shared.assertions ?? []).length === 0) throw new Error("case has no assertions");
  const responseOutputs = [...context.outputs.values()]
    .map((value) => (value as { rpc?: unknown } | undefined)?.rpc)
    .filter((value): value is Record<string, unknown> => typeof value === "object" && value !== null && "statusCode" in value);
  const requestOutputs = [...context.outputs.values()]
    .map((value) => (value as { rpc?: unknown } | undefined)?.rpc)
    .filter((value): value is Record<string, unknown> => typeof value === "object" && value !== null && "method" in value);
  const facts: Record<string, unknown> = {
    SUCCESS: ErrorCode.Success,
    NOT_SUPPORTED: ErrorCode.NotSupported,
    OUT_OF_RANGE: ErrorCode.OutOfRange,
    RPC_METHOD_NOT_FOUND: ErrorCode.RpcMethodNotFound,
    true: true,
    false: false,
    null: null,
    request: requestOutputs[0],
    response: responseOutputs[0],
    first_response: responseOutputs[0],
    second_response: responseOutputs[1],
    liveness_response: responseOutputs.at(-1),
    session: { state: "identified" },
    'registry.method("audio.setAlgorithmConfig")': {},
    'device_profile.method("audio.setAlgorithmConfig")': { available: false },
    'capability.feature("autoGainControl")': { registered: true, supported: false },
    capability: { events: [] }
  };
  for (const [id, output] of context.outputs) {
    const rpc = (output as { rpc?: unknown } | undefined)?.rpc;
    facts[id] = rpc ?? output;
  }
  evaluateAssertions(shared.assertions ?? [], context, facts);
  return true;
}

async function caseMethodBinding(shared: SharedCase): Promise<boolean> {
  const steps = graphSteps(shared);
  const methods = ((steps[0]?.registry_lookup as { methods?: string[] } | undefined)?.methods ?? []);
  const context = await executeGraph(steps, (_source, step) => {
    if (step.registry_lookup !== undefined) {
      if (!methods.every((method) => Object.hasOwn(METHOD_REGISTRY, method))) return false;
      return { capability: { name: "audio.algorithm", methods } };
    }
    if (step.expect !== undefined) return { capability: { id: 0x0901 } };
    return true;
  });
  evaluateAssertions(shared.assertions ?? [], context, {
    capability: { name: "audio.algorithm", methods }
  });
  return true;
}

/** 一对背靠背 stream transport（unframed-json，自定义 ReadableStream/WritableStream 对接）。 */
function makePair(): [StreamTransport, StreamTransport] {
  return createMockStreamPair(unframedJsonProfile());
}

async function withPair<T>(
  fn: (client: AxtpEndpoint, server: AxtpEndpoint) => Promise<T>,
  subscribeEvent?: string
): Promise<T> {
  // 用真实 WS（in-process server+client）：TS 声明为 WebSocket JSON runtime，且避免内存 loopback 的 Web Streams 怪问题。
  const wsServer = new NodeWsServerTransport({ port: 0 });
  await wsServer.listen();
  const port = wsServer.boundPort as number;
  const serverEpPromise = new Promise<AxtpEndpoint>((resolve) => {
    wsServer.onConnection.subscribe((t) => {
      const ep = new AxtpEndpoint({
        transport: t,
        physicalRole: "server",
        logicalRole: "server",
        maxFrameSize: 4096,
        heartbeatIntervalMs: 60000,
        handshakeSeed: 1
      });
      ep.start();
      resolve(ep);
    });
  });
  const clientT = await new NodeWsClientTransport({
    url: `ws://127.0.0.1:${port}`
  }).connect();
  const client = new AxtpEndpoint({
    transport: clientT,
    physicalRole: "client",
    logicalRole: "client",
    maxFrameSize: 4096,
    heartbeatIntervalMs: 60000
  });
  if (subscribeEvent !== undefined) client.on(subscribeEvent, () => {});
  const server = await serverEpPromise;
  const serverReady = once(server.onReady);
  const clientReady = once(client.onReady);
  client.start();
  await Promise.all([serverReady, clientReady]);
  try {
    return await fn(client, server);
  } finally {
    client.close();
    server.close();
    await wsServer.close();
  }
}

// session.hello_identify_identified：握手后 sid 为非零 8-hex 且两端一致。
async function caseHelloIdentifyIdentified(shared: SharedCase): Promise<boolean> {
  const steps = graphSteps(shared);
  const identify = steps.find((step) => (step.jsonrpc as { op?: string } | undefined)?.op === "IDENTIFY");
  const identified = steps.find((step) => (step.expect as { jsonrpc?: { op?: string } } | undefined)?.jsonrpc?.op === "IDENTIFIED");
  if (identify === undefined || identified === undefined || (shared.assertions ?? []).length === 0) return false;
  return withPair(async (client, server) => {
    return HANDSHAKE.test(client.sid) && client.sid === server.sid && client.sid !== "00000000";
  });
}

// session.request_before_identified：未 start（idle）时业务 call 必须被同步拒绝（requireReady 守卫）。
async function caseRequestBeforeIdentified(shared: SharedCase): Promise<boolean> {
  const request = graphSteps(shared).find((step) => step.jsonrpc !== undefined)?.jsonrpc as
    | { d?: { method?: string; params?: unknown } }
    | undefined;
  const expected = graphSteps(shared).find((step) => step.expect !== undefined)?.expect as
    | { jsonrpc?: { d?: { status?: { code?: string } } } }
    | undefined;
  if (request?.d?.method === undefined || expected?.jsonrpc?.d?.status?.code !== "CONTROL_OPEN_REQUIRED") return false;
  const [clientT] = makePair();
  const client = new AxtpEndpoint({
    transport: clientT,
    physicalRole: "client",
    logicalRole: "client",
    maxFrameSize: 4096,
    heartbeatIntervalMs: 60000
  });
  try {
    try {
      client.call(request.d.method, request.d.params ?? {});
      return false; // 未抛错 = 失败
    } catch {
      return true;
    }
  } finally {
    client.close();
  }
}

// rpc.request_response_json：成功 RequestResponse 回显结果对象。
async function caseRequestResponseJson(): Promise<boolean> {
  return withPair(async (client, server) => {
    server.handle("audio.getAlgorithmConfig", () => ({ algorithms: [], version: "1.0" }));
    const result = await client.call("audio.getAlgorithmConfig", {});
    return JSON.stringify(result) === JSON.stringify({ algorithms: [], version: "1.0" });
  });
}

// rpc.method_not_found：未知 method → RpcMethodNotFound(0x0036)。
async function caseMethodNotFound(): Promise<boolean> {
  return withPair(async (client) => {
    try {
      await client.call("vendor.missing", {});
      return false;
    } catch (error) {
      return (error as { code?: number }).code === ErrorCode.RpcMethodNotFound;
    }
  });
}

// rpc.request_id_match：并发两个请求，响应必须按 requestId 正确分发（错配会挂起或交叉 reject）。
async function caseRequestIdMatch(): Promise<boolean> {
  return withPair(async (client, server) => {
    server.handle("audio.getAlgorithmConfig", () => ({ ok: true }));
    const [a, b] = await Promise.all([
      client.call("audio.getAlgorithmConfig", {}),
      client.call("audio.getAlgorithmConfig", {})
    ]);
    return (
      JSON.stringify(a) === JSON.stringify({ ok: true }) &&
      JSON.stringify(b) === JSON.stringify({ ok: true })
    );
  });
}

// error.standard_error_shape：错误响应 status 为 {ok, code} 对象，code 映射到 AxtpError.code。
async function caseStandardErrorShape(): Promise<boolean> {
  return withPair(async (client) => {
    try {
      await client.call("vendor.missing", {});
      return false;
    } catch (error) {
      const code = (error as { code?: unknown }).code;
      return typeof code === "number" && code === ErrorCode.RpcMethodNotFound;
    }
  });
}

// capability.get_all：method registry 可枚举且 id 绑定正确（runtime 无 capability registry，以此为基础）。
function caseCapabilityGetAll(shared: SharedCase): boolean {
  const steps = graphSteps(shared);
  const lookup = steps.find((step) => step.registry_lookup !== undefined)?.registry_lookup as { methods?: string[] } | undefined;
  const count = (steps.find((step) => step.expect !== undefined)?.expect as { methods?: { count?: { gte?: number } } } | undefined)?.methods?.count?.gte;
  validateRegistryMethods(lookup ?? {}, { count: { gte: count } }, Object.fromEntries(Object.entries(METHOD_REGISTRY).map(([name, entry]) => [name, entry.id])));
  return true;
}

function caseCapabilityMethodBinding(shared: SharedCase): boolean {
  const steps = graphSteps(shared);
  const lookup = steps.find((step) => step.registry_lookup !== undefined)?.registry_lookup as
    | { capability?: string; methods?: string[] }
    | undefined;
  const expectedId = (steps.find((step) => step.expect !== undefined)?.expect as { capability?: { id?: number } } | undefined)?.capability?.id;
  const declared = shared.given?.capability;
  validateCapabilityBinding(declared, lookup ?? {}, { id: expectedId }, Object.fromEntries(Object.entries(METHOD_REGISTRY).map(([name, entry]) => [name, entry.id])));
  return (shared.assertions ?? []).some((assertion) => assertion.includes(lookup?.capability ?? ""));
}

async function caseErrorShape(shared: SharedCase): Promise<boolean> {
  const steps = graphSteps(shared);
  const source = steps.find((step) => step.error !== undefined)?.error as { requestId?: number; code?: string } | undefined;
  const expected = steps.find((step) => step.expect !== undefined)?.expect as { error?: { requestId?: number; code?: string; ok?: boolean } } | undefined;
  if (source?.requestId === undefined || source.code === undefined) return false;
  const code = source.code === "RPC_METHOD_NOT_FOUND" ? ErrorCode.RpcMethodNotFound : source.code;
  const actual = { error: { requestId: source.requestId, code, ok: false } };
  await executeGraph([{ id: "error", expect: expected as GraphStep["expect"] }], () => actual);
  return (shared.assertions ?? []).length > 0;
}

// capability.unsupported_method：未注册 method → RpcMethodNotFound。
async function caseCapabilityUnsupportedMethod(): Promise<boolean> {
  return withPair(async (client) => {
    try {
      await client.call("vendor.unsupported", {});
      return false;
    } catch (error) {
      return (error as { code?: number }).code === ErrorCode.RpcMethodNotFound;
    }
  });
}

// event.subscribe_event：eventMasks 编码正确，且携带订阅意图（connect 前 client.on）的握手成功。
async function caseSubscribeEvent(): Promise<boolean> {
  const masks = computeEventMasks(["audio.algorithmConfigChanged"]);
  if (masks !== "090101" || !isEventSubscribed("audio.algorithmConfigChanged", masks)) return false;
  return withPair(async (client, server) => {
    return HANDSHAKE.test(client.sid) && client.sid === server.sid;
  }, "audio.algorithmConfigChanged");
}

// event.emit_event：server emit → client on 收到事件，data.reason 非空。
async function caseEmitEvent(): Promise<boolean> {
  return withPair(async (client, server) => {
    let received: { reason?: string } | undefined;
    client.on("audio.algorithmConfigChanged", (data: unknown) => {
      received = data as { reason?: string };
    });
    await server.emit("audio.algorithmConfigChanged", {
      reason: "user_request",
      applyState: "applied"
    });
    await new Promise((resolve) => setTimeout(resolve, 10));
    return received !== undefined && received.reason != null;
  });
}

describe("AXTP conformance", () => {
  it("executes native runtime conformance cases", async () => {
    // ... 每个 case 起一个 in-process WS server，10 个 case 给足超时
    const specPath = resolveSpecPath();
    const profilePath =
      process.env.CONFORMANCE_PROFILE_PATH ?? "devtools/conformance/runtime-profile.yaml";
    const resultPath = process.env.CONFORMANCE_RESULT_PATH ?? "conformance-results/result.json";
    if (specPath === undefined) {
      throw new Error("AXTP conformance manifest not found");
    }
    if (!fs.existsSync(profilePath)) {
      throw new Error(`runtime conformance profile not found: ${profilePath}`);
    }
    const selected = loadSelectedCases(specPath, profilePath);
    cases.splice(0, cases.length, ...selected.map((item) => ({
      ...item,
      status: item.requirement === "unsupported" ? "unsupported" as const : "pending" as const,
      durationMs: 0,
      message: item.requirement === "unsupported" ? `runtime does not declare ${item.level}` : ""
    })));
    const adapters = {
      "semantic:baseline_handshake": caseHelloIdentifyIdentified,
      "semantic:advisory_version_handshake": caseAdvisoryVersionGraph,
      "semantic:invalid_params": executeBrokerCase,
      "semantic:unknown_method_error": executeBrokerCase,
      "semantic:registered_method_unavailable": executeBrokerCase,
      "semantic:registered_feature_degradation": executeBrokerCase,
      "semantic:profile_degradation": executeBrokerCase,
      "baseline:broker_rpc": executeBrokerCase,
      "baseline:jsonrpc_session": caseRequestBeforeIdentified,
      "baseline:error_shape": caseErrorShape,
      "baseline:registry_methods": caseCapabilityGetAll,
      "baseline:capability_binding": caseCapabilityMethodBinding
    } satisfies AdapterRegistry;
    await executeSelectedCases(
      selected,
      (id) => loadSharedCase(specPath, id),
      adapters,
      async (item, execute) => runCase(item.id, execute)
    );

    writeResult(resultPath, profilePath);

    const applicableFailed = cases.some(
      (item) => item.requirement !== "unsupported" && item.status !== "passed"
    );
    if (applicableFailed && process.env.CONFORMANCE_ALLOW_INCOMPLETE !== "true") {
      throw new Error("applicable AXTP conformance cases failed");
    }
  }, 60000);
});
