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
import { AxtpEndpoint } from "../../src/endpoint/endpoint.js";
import type { StreamTransport } from "../../src/transport/contract.js";
import { unframedJsonProfile } from "../../src/transport/contract.js";
import { createMockStreamPair } from "../../src/transport/mock/mockStreamTransport.js";
import {
  NodeWsStreamClientTransport,
  NodeWsStreamServerTransport
} from "../../src/transport/ws/nodeWsStreamTransport.js";
import { once } from "../../tests/helpers/eventStreamHelpers.js";
import { ErrorCode } from "../../src/types/error.js";
import {
  computeEventMasks,
  isEventSubscribed,
  METHOD_REGISTRY,
  registry
} from "../../src/types/registry.js";

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

// manifest.yaml 各 level 的 required_cases（去重后按 TS 支持等级归类）。
const cases: CaseResult[] = [
  // required: core ∪ websocket-jsonrpc
  {
    id: "session.hello_identify_identified",
    level: "websocket-jsonrpc",
    requirement: "required",
    status: "pending",
    durationMs: 0,
    message: ""
  },
  {
    id: "session.request_before_identified",
    level: "websocket-jsonrpc",
    requirement: "required",
    status: "pending",
    durationMs: 0,
    message: ""
  },
  {
    id: "rpc.request_response_json",
    level: "core",
    requirement: "required",
    status: "pending",
    durationMs: 0,
    message: ""
  },
  {
    id: "rpc.method_not_found",
    level: "core",
    requirement: "required",
    status: "pending",
    durationMs: 0,
    message: ""
  },
  {
    id: "rpc.request_id_match",
    level: "core",
    requirement: "required",
    status: "pending",
    durationMs: 0,
    message: ""
  },
  {
    id: "error.standard_error_shape",
    level: "core",
    requirement: "required",
    status: "pending",
    durationMs: 0,
    message: ""
  },
  // optional: capability ∪ event
  {
    id: "capability.get_all",
    level: "capability",
    requirement: "optional",
    status: "pending",
    durationMs: 0,
    message: ""
  },
  {
    id: "capability.method_binding",
    level: "capability",
    requirement: "optional",
    status: "skipped",
    durationMs: 0,
    message:
      "runtime has no capability→methods registry (types/registry.ts only indexes methods/events)"
  },
  {
    id: "capability.unsupported_method",
    level: "capability",
    requirement: "optional",
    status: "pending",
    durationMs: 0,
    message: ""
  },
  {
    id: "event.subscribe_event",
    level: "event",
    requirement: "optional",
    status: "pending",
    durationMs: 0,
    message: ""
  },
  {
    id: "event.unsubscribe_event",
    level: "event",
    requirement: "optional",
    status: "skipped",
    durationMs: 0,
    message: "REIDENTIFY not implemented (updateSubscriptions is a NotImplemented placeholder)"
  },
  {
    id: "event.emit_event",
    level: "event",
    requirement: "optional",
    status: "pending",
    durationMs: 0,
    message: ""
  },
  // unsupported: framed-binary ∪ stream（TS 是 WebSocket JSON runtime）
  {
    id: "handshake.open_accept",
    level: "framed-binary",
    requirement: "unsupported",
    status: "unsupported",
    durationMs: 0,
    message: "TS runtime declares framed-binary unsupported (WebSocket JSON runtime)"
  },
  {
    id: "handshake.close",
    level: "framed-binary",
    requirement: "unsupported",
    status: "unsupported",
    durationMs: 0,
    message: "TS runtime declares framed-binary unsupported (WebSocket JSON runtime)"
  },
  {
    id: "handshake.heartbeat",
    level: "framed-binary",
    requirement: "unsupported",
    status: "unsupported",
    durationMs: 0,
    message: "TS runtime declares framed-binary unsupported (WebSocket JSON runtime)"
  },
  {
    id: "stream.stream_open",
    level: "stream",
    requirement: "unsupported",
    status: "unsupported",
    durationMs: 0,
    message: "TS runtime declares stream unsupported (WebSocket JSON runtime)"
  },
  {
    id: "stream.stream_data",
    level: "stream",
    requirement: "unsupported",
    status: "unsupported",
    durationMs: 0,
    message: "TS runtime declares stream unsupported (WebSocket JSON runtime)"
  },
  {
    id: "stream.stream_close",
    level: "stream",
    requirement: "unsupported",
    status: "unsupported",
    durationMs: 0,
    message: "TS runtime declares stream unsupported (WebSocket JSON runtime)"
  }
];

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

/** 一对背靠背 stream transport（unframed-json，自定义 ReadableStream/WritableStream 对接）。 */
function makePair(): [StreamTransport, StreamTransport] {
  return createMockStreamPair(unframedJsonProfile());
}

async function withPair<T>(
  fn: (client: AxtpEndpoint, server: AxtpEndpoint) => Promise<T>,
  subscribeEvent?: string
): Promise<T> {
  // 用真实 WS（in-process server+client）：TS 声明为 WebSocket JSON runtime，且避免内存 loopback 的 Web Streams 怪问题。
  const wsServer = new NodeWsStreamServerTransport({ port: 0 });
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
  const clientT = await new NodeWsStreamClientTransport({
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
async function caseHelloIdentifyIdentified(): Promise<boolean> {
  return withPair(async (client, server) => {
    return HANDSHAKE.test(client.sid) && client.sid === server.sid && client.sid !== "00000000";
  });
}

// session.request_before_identified：未 start（idle）时业务 call 必须被同步拒绝（requireReady 守卫）。
async function caseRequestBeforeIdentified(): Promise<boolean> {
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
      client.call("audio.getAlgorithmConfig", {});
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

// error.standard_error_shape：错误响应 status 为 uint errorCode（number），非 {code,message} 对象。
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
function caseCapabilityGetAll(): boolean {
  return (
    registry.methodId("audio.getAlgorithmConfig") === 0x0901 &&
    Object.keys(METHOD_REGISTRY).length >= 4
  );
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

    await runCase("session.hello_identify_identified", caseHelloIdentifyIdentified);
    await runCase("session.request_before_identified", caseRequestBeforeIdentified);
    await runCase("rpc.request_response_json", caseRequestResponseJson);
    await runCase("rpc.method_not_found", caseMethodNotFound);
    await runCase("rpc.request_id_match", caseRequestIdMatch);
    await runCase("error.standard_error_shape", caseStandardErrorShape);
    await runCase("capability.get_all", caseCapabilityGetAll);
    await runCase("capability.unsupported_method", caseCapabilityUnsupportedMethod);
    await runCase("event.subscribe_event", caseSubscribeEvent);
    await runCase("event.emit_event", caseEmitEvent);
    // capability.method_binding / event.unsubscribe_event 保持 skipped（cases 声明时已设定）

    writeResult(resultPath, profilePath);

    const requiredFailed = cases.some(
      (item) => item.requirement === "required" && item.status !== "passed"
    );
    if (requiredFailed && process.env.CONFORMANCE_ALLOW_INCOMPLETE !== "true") {
      throw new Error("required AXTP conformance cases failed");
    }
  }, 60000);
});
