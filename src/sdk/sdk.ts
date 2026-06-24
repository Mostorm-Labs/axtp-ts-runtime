import { ControlOpcode, ErrorCode, RpcBodyEncoding, RpcEncoding, RpcOp } from "../core/protocol/generated/axtp_ids_generated.js";
import type { AxtpEventName, AxtpEventPayload } from "../core/protocol/generated/event_map_generated.js";
import type { AxtpMethodName, AxtpRequest, AxtpResponse } from "../core/protocol/generated/method_map_generated.js";
import { MethodRegistry, RegistryLookup } from "../core/protocol/generated/registry_generated.js";
import { SourceProtocol, defaultPayloadMeta, rpcPayload, type RpcPayload } from "../core/protocol/model/model.js";
import { bodyEncodingForRpcEncoding, isJsonBinaryRpcEncoding, rpcEncodingJsonBinary } from "../core/protocol/rpcEncoding.js";
import { JsonRpcEncoder } from "../core/protocol/wire/codec.js";
import { BasicBroker, BrokerResult, type JsonRpcHandler, type LegacyRawMethodHandler, type RawRpcHandler, type RpcContext, type TlvRpcHandler } from "../core/runtime/broker/broker.js";
import { AxtpEndpoint } from "../core/runtime/endpoint/endpoint.js";
import { AxtpWireMode, type ITransport } from "../core/runtime/transport/transport.js";
import { bytesToText, toBytes, type Bytes } from "../core/support/io/bytes.js";

export interface ClientOptions {
  autoOpen?: boolean;
  autoIdentify?: boolean;
  wireMode?: AxtpWireMode;
  timeoutMs?: number;
  pollIntervalMs?: number;
}

export interface CallOptions {
  encoding?: RpcEncoding;
  timeoutMs?: number;
}

export interface SdkError {
  ok: boolean;
  code: ErrorCode;
  message: string;
}

export interface AppReadyTraceEvent {
  stage: string;
  action: string;
  statusCode: ErrorCode;
  sid: string;
  bodyText: string;
  detail: string;
  randomSeed?: number;
}

export interface AppReadyOptions {
  timeoutMs?: number;
  eventMasks?: string;
  randomSeed?: number;
  trace?: (event: AppReadyTraceEvent) => void;
}

export interface AppReadyResult {
  ok: boolean;
  statusCode: ErrorCode;
  stage: string;
  sid: string;
  randomSeed?: number;
}

const defaultClientOptions: Required<ClientOptions> = {
  autoOpen: true,
  autoIdentify: true,
  wireMode: AxtpWireMode.FramedBinary,
  timeoutMs: 1000,
  pollIntervalMs: 1
};

export class AxtpClient {
  private readonly options: Required<ClientOptions>;
  private readonly brokerValue = new BasicBroker();
  private endpointValue = new AxtpEndpoint(this.brokerValue);
  private readonly registryValue = MethodRegistry.fromGeneratedDefaults();
  private readonly localHandlers = new Map<number, LegacyRawMethodHandler>();
  private readonly eventHandlers = new Map<number, (payload: RpcPayload) => void>();
  private transport: ITransport | undefined;
  private connected = false;
  private nextRequestId = 1;
  private nextControlId = 1;
  private lastErrorValue: SdkError = { ok: true, code: ErrorCode.Success, message: "" };
  private appReady = false;
  private sessionSidValue = "";
  private lastAppReadyValue: AppReadyResult = {
    ok: false,
    statusCode: ErrorCode.Success,
    stage: "",
    sid: ""
  };

  constructor(options: ClientOptions = {}) {
    this.options = { ...defaultClientOptions, ...options };
  }

  async attachTransport(transport: ITransport): Promise<void> {
    await this.close();
    this.transport = transport;
    this.endpointValue = new AxtpEndpoint(this.brokerValue);
    this.endpointValue.attachTransport(transport);
    this.appReady = false;
    this.sessionSidValue = "";
    if (this.options.autoOpen) {
      await transport.open();
    }
    this.connected = true;
  }

  async close(): Promise<void> {
    if (this.transport !== undefined) {
      await this.transport.close();
    }
    this.connected = false;
    this.appReady = false;
    this.sessionSidValue = "";
  }

  isConnected(): boolean {
    return this.connected;
  }

  lastError(): SdkError {
    return this.lastErrorValue;
  }

  registry(): MethodRegistry {
    return this.registryValue;
  }

  async poll(): Promise<void> {
    await this.endpointValue.poll();
  }

  async ensureAppReady(options: AppReadyOptions = {}): Promise<AppReadyResult> {
    const emitTrace = (
      stage: string,
      action: string,
      statusCode = ErrorCode.Success,
      sid = "",
      bodyText = "",
      detail = ""
    ): void => {
      options.trace?.({ stage, action, statusCode, sid, bodyText, detail, randomSeed: options.randomSeed });
    };

    emitTrace("start", "begin", ErrorCode.Success, "", "", "ensureAppReady entered");
    if (this.appReady && this.sessionSidValue.length > 0) {
      const result = {
        ok: true,
        statusCode: ErrorCode.Success,
        stage: "app-ready",
        sid: this.sessionSidValue,
        randomSeed: this.lastAppReadyValue.randomSeed
      };
      this.lastAppReadyValue = result;
      this.lastErrorValue = { ok: true, code: ErrorCode.Success, message: "" };
      emitTrace("app-ready", "already-ready", ErrorCode.Success, result.sid);
      return result;
    }

    if (this.transport === undefined) {
      const result = this.appReadyError(ErrorCode.Unavailable, "transport", "transport unavailable");
      emitTrace("transport", "error", result.statusCode, "", "", "transport unavailable");
      return result;
    }

    const profile = this.transport.profile();
    this.endpointValue.core().configure(profile);
    const deadline = Date.now() + (options.timeoutMs ?? this.options.timeoutMs);
    if (profile.wireMode === AxtpWireMode.FramedBinary) {
      const controlId = this.nextControlId;
      this.nextControlId += 1;
      emitTrace("control-open", "send", ErrorCode.Success, "", "", `controlId=${controlId}`);
      await this.endpointValue.sendControlOpen(controlId);
      emitTrace("control-accept", "wait", ErrorCode.Success, "", "", `controlId=${controlId}`);
      while (Date.now() < deadline) {
        await this.poll();
        const accept = this.endpointValue.tryTakeControlNotice(ControlOpcode.Accept);
        if (accept !== undefined) {
          if (
            accept.controlId === controlId &&
            accept.statusCode === ErrorCode.Success &&
            this.endpointValue.core().controlSessionOpen()
          ) {
            emitTrace("control-accept", "receive", ErrorCode.Success, "", "", "controlId matched");
            break;
          }
          const code = accept.statusCode === ErrorCode.Success ? ErrorCode.ControlOpenRejected : accept.statusCode;
          const result = this.appReadyError(code, "control-open", "control open rejected");
          emitTrace("control-open", "error", result.statusCode);
          return result;
        }
        await sleep(this.options.pollIntervalMs);
      }
      if (!this.endpointValue.core().controlSessionOpen()) {
        const result = this.appReadyError(ErrorCode.RpcResponseTimeout, "control-accept", "control accept timeout");
        emitTrace("control-accept", "timeout", result.statusCode);
        return result;
      }
    }

    emitTrace("hello", "wait");
    let gotHello = false;
    while (Date.now() < deadline) {
      await this.poll();
      const hello = this.endpointValue.tryTakeSessionRpc(RpcOp.Hello);
      if (hello !== undefined) {
        gotHello = true;
        emitTrace("hello", "receive", ErrorCode.Success, hello.meta.jsonSid, bytesToText(hello.body));
        break;
      }
      await sleep(this.options.pollIntervalMs);
    }
    if (!gotHello) {
      const result = this.appReadyError(ErrorCode.RpcResponseTimeout, "hello", "hello timeout");
      emitTrace("hello", "timeout", result.statusCode);
      return result;
    }

    const randomSeed = options.randomSeed ?? Math.floor(Math.random() * 0xffffffff);
    emitTrace("identify", "send", ErrorCode.Success, "", "", `randomSeed=${randomSeed}`);
    await this.endpointValue.sendRpcSession(JsonRpcEncoder.makeIdentify(randomSeed, options.eventMasks ?? ""));

    emitTrace("identified", "wait");
    while (Date.now() < deadline) {
      await this.poll();
      const identified = this.endpointValue.tryTakeSessionRpc(RpcOp.Identified);
      if (identified !== undefined) {
        const sid = identified.meta.jsonSid;
        const bodyText = bytesToText(identified.body);
        if (sid.length === 0) {
          const result = this.appReadyError(ErrorCode.RpcPayloadInvalid, "identified", "identified sid missing");
          emitTrace("identified", "error", result.statusCode, "", bodyText);
          return result;
        }
        const result = {
          ok: true,
          statusCode: ErrorCode.Success,
          stage: "app-ready",
          sid,
          randomSeed
        };
        this.appReady = true;
        this.sessionSidValue = sid;
        this.lastAppReadyValue = result;
        this.lastErrorValue = { ok: true, code: ErrorCode.Success, message: "" };
        emitTrace("app-ready", "ready", ErrorCode.Success, sid, bodyText);
        return result;
      }
      await sleep(this.options.pollIntervalMs);
    }

    const result = this.appReadyError(ErrorCode.RpcResponseTimeout, "identified", "identified timeout");
    emitTrace("identified", "timeout", result.statusCode);
    return result;
  }

  isAppReady(): boolean {
    return this.appReady;
  }

  sessionSid(): string {
    return this.sessionSidValue;
  }

  lastAppReadyResult(): AppReadyResult {
    return this.lastAppReadyValue;
  }

  registerMethod(methodId: number, handler: LegacyRawMethodHandler): void {
    this.localHandlers.set(methodId, handler);
  }

  registerEventHandler(eventId: number, handler: (payload: RpcPayload) => void): void {
    this.eventHandlers.set(eventId, handler);
  }

  /**
   * Typed event subscription: `handler` receives the JSON-decoded event
   * payload. The raw `registerEventHandler` remains available for non-JSON
   * or raw-byte event handling.
   */
  onEvent<K extends AxtpEventName>(event: K, handler: (payload: AxtpEventPayload<K>) => void): void {
    const eventId = RegistryLookup.eventIdByName(event);
    if (eventId === undefined) return;
    this.registerEventHandler(eventId, (payload) => {
      const text = bytesToText(payload.body);
      handler((text.length === 0 ? {} : JSON.parse(text)) as AxtpEventPayload<K>);
    });
  }

  async callRaw(request: RpcPayload, options?: CallOptions): Promise<RpcPayload>;
  async callRaw(methodId: number, encoding: RpcEncoding, body: Bytes, options?: CallOptions): Promise<Bytes>;
  async callRaw(
    requestOrMethodId: RpcPayload | number,
    encodingOrOptions: RpcEncoding | CallOptions = {},
    body?: Bytes,
    options: CallOptions = {}
  ): Promise<RpcPayload | Bytes> {
    if (typeof requestOrMethodId === "number") {
      const encoding = encodingOrOptions as RpcEncoding;
      const payload = this.makeDynamicRequest(requestOrMethodId, encoding, body ?? new Uint8Array());
      const response = await this.callRawPayload(payload, options);
      this.lastErrorValue = response.statusCode === ErrorCode.Success
        ? { ok: true, code: ErrorCode.Success, message: "" }
        : { ok: false, code: response.statusCode, message: "" };
      return response.body;
    }
    return this.callRawPayload(requestOrMethodId, encodingOrOptions as CallOptions);
  }

  async callJson<K extends AxtpMethodName>(method: K, params: AxtpRequest<K>, options?: CallOptions): Promise<AxtpResponse<K>>;
  async callJson(methodId: number, params: unknown, options?: CallOptions): Promise<unknown>;
  async callJson(method: number | string, params: unknown, options: CallOptions = {}): Promise<unknown> {
    const methodId = typeof method === "number" ? method : this.registryValue.findMethodId(method);
    if (methodId === undefined) {
      this.lastErrorValue = { ok: false, code: ErrorCode.RpcMethodNotFound, message: "method not found" };
      throw new Error(`AXTP method not found: ${String(method)}`);
    }
    const body = params === undefined ? "" : JSON.stringify(params);
    const bytes = await this.callRaw(methodId, RpcEncoding.Json, toBytes(body), { ...options, encoding: RpcEncoding.Json });
    if (this.lastErrorValue.code !== ErrorCode.Success) {
      throw new Error(`AXTP call '${String(method)}' failed: code ${this.lastErrorValue.code}`);
    }
    const text = bytesToText(bytes);
    return text.length === 0 ? {} : JSON.parse(text);
  }

  async callTlv(methodName: string, tlvBody: Bytes, options?: CallOptions): Promise<Bytes>;
  async callTlv(methodId: number, tlvBody: Bytes, options?: CallOptions): Promise<Bytes>;
  async callTlv(method: number | string, tlvBody: Bytes, options: CallOptions = {}): Promise<Bytes> {
    const methodId = typeof method === "number" ? method : this.registryValue.findMethodId(method);
    if (methodId === undefined) {
      this.lastErrorValue = { ok: false, code: ErrorCode.RpcMethodNotFound, message: "method not found" };
      return new Uint8Array();
    }
    return this.callRaw(methodId, rpcEncodingJsonBinary, tlvBody, { ...options, encoding: rpcEncodingJsonBinary });
  }

  async callRawBytes(methodId: number, body: Bytes, options: CallOptions = {}): Promise<Bytes> {
    return this.callRaw(methodId, rpcEncodingJsonBinary, body, { ...options, encoding: rpcEncodingJsonBinary });
  }

  emitRaw(eventPayload: RpcPayload): void {
    this.eventHandlers.get(eventPayload.methodOrEventId)?.(eventPayload);
  }

  private async callRawPayload(request: RpcPayload, options: CallOptions = {}): Promise<RpcPayload> {
    this.normalizeRequest(request);
    const local = this.localHandlers.get(request.methodOrEventId);
    if (local !== undefined) {
      const response = rpcPayload({
        encoding: request.encoding,
        op: RpcOp.RequestResponse,
        requestId: request.requestId,
        methodOrEventId: request.methodOrEventId,
        statusCode: ErrorCode.Success,
        bodyEncoding: request.bodyEncoding,
        meta: request.meta,
        body: await local(request)
      });
      this.lastErrorValue = { ok: true, code: ErrorCode.Success, message: "" };
      return response;
    }

    if (this.transport === undefined) {
      return this.makeErrorResponse(request, ErrorCode.Unavailable);
    }

    this.endpointValue.core().configure(this.transport.profile());
    if (this.options.autoIdentify && !this.appReady) {
      const ready = await this.ensureAppReady({ timeoutMs: options.timeoutMs ?? this.options.timeoutMs });
      if (!ready.ok) return this.makeErrorResponse(request, ready.statusCode);
    }
    this.applySessionSid(request);
    await this.endpointValue.sendRpcRequest(request);
    const deadline = Date.now() + (options.timeoutMs ?? this.options.timeoutMs);
    while (Date.now() < deadline) {
      await this.poll();
      const response = this.endpointValue.tryTakeRpcResponse(request.requestId);
      if (response !== undefined) {
        this.lastErrorValue = response.statusCode === ErrorCode.Success
          ? { ok: true, code: ErrorCode.Success, message: "" }
          : { ok: false, code: response.statusCode, message: "" };
        return response;
      }
      await sleep(this.options.pollIntervalMs);
    }
    return this.makeErrorResponse(request, ErrorCode.RpcResponseTimeout);
  }

  private makeDynamicRequest(methodId: number, encoding: RpcEncoding, body: Bytes): RpcPayload {
    const methodName = this.registryValue.findMethodName(methodId) ?? "";
    return rpcPayload({
      encoding,
      op: RpcOp.Request,
      methodOrEventId: methodId,
      bodyEncoding: bodyEncodingFor(encoding),
      body,
      meta: {
        sourceProtocol: encoding === RpcEncoding.Json ? SourceProtocol.JsonRpc : SourceProtocol.AxtpV1,
        sessionId: 0,
        requestId: 0,
        jsonSid: "",
        jsonMethodOrEventName: methodName
      }
    });
  }

  private normalizeRequest(request: RpcPayload): void {
    if (request.requestId === 0) {
      request.requestId = this.nextRequestId;
      this.nextRequestId += 1;
    }
    if (
      request.bodyEncoding === RpcBodyEncoding.Tlv8 &&
      !isJsonBinaryRpcEncoding(request.encoding)
    ) {
      request.bodyEncoding = bodyEncodingFor(request.encoding);
    }
    request.op = RpcOp.Request;
    request.meta.requestId = request.requestId;
    this.applySessionSid(request);
  }

  private applySessionSid(request: RpcPayload): void {
    if (
      request.meta.sourceProtocol === SourceProtocol.JsonRpc &&
      request.meta.jsonSid.length === 0 &&
      this.appReady &&
      this.sessionSidValue.length > 0
    ) {
      request.meta.jsonSid = this.sessionSidValue;
    }
  }

  private appReadyError(code: ErrorCode, stage: string, message: string): AppReadyResult {
    const result = { ok: false, statusCode: code, stage, sid: "" };
    this.lastAppReadyValue = result;
    this.lastErrorValue = { ok: false, code, message };
    return result;
  }

  private makeErrorResponse(request: RpcPayload, code: ErrorCode): RpcPayload {
    this.lastErrorValue = { ok: false, code, message: "" };
    return rpcPayload({
      encoding: request.encoding,
      op: RpcOp.RequestResponse,
      requestId: request.requestId,
      methodOrEventId: request.methodOrEventId,
      statusCode: code,
      bodyEncoding: request.bodyEncoding,
      meta: request.meta
    });
  }
}

export class AxtpServer {
  private readonly brokerValue = new BasicBroker();
  private readonly endpointValue = new AxtpEndpoint(this.brokerValue);
  private transport: ITransport | undefined;

  async attachTransport(transport: ITransport): Promise<void> {
    this.transport = transport;
    this.endpointValue.attachTransport(transport);
    await transport.open();
  }

  async close(): Promise<void> {
    await this.transport?.close();
  }

  async poll(maxTasks = 8): Promise<void> {
    await this.endpointValue.poll(maxTasks);
  }

  onRaw(methodId: number, handler: RawRpcHandler): void {
    this.brokerValue.registerRawMethod(methodId, handler);
  }

  onJson<K extends AxtpMethodName>(method: K, handler: (context: RpcContext, params: AxtpRequest<K>) => AxtpResponse<K> | Promise<AxtpResponse<K>>): void;
  onJson(methodId: number, handler: JsonRpcHandler): void;
  onJson(method: number | string, handler: ((context: RpcContext, params: unknown) => unknown) | JsonRpcHandler): void {
    if (typeof method === "number") {
      this.brokerValue.registerJsonMethod(method, handler as JsonRpcHandler);
      return;
    }
    const typedHandler = handler as (context: RpcContext, params: unknown) => unknown;
    const wrapped: JsonRpcHandler = async (context, paramsJson) => {
      const params = paramsJson.length > 0 ? JSON.parse(paramsJson) : {};
      const result = await typedHandler(context, params);
      return result === undefined ? "" : JSON.stringify(result);
    };
    this.brokerValue.registerJsonMethod(method as never, wrapped);
  }

  onTlv(method: number | string, handler: TlvRpcHandler): void {
    this.brokerValue.registerTlvMethod(method as never, handler);
  }

  async emitRaw(payload: RpcPayload): Promise<void> {
    this.endpointValue.core().handleBrokerResult(BrokerResult.event(payload));
    await this.endpointValue.flushOutbound();
  }

  /**
   * Typed event emission: `payload` is JSON-encoded into the event body.
   * The raw `emitRaw` remains available for non-JSON or raw-byte events.
   */
  async emit<K extends AxtpEventName>(event: K, payload: AxtpEventPayload<K>): Promise<void> {
    const eventId = RegistryLookup.eventIdByName(event);
    if (eventId === undefined) return;
    const rpc = rpcPayload({
      encoding: RpcEncoding.Json,
      op: RpcOp.Event,
      methodOrEventId: eventId,
      body: toBytes(JSON.stringify(payload)),
      meta: { ...defaultPayloadMeta(), sourceProtocol: SourceProtocol.JsonRpc, jsonMethodOrEventName: event }
    });
    await this.emitRaw(rpc);
  }

  endpoint(): AxtpEndpoint {
    return this.endpointValue;
  }
}

function bodyEncodingFor(encoding: RpcEncoding): RpcBodyEncoding {
  return bodyEncodingForRpcEncoding(encoding);
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
