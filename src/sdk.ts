import { BasicBroker, BrokerResult, type LegacyRawMethodHandler, type RawRpcHandler, type JsonRpcHandler, type TlvRpcHandler } from "./broker.js";
import { type Bytes, bytesToText, toBytes } from "./bytes.js";
import { AxtpEndpoint } from "./endpoint.js";
import { ErrorCode, RpcBodyEncoding, RpcEncoding, RpcOp } from "./generated/axtp_ids_generated.js";
import { MethodRegistry } from "./generated/registry_generated.js";
import { SourceProtocol, rpcPayload, type RpcPayload } from "./model.js";
import { AxtpWireMode, type ITransport } from "./transport.js";

export interface ClientOptions {
  autoOpen?: boolean;
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

const defaultClientOptions: Required<ClientOptions> = {
  autoOpen: true,
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
  private lastErrorValue: SdkError = { ok: true, code: ErrorCode.Success, message: "" };

  constructor(options: ClientOptions = {}) {
    this.options = { ...defaultClientOptions, ...options };
  }

  async attachTransport(transport: ITransport): Promise<void> {
    await this.close();
    this.transport = transport;
    this.endpointValue = new AxtpEndpoint(this.brokerValue);
    this.endpointValue.attachTransport(transport);
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

  registerMethod(methodId: number, handler: LegacyRawMethodHandler): void {
    this.localHandlers.set(methodId, handler);
  }

  registerEventHandler(eventId: number, handler: (payload: RpcPayload) => void): void {
    this.eventHandlers.set(eventId, handler);
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

  async callJson(methodName: string, paramsJson: string, options?: CallOptions): Promise<string>;
  async callJson(methodId: number, paramsJson: string, options?: CallOptions): Promise<string>;
  async callJson(method: number | string, paramsJson: string, options: CallOptions = {}): Promise<string> {
    const methodId = typeof method === "number" ? method : this.registryValue.findMethodId(method);
    if (methodId === undefined) {
      this.lastErrorValue = { ok: false, code: ErrorCode.RpcMethodNotFound, message: "method not found" };
      return "";
    }
    const bytes = await this.callRaw(methodId, RpcEncoding.Json, toBytes(paramsJson), { ...options, encoding: RpcEncoding.Json });
    return bytesToText(bytes);
  }

  async callTlv(methodName: string, tlvBody: Bytes, options?: CallOptions): Promise<Bytes>;
  async callTlv(methodId: number, tlvBody: Bytes, options?: CallOptions): Promise<Bytes>;
  async callTlv(method: number | string, tlvBody: Bytes, options: CallOptions = {}): Promise<Bytes> {
    const methodId = typeof method === "number" ? method : this.registryValue.findMethodId(method);
    if (methodId === undefined) {
      this.lastErrorValue = { ok: false, code: ErrorCode.RpcMethodNotFound, message: "method not found" };
      return new Uint8Array();
    }
    return this.callRaw(methodId, RpcEncoding.Tlv, tlvBody, { ...options, encoding: RpcEncoding.Tlv });
  }

  async callRawBytes(methodId: number, body: Bytes, options: CallOptions = {}): Promise<Bytes> {
    return this.callRaw(methodId, RpcEncoding.Raw, body, { ...options, encoding: RpcEncoding.Raw });
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
        sourceProtocol: this.options.wireMode === AxtpWireMode.WebSocketJsonRpc ? SourceProtocol.JsonRpc : SourceProtocol.AxtpV1,
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
      request.encoding !== RpcEncoding.Tlv &&
      request.encoding !== RpcEncoding.Binary
    ) {
      request.bodyEncoding = bodyEncodingFor(request.encoding);
    }
    request.op = RpcOp.Request;
    request.meta.requestId = request.requestId;
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

  onJson(method: number | string, handler: JsonRpcHandler): void {
    this.brokerValue.registerJsonMethod(method as never, handler);
  }

  onTlv(method: number | string, handler: TlvRpcHandler): void {
    this.brokerValue.registerTlvMethod(method as never, handler);
  }

  async emitRaw(payload: RpcPayload): Promise<void> {
    this.endpointValue.core().handleBrokerResult(BrokerResult.event(payload));
    await this.endpointValue.flushOutbound();
  }

  endpoint(): AxtpEndpoint {
    return this.endpointValue;
  }
}

function bodyEncodingFor(encoding: RpcEncoding): RpcBodyEncoding {
  return encoding === RpcEncoding.Tlv || encoding === RpcEncoding.Binary
    ? RpcBodyEncoding.Tlv8
    : RpcBodyEncoding.RawBytes;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
