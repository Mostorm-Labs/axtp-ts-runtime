import { type Bytes, bytesToText, toBytes } from "./bytes.js";
import { ErrorCode, RpcBodyEncoding, RpcEncoding, RpcOp } from "./generated/axtp_ids_generated.js";
import { MethodRegistry } from "./generated/registry_generated.js";
import type { ControlPayload, RpcPayload, StreamPayload } from "./model.js";
import { rpcPayload } from "./model.js";

export enum BrokerTaskType {
  RpcRequest = "rpcRequest",
  RpcEvent = "rpcEvent",
  StreamData = "streamData",
  StreamClose = "streamClose",
  ControlNotice = "controlNotice",
  ProtocolError = "protocolError"
}

export enum BrokerResultType {
  RpcResponse = "rpcResponse",
  RpcError = "rpcError",
  Event = "event",
  StreamData = "streamData",
  StreamClose = "streamClose",
  Noop = "noop"
}

export interface RpcContext {
  sessionId: number;
  requestId: number;
  methodId: number;
  methodName: string;
  encoding: RpcEncoding;
  sourceProtocol: number;
}

export interface RpcRequestView {
  methodId: number;
  methodName: string;
  requestId: number;
  encoding: RpcEncoding;
  body: Bytes;
}

export interface RpcResponseData {
  encoding: RpcEncoding;
  body: Bytes;
  overrideEncoding: boolean;
}

export type RawRpcHandler = (context: RpcContext, request: RpcRequestView) => RpcResponseData | Promise<RpcResponseData>;
export type LegacyRawMethodHandler = (request: RpcPayload) => Bytes | Promise<Bytes>;
export type JsonRpcHandler = (context: RpcContext, paramsJson: string) => string | Promise<string>;
export type TlvRpcHandler = (context: RpcContext, body: Bytes) => Bytes | Promise<Bytes>;

export interface BrokerTask {
  type: BrokerTaskType;
  rpc: RpcPayload;
  stream?: StreamPayload;
  control?: ControlPayload;
  error?: ErrorCode;
}

export interface BrokerResult {
  type: BrokerResultType;
  rpc?: RpcPayload;
  stream?: StreamPayload;
}

export const BrokerResult = {
  rpcResponse(rpc: RpcPayload): BrokerResult {
    return { type: BrokerResultType.RpcResponse, rpc };
  },
  rpcError(rpc: RpcPayload): BrokerResult {
    return { type: BrokerResultType.RpcError, rpc };
  },
  event(rpc: RpcPayload): BrokerResult {
    return { type: BrokerResultType.Event, rpc };
  },
  streamData(stream: StreamPayload): BrokerResult {
    return { type: BrokerResultType.StreamData, stream };
  },
  streamClose(stream: StreamPayload): BrokerResult {
    return { type: BrokerResultType.StreamClose, stream };
  },
  noop(): BrokerResult {
    return { type: BrokerResultType.Noop };
  }
};

export class BusinessRouter {
  private readonly methodHandlers = new Map<number, RawRpcHandler>();
  private readonly methodRegistry = MethodRegistry.fromGeneratedDefaults();

  registry(): MethodRegistry {
    return this.methodRegistry;
  }

  registerMethod(methodId: number, handler: LegacyRawMethodHandler): void {
    this.registerRawMethod(methodId, async (_context, request) => {
      const response = await handler(rpcPayload({
        encoding: request.encoding,
        op: RpcOp.Request,
        requestId: request.requestId,
        methodOrEventId: request.methodId,
        body: request.body
      }));
      return { encoding: request.encoding, body: response, overrideEncoding: false };
    });
  }

  registerRawMethod(methodId: number, handler: RawRpcHandler): void {
    this.methodHandlers.set(methodId, handler);
  }

  registerJsonMethod(methodId: number, handler: JsonRpcHandler): void;
  registerJsonMethod(methodName: string, handler: JsonRpcHandler): void;
  registerJsonMethod(method: number | string, handler: JsonRpcHandler): void {
    const methodId = typeof method === "number" ? method : this.methodRegistry.findMethodId(method);
    if (methodId === undefined) return;
    this.registerRawMethod(methodId, async (context, request) => ({
      encoding: RpcEncoding.Json,
      body: toBytes(await handler(context, bytesToText(request.body))),
      overrideEncoding: true
    }));
  }

  registerTlvMethod(methodId: number, handler: TlvRpcHandler): void;
  registerTlvMethod(methodName: string, handler: TlvRpcHandler): void;
  registerTlvMethod(method: number | string, handler: TlvRpcHandler): void {
    const methodId = typeof method === "number" ? method : this.methodRegistry.findMethodId(method);
    if (methodId === undefined) return;
    this.registerRawMethod(methodId, async (context, request) => ({
      encoding: RpcEncoding.Tlv,
      body: await handler(context, request.body),
      overrideEncoding: true
    }));
  }

  async handleRpcRequest(request: RpcPayload): Promise<RpcPayload> {
    const response = rpcPayload({
      encoding: request.encoding,
      op: RpcOp.RequestResponse,
      requestId: request.requestId,
      methodOrEventId: request.methodOrEventId,
      statusCode: ErrorCode.Success,
      bodyEncoding: request.bodyEncoding,
      meta: request.meta
    });

    const handler = this.methodHandlers.get(request.methodOrEventId);
    if (handler === undefined) {
      response.statusCode = ErrorCode.RpcMethodNotFound;
      return response;
    }

    const methodName = this.methodRegistry.findMethodName(request.methodOrEventId) ?? "";
    const context: RpcContext = {
      sessionId: request.meta.sessionId,
      requestId: request.requestId,
      methodId: request.methodOrEventId,
      methodName,
      encoding: request.encoding,
      sourceProtocol: request.meta.sourceProtocol
    };
    const view: RpcRequestView = {
      methodId: request.methodOrEventId,
      methodName,
      requestId: request.requestId,
      encoding: request.encoding,
      body: request.body
    };

    try {
      const data = await handler(context, view);
      if (data.overrideEncoding) {
        response.encoding = data.encoding;
        response.bodyEncoding =
          data.encoding === RpcEncoding.Tlv || data.encoding === RpcEncoding.Binary
            ? RpcBodyEncoding.Tlv8
            : RpcBodyEncoding.RawBytes;
      }
      response.body = data.body;
    } catch {
      response.statusCode = ErrorCode.RpcExecutionFailed;
    }
    return response;
  }
}

export class BasicBroker {
  private readonly tasks: BrokerTask[] = [];
  private readonly results: BrokerResult[] = [];
  private readonly router = new BusinessRouter();

  registry(): MethodRegistry {
    return this.router.registry();
  }

  submit(task: BrokerTask): void {
    this.tasks.push(task);
  }

  async poll(maxTasks = 8): Promise<void> {
    let processed = 0;
    while (this.tasks.length > 0 && processed < maxTasks) {
      const task = this.tasks.shift()!;
      processed += 1;

      if (task.type === BrokerTaskType.RpcRequest) {
        const response = await this.router.handleRpcRequest(task.rpc);
        this.results.push(
          response.statusCode === ErrorCode.Success
            ? BrokerResult.rpcResponse(response)
            : BrokerResult.rpcError(response)
        );
        continue;
      }
      if (task.type === BrokerTaskType.RpcEvent) {
        this.results.push(BrokerResult.event(task.rpc));
        continue;
      }
      if (task.type === BrokerTaskType.StreamData && task.stream !== undefined) {
        this.results.push(BrokerResult.streamData(task.stream));
        continue;
      }
      if (task.type === BrokerTaskType.StreamClose && task.stream !== undefined) {
        this.results.push(BrokerResult.streamClose(task.stream));
      }
    }
  }

  pollResult(): BrokerResult | undefined {
    return this.results.shift();
  }

  registerMethod(methodId: number, handler: LegacyRawMethodHandler): void {
    this.router.registerMethod(methodId, handler);
  }

  registerRawMethod(methodId: number, handler: RawRpcHandler): void {
    this.router.registerRawMethod(methodId, handler);
  }

  registerJsonMethod(methodId: number, handler: JsonRpcHandler): void;
  registerJsonMethod(methodName: string, handler: JsonRpcHandler): void;
  registerJsonMethod(method: number | string, handler: JsonRpcHandler): void {
    this.router.registerJsonMethod(method as never, handler);
  }

  registerTlvMethod(methodId: number, handler: TlvRpcHandler): void;
  registerTlvMethod(methodName: string, handler: TlvRpcHandler): void;
  registerTlvMethod(method: number | string, handler: TlvRpcHandler): void {
    this.router.registerTlvMethod(method as never, handler);
  }

  queuedTaskCount(): number {
    return this.tasks.length;
  }

  queuedResultCount(): number {
    return this.results.length;
  }
}
