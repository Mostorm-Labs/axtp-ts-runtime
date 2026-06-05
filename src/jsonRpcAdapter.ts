import { JsonRpcEncoder } from "./codec.js";
import { type Bytes } from "./bytes.js";
import { ErrorCode, RpcBodyEncoding, RpcEncoding, RpcOp } from "./generated/axtp_ids_generated.js";
import { defaultPayloadMeta, rpcPayload, SourceProtocol, type RpcPayload } from "./model.js";
import type { ByteSink } from "./io.js";
import type { AxtpEndpoint } from "./endpoint.js";
import { AxtpWireMode, type ITransport, type TransportProfile } from "./transport.js";

export class WebSocketJsonRpcAdapter implements ByteSink {
  private readonly encoder = new JsonRpcEncoder();
  private helloSent = false;
  private identified = false;
  private nextSessionId = 1;
  private sid = this.makeSessionId();

  constructor(
    private readonly endpoint: AxtpEndpoint,
    private readonly writer: ITransport
  ) {
    this.endpoint.core().configure(WebSocketJsonRpcAdapter.jsonRpcProfile(writer.profile()));
  }

  static jsonRpcProfile(profile: TransportProfile): TransportProfile {
    return {
      ...profile,
      wireMode: AxtpWireMode.WebSocketJsonRpc,
      defaultRpcEncoding: RpcEncoding.Json,
      messageOriented: true,
      supportsTextMessage: true,
      supportsBinaryMessage: false
    };
  }

  async poll(transport?: { poll?: () => void | Promise<void>; hasConnection?: () => boolean }): Promise<void> {
    const hadConnection = transport?.hasConnection?.() ?? true;
    await transport?.poll?.();
    const hasConnection = transport?.hasConnection?.() ?? true;
    if (!hadConnection && hasConnection) {
      this.helloSent = false;
      this.identified = false;
      this.sid = this.makeSessionId();
    }
    if (hasConnection) {
      await this.sendHelloOnce();
    }
  }

  async sendHelloOnce(): Promise<void> {
    if (this.helloSent) return;
    await this.sendRpc(JsonRpcEncoder.makeHello());
    this.helloSent = true;
  }

  onBytes(bytes: Bytes): void {
    void this.handleBytes(bytes);
  }

  async sendRpc(payload: RpcPayload): Promise<void> {
    const bytes = this.encoder.encode(payload);
    await this.writer.sendBytes(bytes);
  }

  async sendEvent(payload: RpcPayload): Promise<void> {
    await this.sendRpc(rpcPayload({
      ...payload,
      encoding: RpcEncoding.Json,
      op: RpcOp.Event,
      meta: {
        ...payload.meta,
        sourceProtocol: SourceProtocol.JsonRpc
      }
    }));
  }

  private async handleBytes(bytes: Bytes): Promise<void> {
    try {
      const object = JSON.parse(new TextDecoder().decode(bytes)) as JsonObject;
      const op = parseOp(object);
      if (op === RpcOp.Identify || op === RpcOp.Reidentify) {
        await this.handleIdentify(object);
        return;
      }
      if (!this.identified && (op === RpcOp.Request || op === RpcOp.RequestBatch)) {
        await this.sendError(parseSid(object), parseRequestIdFromEnvelope(object), ErrorCode.ControlOpenRequired, op);
        return;
      }
      if (op === RpcOp.RequestBatch) {
        await this.sendError(parseSid(object), parseRequestIdFromEnvelope(object), ErrorCode.RpcBatchUnsupported, op);
        return;
      }

      this.endpoint.core().byteSink.onBytes(bytes);
      await this.endpoint.poll();
    } catch {
      await this.sendError("", 0, ErrorCode.RpcPayloadInvalid, RpcOp.Request);
    }
  }

  private async handleIdentify(object: JsonObject): Promise<void> {
    const d = asObject(object.d);
    if (typeof d.resumeSid === "string" && d.resumeSid.length > 0) {
      this.sid = d.resumeSid;
    }
    this.identified = true;
    await this.sendRpc(JsonRpcEncoder.makeIdentified(this.sid));
  }

  private async sendError(sid: string, requestId: number, code: ErrorCode, requestOp: RpcOp): Promise<void> {
    await this.sendRpc(rpcPayload({
      encoding: RpcEncoding.Json,
      op: requestOp === RpcOp.RequestBatch ? RpcOp.RequestBatchResponse : RpcOp.RequestResponse,
      requestId,
      statusCode: code,
      bodyEncoding: RpcBodyEncoding.RawBytes,
      meta: {
        ...defaultPayloadMeta(),
        sourceProtocol: SourceProtocol.JsonRpc,
        jsonSid: sid || this.sid
      }
    }));
  }

  private makeSessionId(): string {
    const id = String(this.nextSessionId);
    this.nextSessionId += 1;
    return id;
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

function parseRequestIdFromEnvelope(object: JsonObject): number {
  try {
    const d = asObject(object.d);
    if (typeof d.id === "number" && Number.isInteger(d.id) && d.id > 0 && d.id <= 0xffffffff) {
      return d.id;
    }
  } catch {
    return 0;
  }
  return 0;
}
