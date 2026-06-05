import { BrokerResultType, type BrokerResult } from "./broker.js";
import { type Bytes } from "./bytes.js";
import { InboundProcessor, OutboundProcessor, type PayloadSink } from "./codec.js";
import { ControlOpcode, ErrorCode, RpcOp } from "./generated/axtp_ids_generated.js";
import {
  SourceProtocol,
  controlPayload,
  type ControlPayload,
  type RpcPayload,
  type StreamPayload
} from "./model.js";
import { defaultTransportProfile, type TransportProfile } from "./transport.js";

export enum CoreEventType {
  RpcRequest = "rpcRequest",
  RpcEvent = "rpcEvent",
  StreamOpen = "streamOpen",
  StreamData = "streamData",
  StreamClose = "streamClose",
  ControlNotice = "controlNotice",
  ProtocolError = "protocolError"
}

export interface CoreEvent {
  type: CoreEventType;
  rpc?: RpcPayload;
  stream?: StreamPayload;
  control?: ControlPayload;
  error?: ErrorCode;
}

export const CoreEvent = {
  rpcRequest(rpc: RpcPayload): CoreEvent {
    return { type: CoreEventType.RpcRequest, rpc };
  },
  rpcEvent(rpc: RpcPayload): CoreEvent {
    return { type: CoreEventType.RpcEvent, rpc };
  },
  streamData(stream: StreamPayload): CoreEvent {
    return { type: CoreEventType.StreamData, stream };
  },
  controlNotice(control: ControlPayload): CoreEvent {
    return { type: CoreEventType.ControlNotice, control };
  },
  protocolError(error: ErrorCode): CoreEvent {
    return { type: CoreEventType.ProtocolError, error };
  }
};

class PendingCallTable {
  private readonly pending = new Set<number>();
  private readonly resolved = new Map<number, RpcPayload>();

  expect(requestId: number): void {
    this.pending.add(requestId);
  }

  resolve(requestId: number, payload: RpcPayload): void {
    this.resolved.set(requestId, payload);
    this.pending.delete(requestId);
  }

  isPending(requestId: number): boolean {
    return this.pending.has(requestId);
  }

  tryTakeResolved(requestId: number): RpcPayload | undefined {
    const payload = this.resolved.get(requestId);
    if (payload === undefined) return undefined;
    this.resolved.delete(requestId);
    return payload;
  }
}

class ControlSession {
  private open = false;
  private lastOpcodeValue = ControlOpcode.Open;

  handle(payload: ControlPayload): ControlPayload | undefined {
    this.lastOpcodeValue = payload.opcode;
    if (payload.opcode === ControlOpcode.Open) {
      this.open = true;
      return this.makeResponse(ControlOpcode.Accept, payload);
    }
    if (payload.opcode === ControlOpcode.Ping) {
      return this.makeResponse(ControlOpcode.Pong, payload);
    }
    if (payload.opcode === ControlOpcode.Close) {
      this.open = false;
      return this.makeResponse(ControlOpcode.CloseAck, payload);
    }
    return undefined;
  }

  isOpen(): boolean {
    return this.open;
  }

  lastOpcode(): ControlOpcode {
    return this.lastOpcodeValue;
  }

  private makeResponse(opcode: ControlOpcode, request: ControlPayload): ControlPayload {
    return controlPayload({
      opcode,
      controlId: request.controlId,
      statusCode: ErrorCode.Success,
      meta: request.meta
    });
  }
}

export class AxtpCore {
  private readonly events: CoreEvent[] = [];
  private readonly outboundBytes: Bytes[] = [];
  private readonly controlSessionValue = new ControlSession();
  private readonly pendingCalls = new PendingCallTable();
  private transportProfile = defaultTransportProfile();
  private readonly payloadSink: PayloadSink = {
    onControl: (payload) => this.handleControl(payload),
    onRpc: (payload) => this.handleRpc(payload),
    onStream: (payload) => this.handleStream(payload)
  };
  private readonly inbound = new InboundProcessor(this.payloadSink);
  private readonly outbound = new OutboundProcessor({
    writeBytes: (bytes) => this.outboundBytes.push(bytes.slice())
  });
  readonly byteSink = {
    onBytes: (bytes: Bytes): void => this.inbound.onBytes(bytes)
  };

  configure(profile: TransportProfile): void {
    this.transportProfile = { ...profile };
    this.inbound.setWireMode(profile.wireMode);
    this.outbound.setWireMode(profile.wireMode);
    if (profile.preferredFrameSize > 0) {
      this.outbound.setMaxFrameSize(profile.preferredFrameSize);
    }
  }

  profile(): TransportProfile {
    return { ...this.transportProfile };
  }

  pollEvent(): CoreEvent | undefined {
    return this.events.shift();
  }

  handleBrokerResult(result: BrokerResult): void {
    switch (result.type) {
      case BrokerResultType.RpcResponse:
        if (result.rpc !== undefined) this.outbound.sendRpcResponse(result.rpc);
        break;
      case BrokerResultType.RpcError:
        if (result.rpc !== undefined) this.outbound.sendRpcError(result.rpc);
        break;
      case BrokerResultType.Event:
        if (result.rpc !== undefined) this.outbound.sendEvent(result.rpc);
        break;
      case BrokerResultType.StreamData:
      case BrokerResultType.StreamClose:
        if (result.stream !== undefined) this.outbound.sendStream(result.stream);
        break;
      case BrokerResultType.Noop:
        break;
    }
  }

  expectRpcResponse(requestId: number): void {
    this.pendingCalls.expect(requestId);
  }

  tryTakeRpcResponse(requestId: number): RpcPayload | undefined {
    return this.pendingCalls.tryTakeResolved(requestId);
  }

  tryPopOutboundBytes(): Bytes | undefined {
    return this.outboundBytes.shift();
  }

  controlSessionOpen(): boolean {
    return this.controlSessionValue.isOpen();
  }

  sendRpcRequest(payload: RpcPayload): void {
    this.outbound.sendRpcRequest(payload);
  }

  private handleControl(payload: ControlPayload): void {
    const response = this.controlSessionValue.handle(payload);
    if (response !== undefined) {
      this.outbound.sendControl(response);
    }
  }

  private handleRpc(payload: RpcPayload): void {
    if (payload.op === RpcOp.Request) {
      this.events.push(CoreEvent.rpcRequest(payload));
      return;
    }
    if (payload.op === RpcOp.Event) {
      this.events.push(CoreEvent.rpcEvent(payload));
      return;
    }
    if (payload.op === RpcOp.RequestResponse) {
      if (!this.pendingCalls.isPending(payload.requestId) && payload.meta.sourceProtocol === SourceProtocol.JsonRpc) {
        this.outbound.sendRpcResponse(payload);
        return;
      }
      this.pendingCalls.resolve(payload.requestId, payload);
      return;
    }
    if (payload.op === RpcOp.RequestBatchResponse && payload.meta.sourceProtocol === SourceProtocol.JsonRpc) {
      this.outbound.sendRpcResponse(payload);
    }
  }

  private handleStream(payload: StreamPayload): void {
    this.events.push(CoreEvent.streamData(payload));
  }
}
