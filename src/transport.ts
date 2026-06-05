import type { Bytes } from "./bytes.js";
import type { ByteSink } from "./io.js";
import { RpcEncoding } from "./generated/axtp_ids_generated.js";

export enum TransportKind {
  Tcp = "tcp",
  WebSocket = "websocket",
  Hid = "hid",
  Ble = "ble",
  Uart = "uart",
  Mock = "mock",
  Custom = "custom"
}

export enum AxtpWireMode {
  FramedBinary = "framedBinary",
  WebSocketJsonRpc = "webSocketJsonRpc"
}

export interface TransportProfile {
  kind: TransportKind;
  wireMode: AxtpWireMode;
  defaultRpcEncoding: RpcEncoding;
  messageOriented: boolean;
  supportsTextMessage: boolean;
  supportsBinaryMessage: boolean;
  preferredFrameSize: number;
}

export interface ITransport {
  bind(sink: ByteSink): void;
  open(): void | Promise<void>;
  close(): void | Promise<void>;
  sendBytes(bytes: Bytes): void | Promise<void>;
  profile(): TransportProfile;
}

export function defaultTransportProfile(): TransportProfile {
  return {
    kind: TransportKind.Custom,
    wireMode: AxtpWireMode.FramedBinary,
    defaultRpcEncoding: RpcEncoding.Tlv,
    messageOriented: false,
    supportsTextMessage: false,
    supportsBinaryMessage: true,
    preferredFrameSize: 4096
  };
}

export class MockTransport implements ITransport {
  private sink: ByteSink | undefined;
  private readonly outgoing: Bytes[] = [];
  private opened = false;

  constructor(private readonly transportProfile: TransportProfile = {
    kind: TransportKind.Mock,
    wireMode: AxtpWireMode.FramedBinary,
    defaultRpcEncoding: RpcEncoding.Tlv,
    messageOriented: false,
    supportsTextMessage: false,
    supportsBinaryMessage: true,
    preferredFrameSize: 4096
  }) {}

  bind(sink: ByteSink): void {
    this.sink = sink;
  }

  open(): void {
    this.opened = true;
  }

  close(): void {
    this.opened = false;
  }

  isOpen(): boolean {
    return this.opened;
  }

  injectIncoming(bytes: Bytes): void {
    this.sink?.onBytes(bytes);
  }

  sendBytes(bytes: Bytes): void {
    this.outgoing.push(bytes.slice());
  }

  profile(): TransportProfile {
    return { ...this.transportProfile };
  }

  tryPopOutgoing(): Bytes | undefined {
    return this.outgoing.shift();
  }

  queuedOutgoingCount(): number {
    return this.outgoing.length;
  }
}
