import { BasicBroker, BrokerTaskType } from "./broker.js";
import type { RpcPayload } from "./model.js";
import { rpcPayload } from "./model.js";
import { AxtpCore, CoreEventType } from "./core.js";
import type { Bytes } from "./bytes.js";
import type { ITransport } from "./transport.js";

export class AxtpEndpoint<TBroker extends BasicBroker = BasicBroker> {
  private readonly coreValue = new AxtpCore();
  private transport: ITransport | undefined;

  constructor(private readonly brokerValue: TBroker) {}

  attachTransport(transport: ITransport): void {
    this.transport = transport;
    this.coreValue.configure(transport.profile());
    transport.bind({ onBytes: (bytes) => this.onTransportBytes(bytes) });
  }

  detachTransport(): void {
    this.transport = undefined;
  }

  async poll(maxTasks = 8): Promise<void> {
    this.drainCoreEvents();
    await this.brokerValue.poll(maxTasks);
    this.drainBrokerResults();
    await this.flushOutbound();
  }

  core(): AxtpCore {
    return this.coreValue;
  }

  broker(): TBroker {
    return this.brokerValue;
  }

  onTransportBytes(bytes: Bytes): void {
    this.coreValue.byteSink.onBytes(bytes);
  }

  async sendRpcRequest(payload: RpcPayload): Promise<void> {
    this.coreValue.expectRpcResponse(payload.requestId);
    this.coreValue.sendRpcRequest(payload);
    await this.flushOutbound();
  }

  tryTakeRpcResponse(requestId: number): RpcPayload | undefined {
    return this.coreValue.tryTakeRpcResponse(requestId);
  }

  async flushOutbound(): Promise<void> {
    if (this.transport === undefined) return;
    while (true) {
      const bytes = this.coreValue.tryPopOutboundBytes();
      if (bytes === undefined) return;
      await this.transport.sendBytes(bytes);
    }
  }

  private drainCoreEvents(): void {
    while (true) {
      const event = this.coreValue.pollEvent();
      if (event === undefined) return;
      if (event.type === CoreEventType.RpcRequest && event.rpc !== undefined) {
        this.brokerValue.submit({ type: BrokerTaskType.RpcRequest, rpc: event.rpc });
      } else if (event.type === CoreEventType.RpcEvent && event.rpc !== undefined) {
        this.brokerValue.submit({ type: BrokerTaskType.RpcEvent, rpc: event.rpc });
      } else if (event.type === CoreEventType.StreamData && event.stream !== undefined) {
        this.brokerValue.submit({ type: BrokerTaskType.StreamData, rpc: rpcPayload(), stream: event.stream });
      }
    }
  }

  private drainBrokerResults(): void {
    while (true) {
      const result = this.brokerValue.pollResult();
      if (result === undefined) return;
      this.coreValue.handleBrokerResult(result);
    }
  }
}
