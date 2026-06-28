// HandshakeOrchestrator：握手状态机编排（单一职责：Hello/Identify/Identified 流程）。
// 持有 Handshake，onLinkReady 时 Logical Server 发 Hello，ingest 握手消息推进状态。
// 通过 SessionIO 发送 outbound（不直接持有 Connection）。

import { Handshake } from "../protocol/engine/handshake.js";
import { RpcOp } from "../protocol/generated/axtp_ids_generated.js";
import type { RpcPayload } from "../protocol/model.js";
import type { LogicalRole } from "../transport/transport.js";

/** Session 提供给子组件的发送接口（避免子组件直接依赖 Connection）。 */
export interface SessionIO {
  sendRpc(payload: RpcPayload): void;
}

export class HandshakeOrchestrator {
  readonly handshake: Handshake;
  private ready = false;

  constructor(
    logicalRole: LogicalRole,
    private readonly io: SessionIO,
    seed?: number,
    eventMasks?: string
  ) {
    this.handshake = new Handshake(logicalRole, seed);
    if (eventMasks) this.handshake.setEventMasks(eventMasks);
  }

  /** 链路 ready 后：Logical Server 发 Hello。 */
  onLinkReady(): void {
    this.handshake.onLinkReady();
    if (this.handshake.role === "server") {
      this.io.sendRpc(this.handshake.startHello());
    }
  }

  /** 处理入站握手消息。返回 becameReady。 */
  ingest(payload: RpcPayload): boolean {
    const result = this.handshake.handle(payload);
    if (result.outbound) this.io.sendRpc(result.outbound);
    if (result.becameReady) {
      this.ready = true;
    }
    return result.becameReady;
  }

  /** 重连后重置握手状态（重新走 Hello/Identify/Identified）。 */
  reset(): void {
    this.ready = false;
    this.handshake.reset();
  }

  get isReady(): boolean {
    return this.ready;
  }

  get sid(): string {
    return this.handshake.sid;
  }

  get state(): string {
    return this.handshake.state;
  }

  /** 当前订阅的 eventMasks（重连时重新携带）。 */
  getEventMasks(): string | undefined {
    return this.handshake.eventMasks;
  }

  static isHandshakeOp(op: RpcOp): boolean {
    return op === RpcOp.Hello || op === RpcOp.Identify || op === RpcOp.Identified;
  }
}
