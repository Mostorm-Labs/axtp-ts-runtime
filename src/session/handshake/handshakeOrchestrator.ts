// HandshakeOrchestrator：握手状态机编排（单一职责：Hello/Identify/Identified 流程）。
// 持有 Handshake，onLinkReady 时 Logical Server 发 Hello，ingest 握手消息推进状态。
// 通过 SessionIO 发送 outbound（不直接持有 Connection）。

import type { RpcMessage } from "../../protocol/model.js";
import { RpcOp } from "../../protocol/model.js";
import type { LogicalRole } from "../../transport/contract.js";
import type { AxtpError } from "../../types/error.js";
import type { SessionIO } from "../types.js";
import { Handshake, type SessionState } from "./handshake.js";

/** ingest 返回值：握手推进结果 + 可能的握手错误。 */
export interface HandshakeIngestResult {
  readonly becameReady: boolean;
  readonly error?: AxtpError;
}

export class HandshakeOrchestrator {
  private readonly handshake: Handshake;

  constructor(
    logicalRole: LogicalRole,
    private readonly io: SessionIO,
    seed?: number,
    eventMasks?: string
  ) {
    this.handshake = new Handshake(logicalRole, seed, eventMasks);
  }

  /** 链路 ready 后：Logical Server 发 Hello。 */
  onLinkReady(): void {
    this.handshake.onLinkReady();
    if (this.handshake.role === "server") {
      this.io.sendRpc(this.handshake.startHello());
    }
  }

  /** 处理入站握手消息。返回推进结果（含可能的握手错误）。 */
  ingest(payload: RpcMessage): HandshakeIngestResult {
    const result = this.handshake.handle(payload);
    if (result.outbound) this.io.sendRpc(result.outbound);
    return { becameReady: result.becameReady, error: result.error };
  }

  /** 重连后重置握手状态（重新走 Hello/Identify/Identified）。 */
  reset(): void {
    this.handshake.reset();
  }

  get isReady(): boolean {
    return this.handshake.isReady;
  }

  get sid(): string {
    return this.handshake.sid;
  }

  get state(): SessionState {
    return this.handshake.state;
  }

  static isHandshakeOp(op: RpcOp): boolean {
    return op === RpcOp.Hello || op === RpcOp.Identify || op === RpcOp.Identified;
  }
}
