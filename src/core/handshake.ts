// Handshake：RPC 会话状态机（Hello/Identify/Identified）—— Core 的会话语义。
// 不掺 wire 差异：收到的都是解码好的 RpcMessage。Runtime gate 4 态：
//   LINK_CONNECTED → FRAMING_READY → APP_READY → CLOSING。
//
// 角色：Logical Server 发 Hello/Identified、生成 sid；Logical Client 发 Identify。
// Hello 发送方 = Logical Server（与 Physical 角色正交）。本端生成 sid 时使用 8 位 hex，混合 randomSeed（spec:207）。
// 由 Core inbound transform 编排：link ready 后 server 发 startHello()，handle() 的 outbound 由 Core 发出。

import { AXTP_SPEC_VERSION } from "../protocol/generated/axtpVersion.js";
import type {
  HelloPayload,
  IdentifyPayload,
  IdentifiedPayload,
  RpcMessage
} from "../protocol/model.js";
import { RpcOp, helloMsg, identifiedMsg, identifyMsg } from "../protocol/model.js";
import type { LogicalRole } from "../transport/contract.js";
import { AxtpError, ErrorCode } from "../types/error.js";
import type { GateState } from "./runtimeGate.js";

export interface HandshakeResult {
  /** 待发送的 RpcMessage（若有），由 Core 负责发出。undefined 表示无需回复。 */
  readonly outbound?: RpcMessage;
  readonly becameReady: boolean;
  readonly error?: AxtpError;
}

export class Handshake {
  private stateValue: GateState = "LINK_CONNECTED";
  private sidValue = "";
  private readonly localEntropy: number;
  /** client 在 Identify 携带的 eventMasks（订阅意图）。重连后保留。 */
  private eventMasksValue: string | undefined;

  constructor(
    private readonly logicalRole: LogicalRole,
    /** server 生成本地熵的种子（与 randomSeed 混合生成 sid）。 */
    localSeed?: number,
    /** client 在 Identify 携带的 eventMasks（订阅意图，hex 编码）。 */
    eventMasks?: string
  ) {
    this.localEntropy = localSeed ?? Math.floor(Math.random() * 0x7fffffff) + 1;
    this.eventMasksValue = eventMasks;
  }

  /** 链路层 ready 后调用（framed: ACCEPT 后 / WS: 连接建立后）→ FRAMING_READY。 */
  onLinkReady(): void {
    if (this.stateValue === "LINK_CONNECTED") {
      this.stateValue = "FRAMING_READY";
    }
  }

  /** Logical Server 产生首条 Hello。Logical Client 不调。 */
  startHello(): HelloPayload {
    return helloMsg("", AXTP_SPEC_VERSION);
  }

  /** 处理入站握手消息。返回 outbound（待发送）/ becameReady / error。 */
  handle(payload: RpcMessage): HandshakeResult {
    switch (payload.op) {
      case RpcOp.Hello:
        return this.handleHello(payload);
      case RpcOp.Identify:
        return this.handleIdentify(payload);
      case RpcOp.Identified:
        return this.handleIdentified(payload);
      case RpcOp.Reidentify:
        if (this.logicalRole !== "server" || !this.isReady || payload.sid !== this.sidValue) {
          return { becameReady: false };
        }
        this.eventMasksValue = payload.eventMasks;
        return { outbound: identifiedMsg(this.sidValue), becameReady: true };
      default:
        return { becameReady: false };
    }
  }

  get state(): GateState {
    return this.stateValue;
  }

  get isReady(): boolean {
    return this.stateValue === "APP_READY";
  }

  get sid(): string {
    return this.sidValue;
  }

  get role(): LogicalRole {
    return this.logicalRole;
  }

  get eventMasks(): string | undefined {
    return this.eventMasksValue;
  }

  /** 重连后重置握手状态（重新走 Hello/Identify/Identified）。eventMasks 保留。 */
  reset(): void {
    this.stateValue = "LINK_CONNECTED";
    this.sidValue = "";
  }

  /** 生成 sid：randomSeed ⊕ 本地熵，8 位 hex，非零（spec:207 禁直接当 sid）。 */
  private generateSid(randomSeed: number): string {
    let mixed = ((randomSeed >>> 0) ^ (this.localEntropy >>> 0)) >>> 0;
    if (mixed === 0) mixed = 1;
    return mixed.toString(16).padStart(8, "0");
  }

  private handleHello(_payload: HelloPayload): HandshakeResult {
    if (this.logicalRole !== "client") return { becameReady: false };
    if (this.stateValue === "LINK_CONNECTED") {
      // WS 模式下 Hello 可能在 LINK_CONNECTED 到达（无 CONTROL），直接推进。
      this.stateValue = "FRAMING_READY";
    }
    const randomSeed = (Math.floor(Math.random() * 0x100000000) || 1) >>> 0;
    return { outbound: identifyMsg("", randomSeed, this.eventMasksValue), becameReady: false };
  }

  private handleIdentify(payload: IdentifyPayload): HandshakeResult {
    if (this.logicalRole !== "server") return { becameReady: false };
    this.eventMasksValue = payload.eventMasks;
    this.sidValue = this.generateSid(payload.randomSeed);
    this.stateValue = "APP_READY";
    return { outbound: identifiedMsg(this.sidValue), becameReady: true };
  }

  private handleIdentified(payload: IdentifiedPayload): HandshakeResult {
    if (this.logicalRole !== "client") return { becameReady: false };
    const sid = payload.sid;
    // 接收端把 sid 作为 peer 分配的 opaque token 原样保存/回传。
    // 虽然本端生成 sid 时使用 8 位 hex，但这里不规范化短字符串（如 "3" -> "00000003"），
    // 否则后续业务消息可能和对端 session 表使用的原始 sid 不一致。
    if (sid.length === 0 || sid === "00000000") {
      return {
        becameReady: false,
        error: new AxtpError(ErrorCode.RpcPayloadInvalid, `invalid sid: ${sid}`)
      };
    }
    this.sidValue = sid;
    this.stateValue = "APP_READY";
    return { becameReady: true };
  }
}
