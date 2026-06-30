// Handshake：唯一的会话状态机（Hello/Identify/Identified）。
// 不掺 wire 差异——收到的都是解码好的 RpcMessage（由 Connection 上交）。
// 规范 Runtime gate 4 态：LINK_CONNECTED -> FRAMING_READY -> APP_READY -> CLOSING。
// 会话语义，归 Session。
//
// 角色：Logical Server 发 Hello/Identified、生成 sid；Logical Client 发 Identify、校验 axtpVersion。
// Hello 发送方 = Logical Server（与 Physical 角色正交，方向与物理连接方向解耦）。
// sid = 8 位 hex，混合 randomSeed（禁直接当 sid，spec:207）。
// Identified.d = {}（对齐 conformance；sid 在 envelope 外层）。

import { AXTP_SPEC_VERSION } from "../../protocol/generated/axtpVersion.js";
import type {
  HelloPayload,
  IdentifyPayload,
  IdentifiedPayload,
  RpcMessage
} from "../../protocol/model.js";
import { RpcOp, helloMsg, identifiedMsg, identifyMsg } from "../../protocol/model.js";
import type { LogicalRole } from "../../transport/contract.js";
import { AxtpError, ErrorCode } from "../../types/error.js";

/**
 * axtpVersion 兼容判定：主版本=1 即接受（spec:205 它是 spec compatibility authority，
 * spec 未规定精确匹配算法）。支持 "1"/"1.0"/"1.0.0"；拒绝 "2.x"/空串。
 */
function isAxtpVersionCompatible(version: string): boolean {
  const major = Number.parseInt(version, 10);
  return Number.isInteger(major) && major === 1;
}

export type SessionState = "LINK_CONNECTED" | "FRAMING_READY" | "APP_READY" | "CLOSING";

export interface HandshakeResult {
  /** 待发送的 RpcMessage（若有），由 Connection 负责发送字节。undefined 表示无需回复。 */
  readonly outbound?: RpcMessage;
  /** 是否进入 APP_READY。 */
  readonly becameReady: boolean;
  /** 错误（若有）。 */
  readonly error?: AxtpError;
}

export class Handshake {
  private stateValue: SessionState = "LINK_CONNECTED";
  private sidValue = "";
  private readonly localEntropy: number;
  /** client 在 Identify 携带的 eventMasks（订阅意图）。重连后保留（handler 表未变）。 */
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

  /**
   * 链路层 ready 后调用（framed: ACCEPT 后 / WS: 连接建立后）。
   * 进入 FRAMING_READY。server 在此后会发 Hello（见 startHello）。
   */
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
      default:
        return { becameReady: false };
    }
  }

  /** 生成 sid：randomSeed ⊕ 本地状态，8 位 hex，非零（spec:207 禁直接当 sid）。 */
  private generateSid(randomSeed: number): string {
    let mixed = ((randomSeed >>> 0) ^ (this.localEntropy >>> 0)) >>> 0;
    if (mixed === 0) mixed = 1;
    return mixed.toString(16).padStart(8, "0");
  }

  get state(): SessionState {
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

  /** 重连后重置握手状态（重新走 Hello/Identify/Identified）。eventMasks 保留。 */
  reset(): void {
    this.stateValue = "LINK_CONNECTED";
    this.sidValue = "";
  }

  private handleHello(payload: HelloPayload): HandshakeResult {
    // Logical Client 收 Hello 回 Identify；Logical Server 不应收到 Hello（自己是发送方）
    if (this.logicalRole !== "client") {
      return { becameReady: false };
    }
    if (this.stateValue === "LINK_CONNECTED") {
      // WS 模式下 Hello 可能在 LINK_CONNECTED 到达（无 CONTROL），直接推进。
      this.stateValue = "FRAMING_READY";
    }
    // spec:205: axtpVersion 是 spec compatibility authority。缺失/主版本非 1 都拒绝。
    const version = payload.axtpVersion;
    if (!isAxtpVersionCompatible(version)) {
      return {
        becameReady: false,
        error: new AxtpError(
          ErrorCode.RpcPayloadInvalid,
          `unsupported or missing axtpVersion: ${version || "(absent)"}`
        )
      };
    }
    // client 回 Identify（带 randomSeed + eventMasks）
    const randomSeed = (Math.floor(Math.random() * 0x100000000) || 1) >>> 0;
    const identify = identifyMsg("", randomSeed, this.eventMasksValue);
    return { outbound: identify, becameReady: false };
  }

  private handleIdentify(payload: IdentifyPayload): HandshakeResult {
    // Logical Server 收 Identify 回 Identified、生成 sid
    if (this.logicalRole !== "server") {
      return { becameReady: false };
    }
    this.sidValue = this.generateSid(payload.randomSeed);
    const identified = identifiedMsg(this.sidValue);
    this.stateValue = "APP_READY";
    return { outbound: identified, becameReady: true };
  }

  private handleIdentified(payload: IdentifiedPayload): HandshakeResult {
    // Logical Client 收 Identified 变 ready
    if (this.logicalRole !== "client") {
      return { becameReady: false };
    }
    // sid 在 envelope 外层（conformance: identified.d == {}）
    const sid = payload.sid;
    // spec:211 APP_READY 后 malformed/empty/non-hex/zero sid MUST 拒绝
    if (!/^[0-9a-fA-F]{8}$/.test(sid) || sid === "00000000") {
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
