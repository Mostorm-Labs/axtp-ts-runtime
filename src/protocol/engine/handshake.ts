// Handshake：唯一的会话状态机（Hello/Identify/Identified）。
// 不掺 wire 差异——收到的都是解码好的 RpcPayload（由 Connection 上交）。
// 规范 Runtime gate 4 态：LINK_CONNECTED -> FRAMING_READY -> APP_READY -> CLOSING。
// 会话语义，归 Session。
//
// 角色：Logical Server 发 Hello/Identified、生成 sid；Logical Client 发 Identify、校验 axtpVersion。
// Hello 发送方 = Logical Server（与 Physical 角色正交，方向与物理连接方向解耦）。
// sid = 8 位 hex，混合 randomSeed（禁直接当 sid，spec:207）。
// Identified.d = {}（对齐 conformance；sid 在 envelope 外层）。

import { ErrorCode, RpcOp } from "../../protocol/generated/axtp_ids_generated.js";
import type { LogicalRole } from "../../transport/transport.js";
import { AxtpError } from "../../types/error.js";
import type { RpcPayload } from "../model.js";
import { rpcPayload } from "../model.js";

export type SessionState = "LINK_CONNECTED" | "FRAMING_READY" | "APP_READY" | "CLOSING";

export interface HandshakeResult {
  /** 待发送的 payload（若有），由 Connection 负责发送字节。undefined 表示无需回复。 */
  readonly outbound?: RpcPayload;
  /** 是否进入 APP_READY。 */
  readonly becameReady: boolean;
  /** 错误（若有，非致命则 outbound 携带 error response）。 */
  readonly error?: AxtpError;
}

export class Handshake {
  private stateValue: SessionState = "LINK_CONNECTED";
  private sidValue = "";
  private readonly localState: number;

  constructor(
    private readonly logicalRole: LogicalRole,
    /** server 生成本地熵的种子（与 randomSeed 混合生成 sid）。 */
    localSeed?: number
  ) {
    this.localState = localSeed ?? Math.floor(Math.random() * 0x7fffffff) + 1;
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
  startHello(): RpcPayload {
    return rpcPayload({
      op: RpcOp.Hello,
      jsonSid: "",
      body: new TextEncoder().encode(JSON.stringify({ axtpVersion: "1.0.0" })),
      meta: {}
    });
  }

  /**
   * 处理入站握手消息（RpcPayload）。返回 outbound（待发送）/ becameReady / error。
   */
  handle(payload: RpcPayload): HandshakeResult {
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
  generateSid(randomSeed: number): string {
    let mixed = ((randomSeed >>> 0) ^ (this.localState >>> 0)) >>> 0;
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

  /** 重连后重置握手状态（重新走 Hello/Identify/Identified）。 */
  reset(): void {
    this.stateValue = "LINK_CONNECTED";
    this.sidValue = "";
  }

  /** 进入 CLOSING（连接关闭流程）。 */
  enterClosing(): void {
    this.stateValue = "CLOSING";
  }

  /** 期望的 eventMasks（client 在 Identify 携带；server 从 Identify 读取）。 */
  get eventMasks(): string | undefined {
    return this.eventMasksValue;
  }

  private eventMasksValue: string | undefined;

  /** client: 设置要在 Identify 携带的 eventMasks。 */
  setEventMasks(eventMasks: string): void {
    this.eventMasksValue = eventMasks;
  }

  private handleHello(payload: RpcPayload): HandshakeResult {
    // Logical Client 收 Hello 回 Identify；Logical Server 不应收到 Hello（自己是发送方）
    if (this.logicalRole !== "client") {
      return { becameReady: false };
    }
    if (this.stateValue === "LINK_CONNECTED") {
      // WS 模式下 Hello 可能在 LINK_CONNECTED 到达（无 CONTROL），直接推进。
      this.stateValue = "FRAMING_READY";
    }
    // 校验 axtpVersion（spec:205 Hello.axtpVersion 是 spec compatibility authority）
    try {
      const d = JSON.parse(new TextDecoder().decode(payload.body)) as { axtpVersion?: string };
      if (typeof d.axtpVersion === "string" && !d.axtpVersion.startsWith("1.")) {
        return {
          becameReady: false,
          error: new AxtpError(
            ErrorCode.ControlNegotiationFailed,
            `unsupported axtpVersion: ${d.axtpVersion}`
          )
        };
      }
    } catch {
      return {
        becameReady: false,
        error: new AxtpError(ErrorCode.RpcPayloadInvalid, "invalid Hello body")
      };
    }
    // client 回 Identify（带 randomSeed + eventMasks）
    const randomSeed = (Math.floor(Math.random() * 0x7fffffff) + 1) >>> 0;
    const body: Record<string, unknown> = { randomSeed };
    if (this.eventMasksValue) body.eventMasks = this.eventMasksValue;
    const identify = rpcPayload({
      op: RpcOp.Identify,
      jsonSid: "",
      body: new TextEncoder().encode(JSON.stringify(body)),
      meta: { randomSeed, jsonEventMasks: this.eventMacksForMeta() }
    });
    return { outbound: identify, becameReady: false };
  }

  private handleIdentify(payload: RpcPayload): HandshakeResult {
    // Logical Server 收 Identify 回 Identified、生成 sid
    if (this.logicalRole !== "server") {
      return { becameReady: false };
    }
    try {
      const d = JSON.parse(new TextDecoder().decode(payload.body)) as {
        randomSeed?: number;
        eventMasks?: string;
      };
      const randomSeed = typeof d.randomSeed === "number" ? d.randomSeed >>> 0 : 0;
      this.eventMasksValue = typeof d.eventMasks === "string" ? d.eventMasks : "";
      this.sidValue = this.generateSid(randomSeed);
      const identified = rpcPayload({
        op: RpcOp.Identified,
        jsonSid: this.sidValue,
        body: new TextEncoder().encode("{}"),
        meta: {}
      });
      this.stateValue = "APP_READY";
      return { outbound: identified, becameReady: true };
    } catch {
      return {
        becameReady: false,
        error: new AxtpError(ErrorCode.RpcPayloadInvalid, "invalid Identify body")
      };
    }
  }

  private handleIdentified(payload: RpcPayload): HandshakeResult {
    // Logical Client 收 Identified 变 ready
    if (this.logicalRole !== "client") {
      return { becameReady: false };
    }
    // sid 在 envelope 外层（conformance: identified.d == {}）
    const sid = payload.jsonSid ?? "";
    if (!/^[0-9a-fA-F]{8}$/.test(sid)) {
      return {
        becameReady: false,
        error: new AxtpError(ErrorCode.RpcPayloadInvalid, `invalid sid: ${sid}`)
      };
    }
    this.sidValue = sid;
    this.stateValue = "APP_READY";
    return { becameReady: true };
  }

  private eventMacksForMeta(): string | undefined {
    return this.eventMasksValue;
  }
}
