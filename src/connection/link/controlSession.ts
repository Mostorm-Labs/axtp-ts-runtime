// ControlSession：链路层状态机（OPEN/ACCEPT/CLOSE/CLOSE_ACK，framed only）。
// 连接语义，归 Connection。与 Handshake（会话语义）正交。
// spec:121 拒绝 OPEN = 带非零 statusCode 的 ACCEPT（无 REJECT opcode）。
// spec:123 对 OPEN/HEARTBEAT/CLOSE 的 response MUST 回显 controlId。
// spec:142 Phase1 不要求 ACK/NACK/RESUME。
// 协商（maxFrameSize/heartbeatIntervalMs/supportedRpcEncodings）结果供 Connection 配置 codec + 心跳。

import {
  clampHeartbeatInterval,
  decodeControl,
  defaultOpenParams,
  encodeAccept,
  encodeClose,
  encodeCloseAck,
  encodeOpen,
  encodeRejectedAccept,
  type NegotiationParams
} from "../../protocol/codec/control.js";
import { ControlOpcode, RpcEncoding } from "../../protocol/model.js";
import type { PhysicalRole } from "../../transport/contract.js";
import { AxtpError, ErrorCode } from "../../types/error.js";

export interface NegotiatedLink {
  readonly maxFrameSize: number;
  readonly heartbeatIntervalMs: number;
  readonly selectedRpcEncoding: number;
  readonly accepted: boolean;
}

export interface ControlSessionCallbacks {
  /** 链路 OPEN/ACCEPT 成功（framed）。 */
  onLinkReady?: (negotiated: NegotiatedLink) => void;
  /** D1: OPEN 被拒绝（非零 statusCode 的 ACCEPT），Connection 应关闭连接。 */
  onOpenRejected?: (statusCode: number) => void;
  /** 需要发送字节（CONTROL 帧）。 */
  onSendBytes?: (bytes: Uint8Array) => void;
  /** 收到对端 HEARTBEAT（需回 ack）—— 由 Connection 处理发送。 */
  onHeartbeat?: (controlId: number) => void;
  /** 收到对端 HEARTBEAT_ACK（心跳重置）。 */
  onHeartbeatAck?: (controlId: number) => void;
  /** 链路进入 CLOSING（收到 CLOSE）。 */
  onClosing?: (controlId: number) => void;
  /** decode 失败（畸形 CONTROL 帧）：上报，不再静默丢弃。 */
  onError?: (err: AxtpError) => void;
}

/** 链路层生命周期状态（与 Connection/Session/Handshake 的显式状态机风格一致）。 */
export type ControlLinkState =
  | "idle" // 构造后，未发 OPEN / 未收 OPEN
  | "opening" // client 已发 OPEN，等 ACCEPT
  | "open" // OPEN/ACCEPT 成功，链路可用
  | "closing" // 本端已发 CLOSE，等 CLOSE_ACK
  | "closed"; // 收到 CLOSE / CLOSE_ACK，链路终结

/**
 * framed-binary 链路层状态机。
 * client 角色：主动发 OPEN，等 ACCEPT。
 * server 角色：等 OPEN，回 ACCEPT（协商）。
 */
export class ControlSession {
  private linkState: ControlLinkState = "idle";
  private nextControlId = 1;
  private pendingOpenId: number | undefined;
  private negotiated: NegotiatedLink | undefined;
  private readonly localParams: NegotiationParams;

  constructor(
    private readonly physicalRole: PhysicalRole,
    private readonly callbacks: ControlSessionCallbacks,
    localParams?: NegotiationParams
  ) {
    this.localParams = localParams ?? defaultOpenParams();
  }

  /** Physical Client: 发起 OPEN。 */
  sendOpen(): void {
    const controlId = this.allocControlId();
    this.pendingOpenId = controlId;
    this.linkState = "opening";
    const bytes = encodeOpen(controlId, this.localParams);
    this.callbacks.onSendBytes?.(bytes);
  }

  get isOpen(): boolean {
    return this.linkState === "open";
  }

  /** 主动发起 CLOSE。 */
  sendClose(): void {
    const controlId = this.allocControlId();
    this.linkState = "closing";
    const bytes = encodeClose(controlId);
    this.callbacks.onSendBytes?.(bytes);
  }

  /**
   * 处理入站 CONTROL 字节（已剥离 frame header 的 payload body）。
   * B7: 包 try/catch 防止畸形帧导致未捕获异常。
   */
  handleControlBody(body: Uint8Array): void {
    let decoded;
    try {
      decoded = decodeControl(body);
    } catch (err) {
      // 畸形 CONTROL 帧（body 不足 5B 等）：上报 onError，不再静默丢弃
      this.callbacks.onError?.(
        new AxtpError(ErrorCode.ControlPayloadInvalid, "malformed CONTROL frame", err)
      );
      return;
    }
    switch (decoded.opcode) {
      case ControlOpcode.Open:
        this.handleOpen(decoded.controlId, decoded.tlv);
        break;
      case ControlOpcode.Accept:
        this.handleAccept(decoded.controlId, decoded.statusCode, decoded.tlv);
        break;
      case ControlOpcode.Heartbeat:
        this.callbacks.onHeartbeat?.(decoded.controlId);
        break;
      case ControlOpcode.HeartbeatAck:
        this.callbacks.onHeartbeatAck?.(decoded.controlId);
        break;
      case ControlOpcode.Close:
        this.handleClose(decoded.controlId);
        break;
      case ControlOpcode.CloseAck:
        this.linkState = "closed";
        break;
    }
  }

  private handleOpen(controlId: number, tlv: Partial<NegotiationParams>): void {
    // Physical Server 处理 OPEN 回 ACCEPT
    if (this.physicalRole !== "server") return;

    // spec:127-134 + spec:123: 校验 OPEN 必需 TLV 存在性（缺失/异常 → CONTROL_NEGOTIATION_FAILED）
    const requiredFields = [
      tlv.maxFrameSize,
      tlv.heartbeatIntervalMs,
      tlv.supportedPayloadTypes,
      tlv.supportedRpcEncodings,
      tlv.ackMode
    ];
    if (requiredFields.some((v) => v === undefined)) {
      this.callbacks.onSendBytes?.(encodeRejectedAccept(controlId, ErrorCode.ControlNegotiationFailed));
      return;
    }

    // 校验 maxFrameSize 合理性（12B header + 2B CRC + 至少 1B payload = 15B 最小帧）
    if ((tlv.maxFrameSize ?? 0) < 15) {
      this.callbacks.onSendBytes?.(encodeRejectedAccept(controlId, ErrorCode.ControlNegotiationFailed));
      return;
    }

    // 校验对端是否支持 JSON（本期 JSON-only）
    const peerSupportsJson = (tlv.supportedRpcEncodings ?? 0) & RpcEncoding.Json;
    if (!peerSupportsJson) {
      this.callbacks.onSendBytes?.(encodeRejectedAccept(controlId, ErrorCode.ControlNegotiationFailed));
      return;
    }

    // 协商：maxFrameSize 取双方较小；heartbeat 取 peer 值（非双方 min 协商），并 clamp 到合法范围。
    // 上方 requiredFields 校验已确保非 undefined，但 TS 无法跨行收窄，用局部变量避免 !。
    const negotiatedMaxFrame = tlv.maxFrameSize ?? this.localParams.maxFrameSize;
    const peerHeartbeat = tlv.heartbeatIntervalMs ?? this.localParams.heartbeatIntervalMs;
    const maxFrameSize = Math.min(this.localParams.maxFrameSize, negotiatedMaxFrame);
    const heartbeatIntervalMs = clampHeartbeatInterval(peerHeartbeat);
    // spec:127-134 ACCEPT 必需字段（不含 OPEN 专用的 supportedRpcEncodings/supportedPayloadTypes）
    const acceptParams: NegotiationParams = {
      maxFrameSize,
      supportedPayloadTypes: this.localParams.supportedPayloadTypes,
      heartbeatIntervalMs,
      ackMode: this.localParams.ackMode,
      selectedRpcEncoding: RpcEncoding.Json // JSON only
    };
    const bytes = encodeAccept(controlId, acceptParams);
    this.linkState = "open";
    this.negotiated = {
      maxFrameSize,
      heartbeatIntervalMs,
      selectedRpcEncoding: RpcEncoding.Json,
      accepted: true
    };
    this.callbacks.onSendBytes?.(bytes);
    this.callbacks.onLinkReady?.(this.negotiated);
  }

  private handleAccept(
    controlId: number,
    statusCode: number,
    tlv: Partial<NegotiationParams>
  ): void {
    // Physical Client 处理 ACCEPT（自己发的 OPEN 的回应）
    if (this.physicalRole !== "client") return;
    if (controlId !== this.pendingOpenId) return;
    this.pendingOpenId = undefined;
    if (statusCode !== ErrorCode.Success) {
      // D1: 被拒绝——保持 opening（由 Connection.onControlOpenRejected → close() 接管关闭）。
      this.negotiated = {
        maxFrameSize: 0,
        heartbeatIntervalMs: 0,
        selectedRpcEncoding: 0,
        accepted: false
      };
      this.callbacks.onOpenRejected?.(statusCode);
      return;
    }
    this.linkState = "open";
    this.negotiated = {
      maxFrameSize: tlv.maxFrameSize ?? this.localParams.maxFrameSize,
      heartbeatIntervalMs: tlv.heartbeatIntervalMs ?? this.localParams.heartbeatIntervalMs,
      selectedRpcEncoding: tlv.selectedRpcEncoding ?? RpcEncoding.Json,
      accepted: true
    };
    this.callbacks.onLinkReady?.(this.negotiated);
  }

  private handleClose(controlId: number): void {
    this.linkState = "closed";
    // 回 CLOSE_ACK（回显 controlId）
    this.callbacks.onSendBytes?.(encodeCloseAck(controlId));
    this.callbacks.onClosing?.(controlId);
  }

  /**
   * 分配一个 controlId（OPEN/CLOSE/HEARTBEAT 等所有 CONTROL 共用，单一分配器）。
   * 从 1 递增，&0xffff 回滚。spec:123 response MUST 回显 controlId；OPEN 在 handleAccept
   * 校验 pendingOpenId，HEARTBEAT/CLOSE 的 ack 仅按 opcode 处理（cid 不参与匹配判定）。
   */
  allocControlId(): number {
    const id = this.nextControlId;
    this.nextControlId = (this.nextControlId + 1) & 0xffff;
    return id;
  }
}
