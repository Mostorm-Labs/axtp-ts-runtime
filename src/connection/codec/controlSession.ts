// ControlSession：链路层状态机（OPEN/ACCEPT/CLOSE/CLOSE_ACK，framed only）。
// 连接语义，归 Connection。与 Handshake（会话语义）正交。
// spec:121 拒绝 OPEN = 带非零 statusCode 的 ACCEPT（无 REJECT opcode）。
// spec:123 对 OPEN/HEARTBEAT/CLOSE 的 response MUST 回显 controlId。
// spec:142 Phase1 不要求 ACK/NACK/RESUME。
// 协商（maxFrameSize/heartbeatIntervalMs/supportedRpcEncodings）结果供 Connection 配置 codec + 心跳。

import {
  decodeControl,
  defaultOpenParams,
  encodeAccept,
  encodeClose,
  encodeCloseAck,
  encodeOpen,
  encodeReject,
  type NegotiationParams
} from "../../protocol/codec/control.js";
import { ControlOpcode, RpcEncoding } from "../../protocol/model.js";
import type { PhysicalRole } from "../../transport/transport.js";
import { ErrorCode } from "../../types/error.js";

export interface NegotiatedLink {
  readonly maxFrameSize: number;
  readonly heartbeatIntervalMs: number;
  readonly selectedRpcEncoding: number;
  readonly accepted: boolean;
}

export interface ControlSessionCallbacks {
  /** 链路 OPEN/ACCEPT 成功（framed）。 */
  onLinkReady?: (negotiated: NegotiatedLink) => void;
  /** D1: 链路被拒绝（非零 statusCode 的 ACCEPT），Connection 应关闭连接。 */
  onRejected?: (statusCode: number) => void;
  /** 需要发送字节（CONTROL 帧）。 */
  onSendBytes?: (bytes: Uint8Array) => void;
  /** 收到对端 HEARTBEAT（需回 ack）—— 由 Connection 处理发送。 */
  onHeartbeat?: (controlId: number) => void;
  /** 收到对端 HEARTBEAT_ACK（心跳重置）。 */
  onHeartbeatAck?: (controlId: number) => void;
  /** 链路进入 CLOSING（收到 CLOSE）。 */
  onClosing?: (controlId: number) => void;
}

/**
 * framed-binary 链路层状态机。
 * client 角色：主动发 OPEN，等 ACCEPT。
 * server 角色：等 OPEN，回 ACCEPT（协商）。
 */
export class ControlSession {
  private open = false;
  private closing = false;
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
    const controlId = this.takeControlId();
    this.pendingOpenId = controlId;
    const bytes = encodeOpen(controlId, this.localParams);
    this.callbacks.onSendBytes?.(bytes);
  }

  get isOpen(): boolean {
    return this.open && !this.closing;
  }

  /** 主动发起 CLOSE。 */
  sendClose(): void {
    const controlId = this.takeControlId();
    this.closing = true;
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
    } catch {
      // 畸形 CONTROL 帧（body 不足 5B 等）：静默丢弃
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
        this.closing = true;
        this.open = false;
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
      this.callbacks.onSendBytes?.(encodeReject(controlId, ErrorCode.ControlNegotiationFailed));
      return;
    }

    // 校验对端是否支持 JSON（本期 JSON-only）
    const peerSupportsJson = (tlv.supportedRpcEncodings ?? 0) & RpcEncoding.Json;
    if (!peerSupportsJson) {
      this.callbacks.onSendBytes?.(encodeReject(controlId, ErrorCode.ControlNegotiationFailed));
      return;
    }

    // 协商：取双方较小 maxFrameSize，heartbeat 取对方值，selectedRpcEncoding = JSON。
    // 上方 requiredFields 校验已确保非 undefined，但 TS 无法跨行收窄，用局部变量避免 !。
    const negotiatedMaxFrame = tlv.maxFrameSize ?? this.localParams.maxFrameSize;
    const negotiatedHeartbeat = tlv.heartbeatIntervalMs ?? this.localParams.heartbeatIntervalMs;
    const maxFrameSize = Math.min(this.localParams.maxFrameSize, negotiatedMaxFrame);
    const heartbeatIntervalMs = negotiatedHeartbeat;
    // spec:127-134 ACCEPT 必需字段（不含 OPEN 专用的 supportedRpcEncodings/supportedPayloadTypes）
    const acceptParams: NegotiationParams = {
      maxFrameSize,
      supportedPayloadTypes: this.localParams.supportedPayloadTypes,
      heartbeatIntervalMs,
      ackMode: this.localParams.ackMode,
      selectedRpcEncoding: RpcEncoding.Json // JSON only
    };
    const bytes = encodeAccept(controlId, acceptParams);
    this.open = true;
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
      // D1: 被拒绝——通知 Connection 关闭连接（而非静默挂死）
      this.negotiated = {
        maxFrameSize: 0,
        heartbeatIntervalMs: 0,
        selectedRpcEncoding: 0,
        accepted: false
      };
      this.callbacks.onRejected?.(statusCode);
      return;
    }
    this.open = true;
    this.negotiated = {
      maxFrameSize: tlv.maxFrameSize ?? this.localParams.maxFrameSize,
      heartbeatIntervalMs: tlv.heartbeatIntervalMs ?? this.localParams.heartbeatIntervalMs,
      selectedRpcEncoding: tlv.selectedRpcEncoding ?? RpcEncoding.Json,
      accepted: true
    };
    this.callbacks.onLinkReady?.(this.negotiated);
  }

  private handleClose(controlId: number): void {
    this.closing = true;
    this.open = false;
    // 回 CLOSE_ACK（回显 controlId）
    this.callbacks.onSendBytes?.(encodeCloseAck(controlId));
    this.callbacks.onClosing?.(controlId);
  }

  private takeControlId(): number {
    const id = this.nextControlId;
    this.nextControlId = (this.nextControlId + 1) & 0xffff;
    return id;
  }
}
