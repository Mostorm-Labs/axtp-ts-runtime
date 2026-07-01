// ControlSession：framed 链路层状态机（OPEN/ACCEPT/CLOSE/CLOSE_ACK，framed only）—— Core 的链路语义。
// 与 Handshake（会话语义）正交。spec:121 拒绝 OPEN = 带非零 statusCode 的 ACCEPT（无 REJECT opcode）。
// spec:123 对 OPEN/HEARTBEAT/CLOSE 的 response MUST 回显 controlId。spec:142 Phase1 不要求 ACK/NACK/RESUME。
// 协商结果（maxFrameSize/heartbeatIntervalMs/selectedRpcEncoding）供 Core 配置 codec + 心跳。
// 回调式纯逻辑：由 Core inbound transform 注入（onSendBytes→core 发出、onLinkReady→enqueue CoreEvent）。

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
} from "../protocol/codec/control.js";
import { ControlOpcode, RpcEncoding } from "../protocol/model.js";
import type { PhysicalRole } from "../transport/contract.js";
import { AxtpError, ErrorCode } from "../types/error.js";

export interface NegotiatedLink {
  readonly maxFrameSize: number;
  readonly heartbeatIntervalMs: number;
  readonly selectedRpcEncoding: number;
  readonly accepted: boolean;
}

export interface ControlSessionCallbacks {
  /** 链路 OPEN/ACCEPT 成功（framed）。 */
  onLinkReady?: (negotiated: NegotiatedLink) => void;
  /** OPEN 被拒绝（非零 statusCode 的 ACCEPT）。 */
  onOpenRejected?: (statusCode: number) => void;
  /** 需要发送 CONTROL 字节（已编码的 payload body，Core 负责成帧+发送）。 */
  onSendBytes?: (bytes: Uint8Array) => void;
  /** 收到对端 HEARTBEAT（需回 ack）—— Core 处理发送。 */
  onHeartbeat?: (controlId: number) => void;
  /** 收到对端 HEARTBEAT_ACK（心跳重置）。 */
  onHeartbeatAck?: (controlId: number) => void;
  /** 链路进入 CLOSING（收到 CLOSE）。 */
  onClosing?: (controlId: number) => void;
  /** decode 失败（畸形 CONTROL 帧）：上报，不再静默丢弃。 */
  onError?: (err: AxtpError) => void;
}

type ControlLinkState = "idle" | "opening" | "open" | "closing" | "closed";

const kMinFrameSize = 15; // 12B header + 2B CRC + 至少 1B payload

/**
 * framed-binary 链路层状态机。client：主动发 OPEN 等 ACCEPT；server：等 OPEN 回 ACCEPT（协商）。
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
    this.callbacks.onSendBytes?.(encodeOpen(controlId, this.localParams));
  }

  get isOpen(): boolean {
    return this.linkState === "open";
  }

  /** 主动发起 CLOSE。 */
  sendClose(): void {
    const controlId = this.allocControlId();
    this.linkState = "closing";
    this.callbacks.onSendBytes?.(encodeClose(controlId));
  }

  /** 处理入站 CONTROL payload body（已剥离 frame header）。畸形帧 try/catch 上报。 */
  handleControlBody(body: Uint8Array): void {
    let decoded;
    try {
      decoded = decodeControl(body);
    } catch (err) {
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
    if (this.physicalRole !== "server") return;
    if (this.linkState === "closed" || this.linkState === "closing") return;
    const required = [
      tlv.maxFrameSize,
      tlv.heartbeatIntervalMs,
      tlv.supportedPayloadTypes,
      tlv.supportedRpcEncodings,
      tlv.ackMode
    ];
    if (
      required.some((v) => v === undefined) ||
      (tlv.maxFrameSize ?? 0) < kMinFrameSize ||
      !((tlv.supportedRpcEncodings ?? 0) & RpcEncoding.Json)
    ) {
      this.callbacks.onSendBytes?.(
        encodeRejectedAccept(controlId, ErrorCode.ControlNegotiationFailed)
      );
      return;
    }
    const maxFrameSize = Math.min(
      this.localParams.maxFrameSize,
      tlv.maxFrameSize ?? this.localParams.maxFrameSize
    );
    const heartbeatIntervalMs = clampHeartbeatInterval(
      tlv.heartbeatIntervalMs ?? this.localParams.heartbeatIntervalMs
    );
    const acceptParams: NegotiationParams = {
      maxFrameSize,
      supportedPayloadTypes: this.localParams.supportedPayloadTypes,
      heartbeatIntervalMs,
      ackMode: this.localParams.ackMode,
      selectedRpcEncoding: RpcEncoding.Json
    };
    this.callbacks.onSendBytes?.(encodeAccept(controlId, acceptParams));
    this.linkState = "open";
    this.negotiated = {
      maxFrameSize,
      heartbeatIntervalMs,
      selectedRpcEncoding: RpcEncoding.Json,
      accepted: true
    };
    this.callbacks.onLinkReady?.(this.negotiated);
  }

  private handleAccept(
    controlId: number,
    statusCode: number,
    tlv: Partial<NegotiationParams>
  ): void {
    if (this.physicalRole !== "client") return;
    if (controlId !== this.pendingOpenId) return;
    this.pendingOpenId = undefined;
    if (statusCode !== ErrorCode.Success) {
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
    this.callbacks.onSendBytes?.(encodeCloseAck(controlId));
    this.callbacks.onClosing?.(controlId);
  }

  /** 分配 controlId（OPEN/CLOSE/HEARTBEAT 共用）。从 1 递增，&0xffff 回滚。 */
  allocControlId(): number {
    const id = this.nextControlId;
    this.nextControlId = (this.nextControlId + 1) & 0xffff;
    return id;
  }
}
