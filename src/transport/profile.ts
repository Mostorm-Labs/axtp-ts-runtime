// TransportProfile：对齐 AXTP spec 的传输 profile 能力模型。
// 协议（axtp.protocol.yaml:102-168 + specs/20-core.md:54-63）的权威模型是
// mode（envelope 形态）派生出 supportsControl / supportsStream / 心跳机制，外加 rpcEncodings。
// 本模块把 mode 抽象为 FrameMode（wire envelope 维度），把派生量实现为纯函数访问器，
// 取代旧的 { supportsControl, supportsKeepalive } 两个对偶布尔。

import { RpcEncoding } from "../protocol/model.js";

/**
 * Wire envelope 形态（spec 两条生产路径）。
 *
 * 仅 2 值：standard-framed（Standard Frame 二进制外壳，承载 CONTROL/RPC/STREAM）
 * 与 unframed-json（每条传输 message 即一个 JSON envelope，仅 RPC）。
 *
 * spec 的 AXTP-WS-CLOUD-REVERSE 在 wire 上与 AXTP-WS-JSON 完全同构（都传输 JSON
 * envelope），其差异纯属拓扑（物理方向≠逻辑角色），由 PhysicalRole/LogicalRole 表达，
 * 故不在此单列 frameMode 值。
 */
export type FrameMode = "standard-framed" | "unframed-json";

/** 已知 profile 身份（spec transports[].name）。可选——自定义 transport 可省略。 */
export type TransportProfileId =
  | "AXTP-TCP"
  | "AXTP-USB-HID"
  | "AXTP-WS-JSON"
  | "AXTP-WS-CLOUD-REVERSE";

/** 心跳机制，由 frameMode 派生。 */
export type KeepaliveMode = "control-heartbeat" | "native-keepalive" | "none";

/**
 * Transport profile——能力与心跳的单一事实来源。
 *
 * supportsControl / supportsStream / keepaliveMode 是派生访问器（纯函数），不作为字段，
 * 因为它们完全由 frameMode 决定。
 *
 * rpcEncodings 语义：本地运行时**实际实现**的编码（用于 CONTROL OPEN TLV 位图构建与
 * 编码器分发），**不是** spec 为该 profile 列出的全集——我们无法编码尚未实现的内容。
 * Phase 1 framed 与 unframed 都只实现 JSON。未来加入 CBOR/JSON_BINARY 时，framed
 * profile 的 rpcEncodings 增长，frameMode 不变。
 */
export interface TransportProfile {
  /** Wire envelope 形态——能力与心跳的单一事实来源。 */
  readonly frameMode: FrameMode;
  /** 本地实际实现的 RPC 编码，按优先级排序。Phase 1 = [Json]。 */
  readonly rpcEncodings: readonly RpcEncoding[];
  /** 已知 profile 身份（若使用 spec 定义的 transport）。自定义 transport = undefined。 */
  readonly profileId?: TransportProfileId;
}

/** 是否承载 CONTROL OPEN/ACCEPT/CLOSE/Heartbeat（= standard-framed）。 */
export function supportsControl(profile: TransportProfile): boolean {
  return profile.frameMode === "standard-framed";
}

/** 是否承载 STREAM 数据平面载荷（= standard-framed）。 */
export function supportsStream(profile: TransportProfile): boolean {
  return profile.frameMode === "standard-framed";
}

/** 协议导出的心跳机制：framed=CONTROL Heartbeat；unframed-json=原生 keepalive（WS ping/pong）。 */
export function keepaliveMode(profile: TransportProfile): KeepaliveMode {
  return profile.frameMode === "standard-framed" ? "control-heartbeat" : "native-keepalive";
}

// ===== profile 工厂 =====

/** Standard Framed Binary profile（AXTP-TCP / AXTP-USB-HID）。profileId 可选。 */
export function framedBinaryProfile(profileId?: "AXTP-TCP" | "AXTP-USB-HID"): TransportProfile {
  return { frameMode: "standard-framed", rpcEncodings: [RpcEncoding.Json], profileId };
}

/**
 * WebSocket Unframed JSON profile。
 * cloud-reverse 拓扑传 "AXTP-WS-CLOUD-REVERSE"（仅诊断身份；frameMode 仍是 unframed-json，
 * 拓扑差异由 PhysicalRole/LogicalRole 表达）。
 */
export function unframedJsonProfile(
  profileId?: "AXTP-WS-JSON" | "AXTP-WS-CLOUD-REVERSE"
): TransportProfile {
  return {
    frameMode: "unframed-json",
    rpcEncodings: [RpcEncoding.Json],
    profileId: profileId ?? "AXTP-WS-JSON"
  };
}
