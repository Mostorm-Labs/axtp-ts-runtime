// Link：统一 framed / unframed 两条链路的抽象（连接语义，归 Connection）。
// Connection 持有一个 Link 实例（按 transport.profile.frameMode 工厂派发），自身不再分支 profile。
// Link 只声明两种 frameMode 共有的契约；framed 专有成员（STREAM 收发、CONTROL CLOSE / OPEN 拒绝事件）
// 由 FramedLink 类自身承载，Connection 经 isFramedLink() 类型守卫访问——避免 unframed 链路出现死成员。
//
//   - FramedLink       (standard-framed)：帧编解码 + ControlSession + CONTROL 心跳（吸收原 CodecPipeline）
//   - UnframedJsonLink (unframed-json)  ：JSON envelope 直收直发 + 原生 keepalive
//
// 对 Session 层完全透明——Session 仍通过 Connection 的 onPayload/onStream/onLinkReady 消费。

import type { Bytes } from "../../io/bytes.js";
import type { RpcMessage } from "../../protocol/model.js";
import type { AxtpError } from "../../types/error.js";
import type { EventStream } from "../../types/events.js";

/**
 * 通用链路契约——所有 frameMode 共有的成员。
 * 入站事件由实现 emit；出站动作由 Connection 委托。
 *
 * framed 专有成员（onStream / onClosing / onOpenRejected / sendStream / sendClose）不在此接口——
 * 它们仅 FramedLink 类提供，Connection 用 isFramedLink() 守卫访问，从而 unframed 链路不再背负永不 emit
 * 的死成员（修复 ISP 违反）。
 */
export interface Link {
  /** 解码后的 RPC payload（CONTROL/STREAM 编解码在实现内部完成）。 */
  readonly onPayload: EventStream<RpcMessage>;
  /** 链路 ready：framed 在 OPEN/ACCEPT 协商成功后；unframed 在 startOpen 时即时。 */
  readonly onLinkReady: EventStream<void>;
  /** 心跳超时（Connection 应 close(HeartbeatTimeout)）。 */
  readonly onHeartbeatTimeout: EventStream<void>;
  /** 链路不可用错误（如 transport 既不支持 CONTROL 也不支持原生 keepalive）。 */
  readonly onError: EventStream<AxtpError>;

  /** 入站：transport 原始字节 → 解码 → emit onPayload（framed 还会 emit onStream）。 */
  ingest(bytes: Bytes): void;
  /** 出站 RPC：实现内部 encode + 成帧(framed) / 直发(unframed)。 */
  sendRpc(payload: RpcMessage): void;
  /** 发起链路建立：framed client 发 OPEN / framed server no-op（等 OPEN）/ unframed 即时 emit onLinkReady。 */
  startOpen(): void;
  /** 链路 ready 后启动心跳（实现自持 Heartbeat 实例与 interval/timeout）。 */
  start(): void;
  /** 停止心跳 + 释放监听（重连/关闭时调用）。 */
  stop(): void;
  /** 链路是否 open：framed=ControlSession open；unframed=已启用且未 stop。 */
  readonly isOpen: boolean;
}
