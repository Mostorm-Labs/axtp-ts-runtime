// Link：统一 framed / unframed 两条链路的抽象（连接语义，归 Connection）。
// Connection 持有一个 Link 实例（按 transport.capabilities 工厂派发），自身不再分支 profile。
// 每个 Link 自持 Heartbeat，把成帧 vs 直发、CONTROL 协商 vs 即时 ready、
// CONTROL Heartbeat vs 原生 keepalive 的差异封装在各自实现里：
//   - FramedLink       (TCP)：帧编解码 + ControlSession + CONTROL 心跳（吸收原 CodecPipeline）
//   - UnframedJsonLink (WS) ：JSON envelope 直收直发 + 原生 keepalive
//
// 对 Session 层完全透明——Session 仍通过 Connection 的 onPayload/onStream/onLinkReady 消费。

import type { Bytes } from "../../io/bytes.js";
import type { RpcPayload, StreamPayload } from "../../protocol/model.js";
import type { AxtpError } from "../../types/error.js";
import type { EventStream } from "../../types/events.js";

/**
 * 链路层统一契约。
 * 入站事件由实现 emit；出站动作由 Connection 委托。
 */
export interface Link {
  /** 解码后的 RPC payload（CONTROL/STREAM 编解码在实现内部完成）。 */
  readonly onPayload: EventStream<RpcPayload>;
  /** 解码后的 STREAM payload（unframed 实现永不 emit——WS 不承载 STREAM）。 */
  readonly onStream: EventStream<StreamPayload>;
  /** 链路 ready：framed 在 OPEN/ACCEPT 协商成功后；unframed 在 startOpen 时即时。 */
  readonly onLinkReady: EventStream<void>;
  /** framed：收到对端 CONTROL CLOSE（Connection 应关闭）。unframed 永不 emit。 */
  readonly onClosing: EventStream<void>;
  /** framed：OPEN 被拒（非零 statusCode ACCEPT）。unframed 永不 emit。 */
  readonly onOpenRejected: EventStream<number>;
  /** 心跳超时（Connection 应 close(HeartbeatTimeout)）。 */
  readonly onHeartbeatTimeout: EventStream<void>;
  /** 链路不可用错误（如 transport 既不支持 CONTROL 也不支持 keepalive）。 */
  readonly onError: EventStream<AxtpError>;

  /** 入站：transport 原始字节 → 解码 → emit onPayload/onStream。 */
  ingest(bytes: Bytes): void;
  /** 出站 RPC：实现内部 encode + 成帧(framed) / 直发(unframed)。 */
  sendRpc(payload: RpcPayload): void;
  /** 出站 STREAM：framed 成帧发送；unframed 抛 NotSupported（spec：WS 不承载 STREAM）。 */
  sendStream(payload: StreamPayload): void;
  /** 发起链路建立：framed client 发 OPEN / framed server no-op（等 OPEN）/ unframed 即时 emit onLinkReady。 */
  startOpen(): void;
  /** 优雅关闭链路：framed 发 CONTROL CLOSE / unframed no-op（由 transport.close() 处理）。 */
  sendClose(): void;
  /** 链路 ready 后启动心跳（实现自持 Heartbeat 实例与 interval/timeout）。 */
  start(): void;
  /** 停止心跳 + 释放监听（重连/关闭时调用）。 */
  stop(): void;
  /** 链路是否 open：framed=ControlSession open；unframed=已启用且未 stop。 */
  readonly isOpen: boolean;
}
