// FramedLink：framed-binary 链路（TCP）。
// 内聚现 Connection 的 framed 路径：CodecPipeline（帧解码/重组/payload 分发 + ControlSession 状态机）
// + Heartbeat（CONTROL Heartbeat/Ack）。sendRpc 内部 encode+成帧，ingest 帧解码，OPEN/ACCEPT 协商与
// CONTROL 心跳全部在此完成。Connection 只看到 RpcPayload/StreamPayload 与链路事件。
//
// 构造无副作用（不发字节）；server 角色等 OPEN，startOpen() 为 no-op。

import type { Bytes } from "../../io/bytes.js";
import { encodeJsonRpc } from "../../protocol/codec/jsonRpc.js";
import type { RpcPayload, StreamPayload } from "../../protocol/model.js";
import type { ITransport, PhysicalRole } from "../../transport/transport.js";
import type { AxtpError } from "../../types/error.js";
import { EventStream } from "../../types/events.js";
import { CodecPipeline } from "../codec/codecPipeline.js";
import { Heartbeat } from "../heartbeat.js";
import type { Link } from "./link.js";

export interface FramedLinkOptions {
  /** 总帧大小上限（含 12B header + 2B CRC），用于 OPEN 提议与入站校验。 */
  readonly maxFrameSize: number;
  /** OPEN 提议的心跳间隔（协商后被对端值覆盖）。 */
  readonly heartbeatIntervalMs: number;
  /** 心跳超时；缺省 max(interval*2, 10000)。 */
  readonly heartbeatTimeoutMs?: number;
}

export class FramedLink implements Link {
  readonly onPayload = new EventStream<RpcPayload>();
  readonly onStream = new EventStream<StreamPayload>();
  readonly onLinkReady = new EventStream<void>();
  readonly onClosing = new EventStream<void>();
  readonly onOpenRejected = new EventStream<number>();
  readonly onHeartbeatTimeout = new EventStream<void>();
  readonly onError = new EventStream<AxtpError>();

  private heartbeat: Heartbeat | undefined;
  /** 协商后的心跳间隔（start() 使用）；link ready 前为 undefined。 */
  private negotiatedIntervalMs: number | undefined;

  private readonly pipeline: CodecPipeline;
  private readonly fallbackIntervalMs: number;
  private readonly heartbeatTimeoutMs: number | undefined;

  constructor(
    private readonly physicalRole: PhysicalRole,
    transport: ITransport,
    options: FramedLinkOptions
  ) {
    this.fallbackIntervalMs = options.heartbeatIntervalMs;
    this.heartbeatTimeoutMs = options.heartbeatTimeoutMs;

    this.pipeline = new CodecPipeline(
      physicalRole,
      transport,
      { maxFrameSize: options.maxFrameSize, heartbeatIntervalMs: options.heartbeatIntervalMs },
      {
        onRpc: (p) => this.onPayload.emit(p),
        onStream: (s) => this.onStream.emit(s),
        onControlHeartbeat: (cid) => this.pipeline.sendHeartbeatAck(cid),
        onControlHeartbeatAck: () => this.heartbeat?.reset(),
        onControlClosing: () => this.onClosing.emit(undefined),
        onControlOpenRejected: (sc) => this.onOpenRejected.emit(sc),
        onLinkReady: (neg) => {
          if (!neg.accepted) return;
          this.pipeline.setMaxFrameSize(neg.maxFrameSize);
          this.negotiatedIntervalMs = neg.heartbeatIntervalMs;
          this.onLinkReady.emit(undefined);
        }
      }
    );
  }

  ingest(bytes: Bytes): void {
    this.pipeline.onBytes(bytes);
  }

  sendRpc(payload: RpcPayload): void {
    this.pipeline.sendRpc(encodeJsonRpc(payload));
  }

  sendStream(payload: StreamPayload): void {
    this.pipeline.sendStreamPayload(payload);
  }

  startOpen(): void {
    // 仅 Physical Client 主动发 OPEN；server 构造后等待对端 OPEN（CodecPipeline 构造无副作用）。
    if (this.physicalRole === "client") this.pipeline.sendOpen();
  }

  sendClose(): void {
    this.pipeline.sendClose();
  }

  start(): void {
    const interval = this.negotiatedIntervalMs ?? this.fallbackIntervalMs;
    const timeout = this.heartbeatTimeoutMs ?? Math.max(interval * 2, 10000);
    this.heartbeat = new Heartbeat({
      intervalMs: interval,
      timeoutMs: timeout,
      onTick: () => {
        const cid = this.pipeline.allocControlId();
        this.pipeline.sendHeartbeat(cid);
      },
      onTimeout: () => this.onHeartbeatTimeout.emit(undefined)
    });
    this.heartbeat.start();
  }

  stop(): void {
    this.heartbeat?.stop();
    this.heartbeat = undefined;
  }

  get isOpen(): boolean {
    return this.pipeline.controlSessionIsOpen;
  }
}
