// UnframedJsonLink：unframed-JSON 链路（WebSocket message 边界）。
// 每个 WS message 即一个 AXTP JSON envelope {sid,op,d}。无成帧 / 无 CONTROL / 无 STREAM / 无 CRC。
// 把现 Connection 的 unframed 内联路径（encodeJsonRpc 直发、decodeJsonRpc 直收、keepalive 心跳）内聚。
// 心跳用 transport 原生 keepalive（sendKeepalive/onKeepaliveAck），spec 明确 WS 不走 CONTROL。

import type { Bytes } from "../../io/bytes.js";
import { decodeJsonRpc, encodeJsonRpc } from "../../protocol/codec/jsonRpc.js";
import type { RpcMessage, StreamPayload } from "../../protocol/model.js";
import type { ITransport } from "../../transport/transport.js";
import { AxtpError, ErrorCode } from "../../types/error.js";
import { EventStream } from "../../types/events.js";
import { Heartbeat } from "../heartbeat.js";
import type { Link } from "./link.js";

export interface UnframedJsonLinkOptions {
  /** 心跳间隔 ms。 */
  readonly heartbeatIntervalMs: number;
  /** 心跳超时；缺省 max(interval*2, 10000)。 */
  readonly heartbeatTimeoutMs?: number;
}

export class UnframedJsonLink implements Link {
  // onClosing / onOpenRejected 是接口契约的一部分，但 unframed 无 CONTROL，永不 emit。
  readonly onPayload = new EventStream<RpcMessage>();
  readonly onStream = new EventStream<StreamPayload>();
  readonly onLinkReady = new EventStream<void>();
  readonly onClosing = new EventStream<void>();
  readonly onOpenRejected = new EventStream<number>();
  readonly onHeartbeatTimeout = new EventStream<void>();
  readonly onError = new EventStream<AxtpError>();

  private heartbeat: Heartbeat | undefined;
  private keepaliveUnsub: (() => void) | undefined;
  private stopped = false;

  constructor(
    private readonly transport: ITransport,
    private readonly options: UnframedJsonLinkOptions
  ) {}

  ingest(bytes: Bytes): void {
    const payload = decodeJsonRpc(bytes);
    if (payload !== undefined) this.onPayload.emit(payload);
    else this.onError.emit(new AxtpError(ErrorCode.RpcPayloadInvalid, "malformed JSON envelope"));
  }

  sendRpc(payload: RpcMessage): void {
    this.transport.send(encodeJsonRpc(payload));
  }

  sendStream(_payload: StreamPayload): void {
    throw new AxtpError(ErrorCode.NotSupported, "STREAM not supported on unframed-JSON transport");
  }

  startOpen(): void {
    // 无 CONTROL 协商：连接已建立即 link ready。
    this.onLinkReady.emit(undefined);
  }

  sendClose(): void {
    // 无 CONTROL CLOSE 帧；优雅关闭由 transport.close() 处理。
  }

  start(): void {
    if (!this.transport.capabilities.supportsKeepalive) {
      this.onError.emit(
        new AxtpError(
          ErrorCode.NotSupported,
          "Transport supports neither CONTROL heartbeat nor native keepalive"
        )
      );
      return;
    }
    const interval = this.options.heartbeatIntervalMs;
    const timeout = this.options.heartbeatTimeoutMs ?? Math.max(interval * 2, 10000);
    this.heartbeat = new Heartbeat({
      intervalMs: interval,
      timeoutMs: timeout,
      onTick: () => this.transport.sendKeepalive?.(),
      onTimeout: () => this.onHeartbeatTimeout.emit(undefined)
    });
    this.keepaliveUnsub = this.transport.onKeepaliveAck?.(() => this.heartbeat?.reset());
    this.heartbeat.start();
  }

  stop(): void {
    this.stopped = true;
    this.heartbeat?.stop();
    this.heartbeat = undefined;
    this.keepaliveUnsub?.();
    this.keepaliveUnsub = undefined;
  }

  get isOpen(): boolean {
    // unframed 无 OPEN/ACCEPT：未 stop 即视为可用（sendClose 为 no-op，故此值仅用于 Connection.close 的门控）。
    return !this.stopped;
  }
}
