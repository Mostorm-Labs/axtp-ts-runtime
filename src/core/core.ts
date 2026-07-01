// AxtpCore：协议正确性引擎——Core 层的对外门面。
//
// 两条 Web Streams：
//   inbound:  TransformStream<Bytes, CoreEvent>   入站字节→解码→gate/control/handshake/pending 路由→CoreEvent
//   outbound: TransformStream<OutboundMessage, Bytes>  出站消息→编码(framed 成帧/unframed JSON)→字节
//
// Core 内部独占 outbound writer（inbound 解码时产生的自动协议响应——ACCEPT/HEARTBEAT_ACK/握手回复/
// ControlOpenRequired——经此发出）；Endpoint 经 send*/call/emit 显式发送。响应在 inbound 内直接
// pendingCalls.resolve（不外露为 CoreEvent）。
//
// 零 I/O 时序（无心跳/握手超时定时器——在 Endpoint）、零业务 handler（在 Broker）。
// 状态住各子模块；每连接一个 AxtpCore（重连时由 Endpoint 重建）。

import type { Bytes } from "../io/bytes.js";
import {
  encodeHeartbeat,
  encodeHeartbeatAck,
  defaultOpenParams
} from "../protocol/codec/control.js";
import {
  RpcOp,
  eventMsg,
  requestMsg,
  responseMsg,
  type ResponsePayload,
  type RpcMessage,
  type StreamPayload
} from "../protocol/model.js";
import type { LogicalRole, PhysicalRole, TransportProfile } from "../transport/contract.js";
import { supportsControl } from "../transport/profile.js";
import { AxtpError, ErrorCode } from "../types/error.js";
import type { CoreEvent, OutboundMessage } from "./events.js";
import { ControlSession } from "./controlSession.js";
import { Handshake } from "./handshake.js";
import { PendingCalls } from "./pendingCalls.js";
import { classifyInbound, type GateState } from "./runtimeGate.js";
import type { WireAdapter, WireSink } from "./wire/adapter.js";
import { FramedWireAdapter } from "./wire/framed.js";
import { UnframedWireAdapter } from "./wire/unframed.js";

export interface CoreOptions {
  readonly profile: TransportProfile;
  readonly physicalRole: PhysicalRole;
  readonly logicalRole: LogicalRole;
  /** framed：OPEN 提议与入站校验的总帧上限。 */
  readonly maxFrameSize: number;
  /** 心跳间隔（fallback：unframed / 协商前）。 */
  readonly heartbeatIntervalMs: number;
  /** server 生成 sid 的本地熵种子（测试确定性）。 */
  readonly handshakeSeed?: number;
  /** client 在 Identify 携带的 eventMasks（订阅意图）。 */
  readonly eventMasks?: string;
}

export class AxtpCore {
  readonly inbound: TransformStream<Bytes, CoreEvent>;
  readonly outbound: TransformStream<OutboundMessage, Bytes>;

  private readonly outWriter: WritableStreamDefaultWriter<OutboundMessage>;
  private readonly wire: WireAdapter;
  private readonly framed: FramedWireAdapter | undefined;
  private readonly control: ControlSession | undefined;
  private readonly handshake: Handshake;
  private readonly pending = new PendingCalls();
  private gate: GateState = "LINK_CONNECTED";
  private readonly logicalRole: LogicalRole;
  private readonly heartbeatIntervalMs: number;
  /** inbound readable-side controller（start 回调捕获，流生命周期内有效，供任意时机 enqueue）。 */
  private readableCtl: TransformStreamDefaultController<CoreEvent> | undefined;

  constructor(opts: CoreOptions) {
    this.logicalRole = opts.logicalRole;
    this.heartbeatIntervalMs = opts.heartbeatIntervalMs;

    if (supportsControl(opts.profile)) {
      const fa = new FramedWireAdapter(opts.maxFrameSize);
      this.wire = fa;
      this.framed = fa;
      this.control = new ControlSession(
        opts.physicalRole,
        {
          onSendBytes: (body) => this.sendControlBody(body),
          onLinkReady: (neg) => this.onLinkReadyInternal(neg.maxFrameSize, neg.heartbeatIntervalMs),
          onOpenRejected: (sc) => this.enqueue({ kind: "linkOpenRejected", statusCode: sc }),
          onHeartbeat: (cid) => this.sendControlBody(encodeHeartbeatAck(cid)),
          onHeartbeatAck: () => this.enqueue({ kind: "heartbeatAck" }),
          onClosing: () => this.enqueue({ kind: "linkClosing" }),
          onError: (err) => this.enqueue({ kind: "error", err })
        },
        defaultOpenParams(opts.maxFrameSize, opts.heartbeatIntervalMs)
      );
    } else {
      this.wire = new UnframedWireAdapter();
      this.framed = undefined;
      this.control = undefined;
    }
    this.handshake = new Handshake(opts.logicalRole, opts.handshakeSeed, opts.eventMasks);

    this.inbound = new TransformStream<Bytes, CoreEvent>(
      {
        start: (ctl) => {
          this.readableCtl = ctl;
        },
        transform: (bytes) => {
          this.routeBytes(bytes);
        }
      },
      undefined,
      { highWaterMark: 1024 }
    );
    this.outbound = new TransformStream<OutboundMessage, Bytes>(
      {
        transform: (msg, ctl) => {
          for (const c of this.encodeOut(msg)) ctl.enqueue(c);
        }
      },
      undefined,
      { highWaterMark: 1024 }
    );
    this.outWriter = this.outbound.writable.getWriter();
  }

  // ===== 公共：Endpoint 调用 =====

  get sid(): string {
    return this.handshake.sid;
  }

  get isAppReady(): boolean {
    return this.gate === "APP_READY";
  }

  /** unframed：连接建立后由 Endpoint 调用（无 CONTROL 协商）。framed 由内部 ACCEPT 触发。 */
  markLinkReady(): void {
    if (this.framed !== undefined) return; // framed 走 control 协商
    this.onLinkReadyInternal(undefined, this.heartbeatIntervalMs);
  }

  /** 出站 RPC call：分配 requestId + 跟踪 + 发送 + 等响应。失败（非 Success）抛 AxtpError。 */
  call(method: string, params: unknown, timeoutMs: number): Promise<unknown> {
    const { promise } = this.pending.request(
      (id) => this.sendRpc(requestMsg(this.handshake.sid, id, method, params)),
      timeoutMs
    );
    return promise.then((resp) => {
      if (resp.status !== ErrorCode.Success) {
        throw new AxtpError(resp.status, `call ${method} failed`, undefined, resp.requestId);
      }
      return resp.result ?? {};
    });
  }

  /** 出站事件（fire-and-forget）。 */
  emit(event: string, payload: unknown): void {
    this.sendRpc(eventMsg(this.handshake.sid, event, payload));
  }

  /** 出站 STREAM 数据（framed）。 */
  sendStream(msg: StreamPayload): void {
    this.send({ kind: "stream", msg });
  }

  /** framed：CONTROL OPEN（Physical Client 发起链路）。 */
  sendControlOpen(): void {
    this.control?.sendOpen();
  }

  /** framed：CONTROL CLOSE（主动关闭）。 */
  sendClose(): void {
    this.control?.sendClose();
  }

  /** framed：发心跳（Endpoint 定时器调用）。 */
  sendHeartbeat(): void {
    const control = this.control;
    if (control === undefined) return;
    const cid = control.allocControlId();
    this.sendControlBody(encodeHeartbeat(cid));
  }

  /** 断连/关闭：reject 所有 pending（Endpoint 在 disconnect 时调用）。 */
  rejectAllPending(err: AxtpError): void {
    this.pending.rejectAll(err);
  }

  // ===== 内部 =====

  /** 出站 RPC 消息（request/response/event/handshake）—— Endpoint 的 broker 回流也经此。 */
  sendRpc(msg: RpcMessage): void {
    this.send({ kind: "rpc", msg });
  }

  private sendControlBody(body: Bytes): void {
    this.send({ kind: "controlBody", body });
  }

  private send(msg: OutboundMessage): void {
    // close/abort 后 async handler 可能仍调 send→写到已关闭的流，catch 拒绝避免 unhandled rejection。
    this.outWriter.write(msg).catch(() => {});
  }

  private enqueue(ev: CoreEvent): void {
    this.readableCtl?.enqueue(ev);
  }

  private encodeOut(msg: OutboundMessage): Bytes[] {
    switch (msg.kind) {
      case "rpc":
        return this.wire.encodeRpc(msg.msg);
      case "controlBody":
        // unframed 不承载 CONTROL（gate/Endpoint 预检应阻止）；若误调则快速失败而非静默丢弃。
        if (this.framed === undefined)
          throw new AxtpError(
            ErrorCode.NotSupported,
            "CONTROL not supported on unframed transport"
          );
        return this.framed.encodeControlBody(msg.body);
      case "stream":
        if (this.framed === undefined)
          throw new AxtpError(ErrorCode.NotSupported, "STREAM not supported on unframed transport");
        return this.framed.encodeStream(msg.msg);
    }
  }

  private onLinkReadyInternal(maxFrameSize: number | undefined, heartbeatIntervalMs: number): void {
    if (this.gate !== "LINK_CONNECTED") return;
    if (maxFrameSize !== undefined) this.framed?.setMaxFrameSize(maxFrameSize);
    this.gate = "FRAMING_READY";
    this.handshake.onLinkReady();
    if (this.logicalRole === "server") this.sendRpc(this.handshake.startHello());
    this.enqueue({ kind: "linkReady", heartbeatIntervalMs });
  }

  private routeBytes(bytes: Bytes): void {
    const sink: WireSink = {
      onControl: (body) => this.control?.handleControlBody(body),
      onRpc: (msg) => this.handleRpc(msg),
      onStream: (msg) => this.handleStream(msg),
      onError: (err) => this.enqueue({ kind: "error", err })
    };
    this.wire.feedBytes(bytes, sink);
  }

  private handleRpc(msg: RpcMessage): void {
    const d = classifyInbound(this.gate, msg.op);
    switch (d.kind) {
      case "handshake":
        this.handleHandshake(msg);
        break;
      case "business":
        this.dispatchBusiness(msg);
        break;
      case "respond-open-required":
        if (msg.op === RpcOp.Request) {
          this.sendRpc(
            responseMsg(this.handshake.sid, msg.requestId, ErrorCode.ControlOpenRequired)
          );
        }
        break;
      case "drop":
        break;
    }
  }

  private handleHandshake(msg: RpcMessage): void {
    const r = this.handshake.handle(msg);
    if (r.outbound !== undefined) this.sendRpc(r.outbound);
    if (r.becameReady) {
      this.gate = "APP_READY";
      this.enqueue({ kind: "handshakeReady", sid: this.handshake.sid });
    }
    if (r.error !== undefined) this.enqueue({ kind: "handshakeError", err: r.error });
  }

  private dispatchBusiness(msg: RpcMessage): void {
    // APP_READY：校验 sid（spec:211 malformed/empty/non-hex/zero/缺失 MUST 拒绝）
    if (msg.sid !== this.handshake.sid) {
      this.enqueue({
        kind: "error",
        err: new AxtpError(ErrorCode.RpcPayloadInvalid, `invalid sid: ${msg.sid}`)
      });
      return;
    }
    switch (msg.op) {
      case RpcOp.Request:
        this.enqueue({ kind: "rpcRequest", msg });
        break;
      case RpcOp.Event:
        this.enqueue({ kind: "rpcEvent", msg });
        break;
      case RpcOp.RequestResponse:
        this.pending.resolve(msg as ResponsePayload);
        break;
    }
  }

  private handleStream(msg: StreamPayload): void {
    if (this.gate !== "APP_READY") return; // pre-ready stream 丢弃
    this.enqueue({ kind: "streamData", msg });
  }
}
