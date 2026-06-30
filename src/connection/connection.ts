// Connection：传输连接 + 链路生命周期编排 + 传输重连（连接语义，不导出）。
// 持有 transport + Link（framed/unframed 按 capabilities 工厂派发）+ ReconnectCoordinator。
// Link 内聚成帧/编解码/CONTROL 协商/心跳；Connection 只做生命周期状态机 + 重连编排 + 事件转发，
// 不再按 profile 分支（唯一的 capabilities 分支收敛在 createLink 工厂）。
//
// 重连：transport.onClose → ReconnectCoordinator → 新 transport → attachTransport(统一重置) → 链路启动。
// 心跳：由 Link 自持（framed=CONTROL Heartbeat/Ack，WS=原生 keepalive），onHeartbeatTimeout → close。

import type { RpcMessage, StreamPayload } from "../protocol/model.js";
import type {
  CloseReason,
  ITransport,
  PhysicalRole,
  TransportFactory
} from "../transport/transport.js";
import { CloseCode } from "../transport/transport.js";
import { AxtpError, ErrorCode } from "../types/error.js";
import { EventStream } from "../types/events.js";
import { FramedLink } from "./link/framedLink.js";
import type { Link } from "./link/link.js";
import { UnframedJsonLink } from "./link/unframedJsonLink.js";
import { resolvePolicy, type ReconnectInfo, type ReconnectPolicy } from "./reconnect/reconnect.js";
import { ReconnectCoordinator } from "./reconnect/reconnectCoordinator.js";

export interface ConnectionOptions {
  heartbeatIntervalMs?: number;
  heartbeatTimeoutMs?: number;
  maxFrameSize?: number;
  reconnect?: ReconnectPolicy;
}

/** Connection 生命周期状态机。 */
export type ConnectionState =
  | "idle" // 构造后未 start
  | "connecting" // start() 后，等链路 ready
  | "ready" // 链路 ready
  | "reconnecting" // 传输断开，重连退避/尝试中
  | "closed"; // 终态

const DEFAULT_HEARTBEAT_INTERVAL_MS = 1000;
const DEFAULT_MAX_FRAME_SIZE = 4096;

export class Connection {
  readonly onClose = new EventStream<CloseReason>();
  readonly onDisconnect = new EventStream<CloseReason>();
  readonly onError = new EventStream<AxtpError>();
  readonly onPayload = new EventStream<RpcMessage>();
  readonly onStream = new EventStream<StreamPayload>();
  readonly onLinkReady = new EventStream<void>();
  readonly onReconnect = new EventStream<ReconnectInfo>();
  readonly onReconnectFailed = new EventStream<void>();

  private transport: ITransport | undefined;
  private readonly transportFactory: TransportFactory;
  private readonly physicalRole: PhysicalRole;
  private readonly options: ConnectionOptions;
  private link: Link | undefined;
  private reconnectCoordinator: ReconnectCoordinator | undefined;
  private transportUnsubs: Array<() => void> = [];

  private connState: ConnectionState = "idle";
  private started = false;

  constructor(
    physicalRole: PhysicalRole,
    transportFactory: TransportFactory,
    options: ConnectionOptions = {}
  ) {
    this.physicalRole = physicalRole;
    this.options = options;
    this.transportFactory = transportFactory;

    const policy = resolvePolicy(options.reconnect);
    if (policy.enabled) {
      this.reconnectCoordinator = new ReconnectCoordinator(
        policy,
        transportFactory,
        (t) => this.handleReconnected(t),
        () => this.handleReconnectFailed(),
        (err) => this.onError.emit(err)
      );
    }
  }

  /** 统一状态转换入口。 */
  private setState(newState: ConnectionState): void {
    if (this.connState === newState) return;
    if (this.connState === "closed") return;
    this.connState = newState;
  }

  get state(): ConnectionState {
    return this.connState;
  }

  get isClosed(): boolean {
    return this.connState === "closed";
  }

  get isReconnecting(): boolean {
    return this.connState === "reconnecting";
  }

  /**
   * 绑定一条 transport：统一重置所有 transport 相关可变状态 + 创建 Link + 订阅事件。
   * 构造和重连都调此方法。确保重连时 Link/pipeline/controlId/心跳完全重置（Link 整体重建）。
   */
  private attachTransport(transport: ITransport): void {
    // 1. detach 旧 transport 订阅
    for (const unsub of this.transportUnsubs) unsub();
    this.transportUnsubs = [];

    // 2. 停旧 Link（含心跳/keepalive 监听）
    this.link?.stop();
    this.link = undefined;

    // 3. 工厂派发创建新 Link（唯一的 capabilities 分支）+ 订阅其事件
    this.link = createLink(this.physicalRole, transport, this.options);
    this.wireLink(this.link);

    // 4. 订阅 transport 事件
    this.transportUnsubs.push(
      transport.onMessage.subscribe((bytes) => {
        if (this.connState === "closed") return;
        this.link?.ingest(bytes);
      })
    );
    this.transportUnsubs.push(
      transport.onClose.subscribe((reason) => this.handleTransportClose(reason))
    );
    this.transportUnsubs.push(transport.onError.subscribe((err) => this.onError.emit(err)));

    transport.attach?.();
  }

  /** 订阅 Link 入站事件，转发为 Connection 事件 / 驱动关闭。 */
  private wireLink(link: Link): void {
    link.onPayload.subscribe((p) => this.onPayload.emit(p));
    link.onStream.subscribe((s) => this.onStream.emit(s));
    link.onLinkReady.subscribe(() => this.handleLinkReady());
    link.onClosing.subscribe(() => this.close(CloseCode.Normal, "remote close"));
    link.onOpenRejected.subscribe((sc) =>
      this.close(CloseCode.HandshakeFailed, `link rejected: 0x${sc.toString(16).padStart(4, "0")}`)
    );
    link.onHeartbeatTimeout.subscribe(() =>
      this.close(CloseCode.HeartbeatTimeout, "heartbeat timeout")
    );
    link.onError.subscribe((err) => this.onError.emit(err));
  }

  start(): void {
    if (this.started) return;
    this.started = true;
    this.setState("connecting");
    void this.openInitialLink();
  }

  /**
   * 首次建立 transport 并启动链路。factory 失败时：
   *   - 有重连策略 → 进入 reconnecting，交给同一 ReconnectCoordinator（与断开重连统一）
   *   - 无重连策略 → terminate（server / disabled 场景）
   * 首次连接失败即协调器的第 1 次重连尝试，与传输断开重连共用同一路径。
   */
  private async openInitialLink(): Promise<void> {
    if (this.isClosed) return;
    try {
      const transport = await this.transportFactory();
      if (this.isClosed) {
        transport.close();
        return;
      }
      this.transport = transport;
      this.attachTransport(transport);
      this.startLinkHandshake();
    } catch (err) {
      if (this.isClosed) return;
      this.onError.emit(
        err instanceof AxtpError
          ? err
          : new AxtpError(ErrorCode.TransportDisconnected, "initial connect failed", err)
      );
      if (this.reconnectCoordinator !== undefined) {
        this.setState("reconnecting");
        this.reconnectCoordinator.start();
      } else {
        this.terminate({
          code: CloseCode.Reconnect,
          reason: "initial connect failed",
          remote: false
        });
      }
    }
  }

  /** 链路握手启动：委托 Link（framed client 发 OPEN / framed server 等 OPEN / WS 即时 ready）。 */
  private startLinkHandshake(): void {
    this.link?.startOpen();
  }

  /**
   * 链路就绪断言。closed 静默丢弃（close 后异步竞态 sendRpc 不击穿）；
   * 其它非 ready 抛 TransportDisconnected。与 Session.requireReady 对齐。
   */
  private assertLinkReady(): boolean {
    if (this.connState === "closed") return false;
    if (this.connState !== "ready")
      throw new AxtpError(
        ErrorCode.TransportDisconnected,
        `connection not ready: ${this.connState}`
      );
    return true;
  }

  sendRpc(payload: RpcMessage): void {
    if (!this.assertLinkReady()) return;
    this.link?.sendRpc(payload);
  }

  sendStream(payload: StreamPayload): void {
    if (!this.assertLinkReady()) return;
    // unframed Link 在此抛 NotSupported（spec：WS 不承载 STREAM）。
    this.link?.sendStream(payload);
  }

  close(code: CloseCode = CloseCode.Normal, reason = "local close"): void {
    if (this.connState === "closed") return;
    this.reconnectCoordinator?.stop();
    this.link?.stop();
    // 死连接（心跳超时 / 传输错误 / 重连失败）：对端已无响应，发 CONTROL CLOSE 帧也收不到 ACK；
    // 此时跳过 sendClose 并用 transport.terminate() 强制断开，避免 ws.close(1000) 在死连接上悬空
    // 等待 close timer。主动优雅关闭（Normal / HandshakeFailed）仍走 sendClose + transport.close()。
    const force =
      code === CloseCode.HeartbeatTimeout ||
      code === CloseCode.TransportError ||
      code === CloseCode.Reconnect;
    if (!force && this.link?.isOpen) this.link.sendClose();
    // 先 terminate（置 connState=closed + emit onClose + cleanupStreams），再断 transport。
    // TCP/WS 的 close()/terminate() 同步 emit onClose → handleTransportClose；此时 connState 已为
    // closed，命中其 `if (connState === "closed") return` 提前返回。否则本地关闭会误发 onDisconnect，
    // 且因 close() 已 stop() 协调器（active=false），handleTransportClose→start() 反而重新武装
    // 重连，定时器到期后 handleReconnected 不检查 closed → “复活”已关闭的连接。
    this.terminate({ code, reason, remote: false });
    const t = this.transport;
    if (force && t?.terminate) t.terminate();
    else t?.close();
  }

  /** 统一收尾：setState(closed) + emit onClose (+可选 onReconnectFailed) + cleanupStreams。 */
  private terminate(reason: CloseReason, emitReconnectFailed = false): void {
    this.setState("closed");
    if (emitReconnectFailed) this.onReconnectFailed.emit(undefined);
    this.onClose.emit(reason);
    this.cleanupStreams();
  }

  // ===== 重连 =====

  private handleTransportClose(reason: CloseReason): void {
    if (this.connState === "closed") return;
    this.link?.stop();

    // 任何断连都通知上层（Session 据此 reject pending calls + abort streams）
    this.onDisconnect.emit(reason);

    if (this.reconnectCoordinator !== undefined) {
      // 有重连策略：进入重连（start 幂等，内部有 active 守卫防重复）
      this.setState("reconnecting");
      this.reconnectCoordinator.start();
      return;
    }

    // 无重连策略：直接关闭
    this.terminate(reason);
  }

  private handleReconnected(newTransport: ITransport): void {
    // 先立即 detach 旧 transport 订阅，防止旧 transport 的异步投递字节进入新 Link
    for (const unsub of this.transportUnsubs) unsub();
    this.transportUnsubs = [];

    this.transport = newTransport;

    const attempt = this.reconnectCoordinator?.attemptCount ?? 0;
    this.onReconnect.emit({ attempt });

    this.attachTransport(newTransport);
    this.setState("connecting");

    this.startLinkHandshake();
  }

  private handleReconnectFailed(): void {
    // 重连耗尽：transport 已不可用，强制 terminate（不等 close 握手）。
    const t = this.transport;
    if (t?.terminate) t.terminate();
    else t?.close();
    this.terminate({ code: CloseCode.Reconnect, reason: "reconnect failed", remote: false }, true);
  }

  // ===== 链路就绪（Link.onLinkReady 统一入口）=====

  private handleLinkReady(): void {
    if (this.connState === "closed") return;
    this.fireLinkReady();
    // 心跳由 Link 自持：interval 在 Link 内部（framed=协商值，unframed=options）。
    this.link?.start();
    // 统一重置重连协调器（framed/unframed 共用此入口，避免 active 永真）。
    this.reconnectCoordinator?.onSuccess();
  }

  private fireLinkReady(): void {
    if (this.connState === "ready") return;
    this.setState("ready");
    this.onLinkReady.emit(undefined);
  }

  private cleanupStreams(): void {
    this.onPayload.close();
    this.onStream.close();
    this.onLinkReady.close();
    this.onError.close();
    this.onReconnect.close();
    this.onReconnectFailed.close();
    this.onDisconnect.close();
    this.onClose.close();
  }
}

/**
 * 按 transport 能力派发 Link 实现。这是 Connection 中唯一读取 capabilities 的地方（工厂派发，非行为分支）。
 */
function createLink(
  physicalRole: PhysicalRole,
  transport: ITransport,
  options: ConnectionOptions
): Link {
  const maxFrameSize = options.maxFrameSize ?? DEFAULT_MAX_FRAME_SIZE;
  const heartbeatIntervalMs = options.heartbeatIntervalMs ?? DEFAULT_HEARTBEAT_INTERVAL_MS;
  if (transport.capabilities.supportsControl) {
    return new FramedLink(physicalRole, transport, {
      maxFrameSize,
      heartbeatIntervalMs,
      heartbeatTimeoutMs: options.heartbeatTimeoutMs
    });
  }
  return new UnframedJsonLink(transport, {
    heartbeatIntervalMs,
    heartbeatTimeoutMs: options.heartbeatTimeoutMs
  });
}
