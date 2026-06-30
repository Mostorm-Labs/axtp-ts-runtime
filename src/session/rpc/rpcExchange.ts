// RpcExchange：RPC 请求/响应/事件 收发（单一职责：call/emit 发起 + dispatcher 匹配 + 入站 Request/Event 路由）。
// 持有 RpcDispatcher（pending call Promise 匹配）。
// 通过 SessionIO 发送，通过 HandlerRouter 路由入站。
// 全程操作编码无关的 RpcMessage——params/result/data 是结构化 JS 值，无 bytes 中转/重复编解码。

import type {
  EventPayload,
  RequestPayload,
  ResponsePayload,
  RpcMessage
} from "../../protocol/model.js";
import { RpcOp, eventMsg, requestMsg, responseMsg } from "../../protocol/model.js";
import { AxtpError, ErrorCode } from "../../types/error.js";
import type { HandlerRouter } from "../handler/handlerRouter.js";
import type { CallContext, SessionIO } from "../types.js";
import { RpcDispatcher } from "./rpcDispatcher.js";

export class RpcExchange {
  private readonly dispatcher = new RpcDispatcher();

  constructor(
    private readonly io: SessionIO,
    private readonly router: HandlerRouter,
    private readonly getSid: () => string,
    private readonly makeCallContext: (requestId: number) => CallContext,
    /** 可观测出口：handler 抛错 / 响应投递失败时上报（行为不变，仅新增上报）。 */
    private readonly onError?: (err: AxtpError) => void
  ) {}

  /** 上报到 Session.onError（可观测出口，不改变控制流）。 */
  private reportError(code: ErrorCode, message: string, cause?: unknown): void {
    this.onError?.(new AxtpError(code, message, cause));
  }

  /** 发起 call。 */
  call(method: string, params: unknown, timeoutMs: number): Promise<unknown> {
    const { requestId, promise } = this.dispatcher.request((id) => {
      this.io.sendRpc(requestMsg(this.getSid(), id, method, params));
    }, timeoutMs);

    return promise.then((payload) => {
      if (payload.status !== ErrorCode.Success) {
        throw new AxtpError(payload.status, `call ${method} failed`, undefined, requestId);
      }
      // 无 result 的成功响应返回 {}（与 decodeJsonBody 语义一致，不返回 undefined）。
      return payload.result ?? {};
    });
  }

  /** 发起 emit（事件发送）。同步 fire-and-forget。 */
  emitEvent(event: string, payload: unknown): void {
    this.io.sendRpc(eventMsg(this.getSid(), event, payload));
  }

  /** 入站 RequestResponse：匹配 dispatcher。 */
  resolveResponse(payload: ResponsePayload): void {
    this.dispatcher.resolve(payload);
  }

  /** 入站 Request：路由到 handler 执行并回响应。 */
  dispatchRequest(payload: RequestPayload): void {
    const handler = this.router.getMethod(payload.method);
    const ctx = this.makeCallContext(payload.requestId);

    if (handler === undefined) {
      this.sendResponse(payload.requestId, ErrorCode.RpcMethodNotFound);
      return;
    }

    Promise.resolve()
      .then(() => handler(ctx, payload.params))
      .then(
        (result) => {
          // 链路可能在 handler 异步执行期间转入 reconnecting/closed：Connection.sendRpc 此时同步抛。
          // 结果不可 JSON 序列化时 encodeJsonRpc 也会抛。此处不可让异常逃逸为进程级
          // unhandledRejection——响应无法投递时上报后忽略（对端会重连/超时重试）。
          try {
            this.sendResponse(payload.requestId, ErrorCode.Success, result);
          } catch (e) {
            this.reportError(ErrorCode.RpcExecutionFailed, "response delivery failed", e);
          }
        },
        (err) => {
          const code = err instanceof AxtpError ? err.code : ErrorCode.RpcExecutionFailed;
          // handler 抛错：上报 server.onError（可观测），再尝试回错误响应。
          this.reportError(
            code,
            `handler threw: ${err instanceof Error ? err.message : String(err)}`,
            err
          );
          try {
            this.sendResponse(payload.requestId, code);
          } catch {
            /* 链路不可用：错误响应无法投递，忽略（已上报） */
          }
        }
      );
  }

  /** 入站 Event：路由到 event handler。单个 handler 抛错静默忽略（不影响其它）。 */
  dispatchEvent(payload: EventPayload): void {
    const handlers = this.router.getEventHandlers(payload.eventName);
    if (handlers.size === 0) return;
    for (const handler of handlers) {
      try {
        handler(payload.data);
      } catch (e) {
        // 单个 handler 抛错：上报 onError（不影响其它 handler，与 stream listener 一致）
        this.reportError(ErrorCode.RpcExecutionFailed, "event handler threw", e);
      }
    }
  }

  /** 断连时 reject 所有 pending。 */
  rejectAll(err: AxtpError): void {
    this.dispatcher.rejectAll(err);
  }

  /** 未 ready 的业务请求 -> 回 CONTROL_OPEN_REQUIRED。payload 可能是任意 op，仅 Request 回响应。 */
  respondOpenRequired(payload: RpcMessage): void {
    if (payload.op === RpcOp.Request) {
      this.sendResponse(payload.requestId, ErrorCode.ControlOpenRequired);
    }
  }

  /** 统一构造 + 发送 RequestResponse。 */
  private sendResponse(requestId: number, status: ErrorCode, result?: unknown): void {
    this.io.sendRpc(responseMsg(this.getSid(), requestId, status, result));
  }
}
