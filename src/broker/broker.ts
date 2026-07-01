// BasicBroker：入站 RPC 业务分发（broker 层）——零协议知识、零 I/O。
//
// dispatchRequest：router 查 handler → 异步执行 → BrokerSink.onResult(Response)。
//   无 handler → MethodNotFound；handler 抛错 → 错误响应 + onError。
// dispatchEvent：router 取全部 handler，逐个同步调用，单个抛错不影响其它。
// 结果（Response/Event 消息）经 BrokerSink 回流 Endpoint → core.outbound。
// dispatch 为单一入口（未来中间件就包这一层，外部 API 不变）。

import {
  responseMsg,
  type EventPayload,
  type RequestPayload,
  type RpcMessage
} from "../protocol/model.js";
import { AxtpError, ErrorCode } from "../types/error.js";
import type {
  CallContext,
  GlobalHandlerSource,
  UntypedEventHandler,
  UntypedMethodHandler
} from "./context.js";
import { HandlerRouter } from "./router.js";

/** Broker 结果回流（Endpoint 注入：Response/Event 消息 → core.outbound）。 */
export interface BrokerSink {
  onResult(msg: RpcMessage): void;
  onError(err: AxtpError): void;
}

export class BasicBroker {
  private readonly router: HandlerRouter;
  private sink: BrokerSink | undefined;
  /** Endpoint 注入：handler 内 ctx.emit → core.emit（出站事件）。 */
  emit: ((event: string, payload: unknown) => void) | undefined;

  constructor(globalSource?: GlobalHandlerSource) {
    this.router = new HandlerRouter(globalSource);
  }

  setSink(sink: BrokerSink): void {
    this.sink = sink;
  }

  setMethod(name: string, handler: UntypedMethodHandler): () => void {
    return this.router.setMethod(name, handler);
  }

  addEventListener(event: string, handler: UntypedEventHandler): () => void {
    return this.router.addEventListener(event, handler);
  }

  /** 入站 Request 分发（异步派发，不 await——保证 Endpoint reader 不被业务阻塞）。 */
  dispatchRequest(msg: RequestPayload): void {
    const handler = this.router.getMethod(msg.method);
    const ctx: CallContext = {
      requestId: msg.requestId,
      sid: msg.sid,
      emit: (event, payload) => this.emit?.(event, payload)
    };
    if (handler === undefined) {
      this.sink?.onResult(responseMsg(msg.sid, msg.requestId, ErrorCode.RpcMethodNotFound));
      return;
    }
    Promise.resolve()
      .then(() => handler(ctx, msg.params))
      .then(
        (result) =>
          this.sink?.onResult(responseMsg(msg.sid, msg.requestId, ErrorCode.Success, result)),
        (err) => {
          const code = err instanceof AxtpError ? err.code : ErrorCode.RpcExecutionFailed;
          this.sink?.onError(
            new AxtpError(
              code,
              `handler threw: ${err instanceof Error ? err.message : String(err)}`,
              err
            )
          );
          this.sink?.onResult(responseMsg(msg.sid, msg.requestId, code));
        }
      );
  }

  /** 入站 Event 分发：多 handler 同步调用，单个抛错不影响其它。 */
  dispatchEvent(msg: EventPayload): void {
    const handlers = this.router.getEventHandlers(msg.eventName);
    for (const h of handlers) {
      try {
        h(msg.data);
      } catch (e) {
        this.sink?.onError(new AxtpError(ErrorCode.RpcExecutionFailed, "event handler threw", e));
      }
    }
  }
}
