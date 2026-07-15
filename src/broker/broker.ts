// BasicBroker：入站 RPC 业务分发（broker 层）——不负责 wire 编解码或 transport I/O。
//
// dispatchRequest：router 查 handler → 异步执行 → BrokerSink.onResult(Response)。
//   未注册 method → MethodNotFound；已注册但无 handler/支持 → NotSupported；handler 抛错 → 错误响应 + onError。
// dispatchEvent：router 取全部 handler，逐个同步调用，单个抛错不影响其它。
// Response 经 BrokerSink 回流 Endpoint → core.outbound；handler 事件由注入的 emit 回流。
// dispatch 为单一入口（未来中间件就包这一层，外部 API 不变）。

import {
  responseMsg,
  type EventPayload,
  type RequestPayload,
  type RpcMessage
} from "../protocol/model.js";
import { diagnosticData, emitDiagnostic, type AxtpDiagnostics } from "../diagnostics.js";
import { AxtpError, ErrorCode } from "../types/error.js";
import { EVENT_REGISTRY, METHOD_REGISTRY } from "../protocol/generated/registry.js";
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
  private readonly unavailableMethods = new Set<string>();
  /** Endpoint 注入：handler 内 ctx.emit → core.emit（出站事件）。 */
  emit: ((event: string, payload: unknown) => void) | undefined;
  /** Server 注入：endpoint localId，传入 CallContext 供 handler 做 server 级定向操作。 */
  id: number | undefined;
  /** Endpoint 注入：诊断日志。 */
  diagnostics: AxtpDiagnostics | undefined;

  constructor(globalSource?: GlobalHandlerSource) {
    this.router = new HandlerRouter(globalSource);
  }

  setSink(sink: BrokerSink): void {
    this.sink = sink;
  }

  setMethod(name: string, handler: UntypedMethodHandler): () => void {
    return this.router.setMethod(name, handler);
  }

  setMethodUnavailable(name: string): () => void {
    this.unavailableMethods.add(name);
    return () => this.unavailableMethods.delete(name);
  }

  addEventListener(event: string, handler: UntypedEventHandler): () => void {
    return this.router.addEventListener(event, handler);
  }

  /** 入站 Request 分发（异步派发，不 await——保证 Endpoint reader 不被业务阻塞）。 */
  dispatchRequest(msg: RequestPayload): void {
    const handler = this.router.getMethod(msg.method);
    const emitFn = (event: string, payload: unknown): void => {
      this.emit?.(event, payload);
    };
    const ctx: CallContext = {
      requestId: msg.requestId,
      sid: msg.sid,
      id: this.id,
      emit: emitFn,
      emitRaw: emitFn
    };
    if (handler === undefined) {
      const code = Object.hasOwn(METHOD_REGISTRY, msg.method) || this.unavailableMethods.has(msg.method)
        ? ErrorCode.NotSupported
        : ErrorCode.RpcMethodNotFound;
      this.sink?.onResult(responseMsg(msg.sid, msg.requestId, code));
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
          const message = err instanceof Error ? err.message : String(err);
          this.sink?.onResult(responseMsg(msg.sid, msg.requestId, code, { message }));
        }
      );
  }

  /** 入站 Event 分发：多 handler 同步调用，单个抛错不影响其它。 */
  dispatchEvent(msg: EventPayload): void {
    // Qualified protocol events come from the generated registry. Ignore future/unknown
    // qualified names while retaining the SDK's intentionally open unqualified raw-event API.
    if (msg.eventName.includes(".") && !Object.hasOwn(EVENT_REGISTRY, msg.eventName)) return;
    const handlers = this.router.getEventHandlers(msg.eventName);
    emitDiagnostic(this.diagnostics, {
      level: handlers.size === 0 ? "warn" : "debug",
      scope: "broker",
      event: "event.dispatch",
      sid: msg.sid,
      endpointId: this.id,
      name: msg.eventName,
      handlerCount: handlers.size,
      data: diagnosticData(this.diagnostics, msg.data)
    });
    for (const h of handlers) {
      try {
        h(msg.data);
      } catch (e) {
        this.sink?.onError(new AxtpError(ErrorCode.RpcExecutionFailed, "event handler threw", e));
      }
    }
  }
}
