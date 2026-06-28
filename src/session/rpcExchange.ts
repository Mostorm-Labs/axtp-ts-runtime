// RpcExchange：RPC 请求/响应收发（单一职责：call 发起 + dispatcher 匹配 + 入站 Request 路由）。
// 持有 RpcDispatcher（pending call Promise 匹配）。
// doCall 发起请求（通过 SessionIO 发送），dispatchRequest 入站 Request 路由到 HandlerRouter。
// 响应到达由 dispatcher.resolve 匹配。

import { RpcDispatcher } from "../protocol/engine/rpcDispatcher.js";
import { ErrorCode, RpcOp } from "../protocol/generated/axtp_ids_generated.js";
import type { RpcPayload } from "../protocol/model.js";
import { rpcPayload } from "../protocol/model.js";
import { AxtpError } from "../types/error.js";
import { registry } from "../types/registry.js";
import type { HandlerRouter } from "./handlerRouter.js";
import type { SessionIO } from "./handshakeOrchestrator.js";
import type { CallContext } from "./session.js";

export class RpcExchange {
  readonly dispatcher = new RpcDispatcher();

  constructor(
    private readonly io: SessionIO,
    private readonly router: HandlerRouter,
    private readonly getSid: () => string,
    private readonly makeCallContext: (requestId: number) => CallContext
  ) {}

  /** 发起 call。 */
  call(method: string, params: unknown, timeoutMs: number): Promise<unknown> {
    const methodId = registry.methodId(method as never) ?? 0;
    const { requestId, promise } = this.dispatcher.request((id) => {
      const rpc = rpcPayload({
        op: RpcOp.Request,
        requestId: id,
        methodOrEventId: methodId,
        jsonSid: this.getSid(),
        body: new TextEncoder().encode(JSON.stringify(params ?? {})),
        meta: { jsonMethodOrEventName: method }
      });
      this.io.sendRpc(rpc);
    }, timeoutMs);

    return promise.then((payload) => {
      if (payload.statusCode !== ErrorCode.Success) {
        throw new AxtpError(
          payload.statusCode as ErrorCode,
          `call ${method} failed`,
          undefined,
          requestId
        );
      }
      if (payload.body.length === 0) return undefined;
      try {
        return JSON.parse(new TextDecoder().decode(payload.body));
      } catch {
        throw new AxtpError(
          ErrorCode.RpcPayloadInvalid,
          `call ${method} response parse failed`,
          undefined,
          requestId
        );
      }
    });
  }

  /** 入站 RequestResponse：匹配 dispatcher。 */
  resolveResponse(payload: RpcPayload): void {
    this.dispatcher.resolve(payload);
  }

  /** 入站 Request：路由到 handler 执行并回响应。 */
  dispatchRequest(payload: RpcPayload): void {
    const methodName = payload.meta.jsonMethodOrEventName ?? "";
    const handler = this.router.getMethod(methodName);
    const ctx = this.makeCallContext(payload.requestId);

    if (handler === undefined) {
      this.io.sendRpc(
        rpcPayload({
          op: RpcOp.RequestResponse,
          requestId: payload.requestId,
          statusCode: ErrorCode.RpcMethodNotFound,
          jsonSid: this.getSid()
        })
      );
      return;
    }

    let params: unknown;
    try {
      params = payload.body.length === 0 ? {} : JSON.parse(new TextDecoder().decode(payload.body));
    } catch {
      this.io.sendRpc(
        rpcPayload({
          op: RpcOp.RequestResponse,
          requestId: payload.requestId,
          statusCode: ErrorCode.RpcPayloadInvalid,
          jsonSid: this.getSid()
        })
      );
      return;
    }

    Promise.resolve()
      .then(() => handler(ctx, params))
      .then(
        (result) => {
          this.io.sendRpc(
            rpcPayload({
              op: RpcOp.RequestResponse,
              requestId: payload.requestId,
              statusCode: ErrorCode.Success,
              jsonSid: this.getSid(),
              body: new TextEncoder().encode(JSON.stringify(result ?? {}))
            })
          );
        },
        (err) => {
          const code = err instanceof AxtpError ? err.code : ErrorCode.RpcExecutionFailed;
          this.io.sendRpc(
            rpcPayload({
              op: RpcOp.RequestResponse,
              requestId: payload.requestId,
              statusCode: code,
              jsonSid: this.getSid()
            })
          );
        }
      );
  }

  /** 断连时 reject 所有 pending。 */
  rejectAll(err: AxtpError): void {
    this.dispatcher.rejectAll(err);
  }

  /** 未 ready 的业务请求 -> CONTROL_OPEN_REQUIRED。 */
  rejectNotReady(payload: RpcPayload): void {
    if (payload.op === RpcOp.Request) {
      this.io.sendRpc(
        rpcPayload({
          op: RpcOp.RequestResponse,
          requestId: payload.requestId,
          statusCode: ErrorCode.ControlOpenRequired,
          jsonSid: this.getSid()
        })
      );
    }
  }
}
