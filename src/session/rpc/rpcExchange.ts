// RpcExchange：RPC 请求/响应/事件 收发（单一职责：call/emit 发起 + dispatcher 匹配 + 入站 Request/Event 路由）。
// 持有 RpcDispatcher（pending call Promise 匹配）。
// 通过 SessionIO 发送，通过 HandlerRouter 路由入站。

import { decodeJsonBody, encodeJsonBody } from "../../protocol/codec/jsonRpc.js";
import { RpcOp } from "../../protocol/generated/axtp_ids_generated.js";
import type { RpcPayload } from "../../protocol/model.js";
import { rpcPayload } from "../../protocol/model.js";
import { AxtpError, ErrorCode } from "../../types/error.js";
import { registry } from "../../types/registry.js";
import type { HandlerRouter } from "../handler/handlerRouter.js";
import type { CallContext, SessionIO } from "../types.js";
import { RpcDispatcher } from "./rpcDispatcher.js";

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
    const methodId = registry.methodId(method) ?? 0;
    const { requestId, promise } = this.dispatcher.request((id) => {
      const rpc = rpcPayload({
        op: RpcOp.Request,
        requestId: id,
        methodOrEventId: methodId,
        jsonSid: this.getSid(),
        body: encodeJsonBody(params),
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
      // 空 body 返回 {}（与 decodeJsonBody 语义一致，不返回 undefined）
      const decoded = decodeJsonBody(payload.body);
      if (decoded === undefined) {
        throw new AxtpError(
          ErrorCode.RpcPayloadInvalid,
          `call ${method} response parse failed`,
          undefined,
          requestId
        );
      }
      return decoded;
    });
  }

  /** 发起 emit（事件发送）。同步 fire-and-forget。 */
  emitEvent(event: string, payload: unknown): void {
    const eventId = registry.eventId(event) ?? 0;
    const rpc = rpcPayload({
      op: RpcOp.Event,
      methodOrEventId: eventId,
      jsonSid: this.getSid(),
      body: encodeJsonBody(payload),
      meta: { jsonMethodOrEventName: event }
    });
    this.io.sendRpc(rpc);
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
      this.sendResponse(payload.requestId, ErrorCode.RpcMethodNotFound);
      return;
    }

    const params = decodeJsonBody(payload.body);
    if (params === undefined) {
      this.sendResponse(payload.requestId, ErrorCode.RpcPayloadInvalid);
      return;
    }

    Promise.resolve()
      .then(() => handler(ctx, params))
      .then(
        (result) => {
          this.sendResponse(payload.requestId, ErrorCode.Success, encodeJsonBody(result));
        },
        (err) => {
          const code = err instanceof AxtpError ? err.code : ErrorCode.RpcExecutionFailed;
          const errMsg = err instanceof Error ? err.message : String(err);
          this.sendResponse(payload.requestId, code, encodeJsonBody({ error: errMsg }));
        }
      );
  }

  /** 入站 Event：路由到 event handler。单个 handler 抛错静默忽略（不影响其它）。 */
  dispatchEvent(payload: RpcPayload): void {
    const eventName = payload.meta.jsonMethodOrEventName ?? "";
    const handlers = this.router.getEventHandlers(eventName);
    if (handlers.size === 0) return;
    const data = decodeJsonBody(payload.body);
    if (data === undefined) return;
    for (const handler of handlers) {
      try {
        handler(data);
      } catch {
        // 单个 handler 抛错静默忽略（与 stream listener 一致）
      }
    }
  }

  /** 断连时 reject 所有 pending。 */
  rejectAll(err: AxtpError): void {
    this.dispatcher.rejectAll(err);
  }

  /** 未 ready 的业务请求 -> 回 CONTROL_OPEN_REQUIRED。 */
  respondOpenRequired(payload: RpcPayload): void {
    if (payload.op === RpcOp.Request) {
      this.sendResponse(payload.requestId, ErrorCode.ControlOpenRequired);
    }
  }

  /** 统一构造 + 发送 RequestResponse（消除重复的 rpcPayload 样板）。 */
  private sendResponse(requestId: number, statusCode: ErrorCode, body?: Uint8Array): void {
    this.io.sendRpc(
      rpcPayload({
        op: RpcOp.RequestResponse,
        requestId,
        statusCode,
        jsonSid: this.getSid(),
        body
      })
    );
  }
}
