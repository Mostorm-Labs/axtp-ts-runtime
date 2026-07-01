// BasicBroker：入站 RPC 分发（broker/broker）。零协议知识、零 I/O。
// dispatchRequest → handler → onResult(Response)；dispatchEvent → handlers。
// 异步派发（不 await handler），结果经 BrokerSink 回流 Endpoint→core。

import { describe, expect, it } from "vitest";
import { BasicBroker } from "../../src/broker/broker.js";
import {
  RpcOp,
  eventMsg,
  requestMsg,
  type ResponsePayload,
  type RpcMessage
} from "../../src/protocol/model.js";
import { AxtpError, ErrorCode } from "../../src/types/error.js";

function capture() {
  const results: RpcMessage[] = [];
  const stats = { errs: 0 };
  const broker = new BasicBroker();
  broker.setSink({ onResult: (m) => results.push(m), onError: () => (stats.errs += 1) });
  return { broker, results, stats };
}

const tick = (): Promise<void> => new Promise((r) => setTimeout(r, 0));

describe("BasicBroker — dispatchRequest", () => {
  it("有 handler → onResult(Success + result)；handler 收到 CallContext(requestId/sid)", async () => {
    const { broker, results } = capture();
    let gotCtx: { requestId: number; sid: string } | undefined;
    broker.setMethod("add", (ctx, p) => {
      gotCtx = ctx as { requestId: number; sid: string };
      return (p as { a: number }).a + 1;
    });
    broker.dispatchRequest(requestMsg("12345678", 5, "add", { a: 1 }));
    await tick();
    expect(results).toHaveLength(1);
    const r = results[0] as ResponsePayload;
    expect(r.op).toBe(RpcOp.RequestResponse);
    expect(r.status).toBe(ErrorCode.Success);
    expect(r.result).toBe(2);
    expect(gotCtx).toMatchObject({ requestId: 5, sid: "12345678" });
  });

  it("无 handler → onResult(MethodNotFound)", async () => {
    const { broker, results } = capture();
    broker.dispatchRequest(requestMsg("12345678", 1, "nope", {}));
    await tick();
    expect((results[0] as ResponsePayload).status).toBe(ErrorCode.RpcMethodNotFound);
  });

  it("handler 抛 AxtpError → onResult(其 code) + onError", async () => {
    const { broker, results, stats } = capture();
    broker.setMethod("boom", () => {
      throw new AxtpError(ErrorCode.RpcExecutionFailed, "x");
    });
    broker.dispatchRequest(requestMsg("12345678", 2, "boom", {}));
    await tick();
    expect((results[0] as ResponsePayload).status).toBe(ErrorCode.RpcExecutionFailed);
    expect(stats.errs).toBe(1);
  });

  it("handler 抛普通 Error → onResult(RpcExecutionFailed) + onError", async () => {
    const { broker, results, stats } = capture();
    broker.setMethod("boom", () => {
      throw new Error("plain");
    });
    broker.dispatchRequest(requestMsg("12345678", 2, "boom", {}));
    await tick();
    expect((results[0] as ResponsePayload).status).toBe(ErrorCode.RpcExecutionFailed);
    expect(stats.errs).toBe(1);
  });

  it("ctx.emit 调用 broker.emit（Endpoint 绑定 core.emit）", async () => {
    const { broker } = capture();
    let emitted: [string, unknown] | undefined;
    broker.emit = (e, p) => {
      emitted = [e, p];
    };
    broker.setMethod("m", (ctx) => {
      ctx.emit("ev", { x: 1 });
      return 0;
    });
    broker.dispatchRequest(requestMsg("12345678", 1, "m", {}));
    await tick();
    expect(emitted).toEqual(["ev", { x: 1 }]);
  });
});

describe("BasicBroker — dispatchEvent", () => {
  it("多 handler 都调用；单个抛错不影响其它 + onError", () => {
    const { broker, stats } = capture();
    let n = 0;
    broker.addEventListener("e", () => {
      throw new Error("x");
    });
    broker.addEventListener("e", () => (n += 1));
    broker.dispatchEvent(eventMsg("12345678", "e", { d: 1 }));
    expect(n).toBe(1);
    expect(stats.errs).toBe(1);
  });

  it("无 handler 的 event：no-op，不报错", () => {
    const { broker, stats } = capture();
    expect(() => broker.dispatchEvent(eventMsg("12345678", "none", {}))).not.toThrow();
    expect(stats.errs).toBe(0);
  });
});
