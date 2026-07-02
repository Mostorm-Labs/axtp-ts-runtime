// PendingCalls：出站 RPC 请求的 requestId→Promise 关联表（core/pendingCalls）。
// 行为契约对齐原 RpcDispatcher：request 原子分配 id+建表+发送、resolve 匹配、超时/断连 rejectAll。

import { describe, expect, it } from "vitest";
import { PendingCalls } from "../../src/core/pendingCalls.js";
import { responseMsg } from "../../src/protocol/model.js";
import { AxtpError, ErrorCode } from "../../src/types/error.js";

describe("PendingCalls", () => {
  it("request: 分配 requestId、同步调 send、随后 resolve 同 id 的 promise 拿到 response", async () => {
    const pc = new PendingCalls();
    let sentId = -1;
    const { requestId, promise } = pc.request((id) => {
      sentId = id;
    }, 1000);
    expect(sentId).toBe(requestId); // send 在建表后被同步调用
    expect(requestId).toBe(1);

    const resp = responseMsg("12345678", requestId, ErrorCode.Success, { ok: true });
    pc.resolve(resp);
    await expect(promise).resolves.toBe(resp);
  });

  it("resolve 未知 requestId 为 no-op（不抛、不影响其它 pending）", async () => {
    const pc = new PendingCalls();
    const { requestId, promise } = pc.request(() => {}, 1000);
    expect(() => pc.resolve(responseMsg("12345678", 999, ErrorCode.Success))).not.toThrow();
    pc.resolve(responseMsg("12345678", requestId, ErrorCode.Success));
    await expect(promise).resolves.toBeTruthy();
  });

  it("requestId 从 1 递增", () => {
    const pc = new PendingCalls();
    const ids = [
      pc.request(() => {}, 1000).requestId,
      pc.request(() => {}, 1000).requestId,
      pc.request(() => {}, 1000).requestId
    ];
    expect(ids).toEqual([1, 2, 3]);
  });

  it("超时：到点未 resolve → promise 以 RpcResponseTimeout + requestId reject", async () => {
    const pc = new PendingCalls();
    const { requestId, promise } = pc.request(() => {}, 20);
    await expect(promise).rejects.toMatchObject({
      code: ErrorCode.RpcResponseTimeout,
      requestId
    });
  });

  it("rejectAll: 所有 pending 以给定 err reject（并清定时器）", async () => {
    const pc = new PendingCalls();
    const p1 = pc.request(() => {}, 1000).promise;
    const p2 = pc.request(() => {}, 1000).promise;
    const err = new AxtpError(ErrorCode.TransportDisconnected, "dc");
    pc.rejectAll(err);
    await expect(p1).rejects.toBe(err);
    await expect(p2).rejects.toBe(err);
  });

  it("request 的 send 抛错：回滚 entry 并 rethrow（不留悬挂 pending）", () => {
    const pc = new PendingCalls();
    expect(() =>
      pc.request(() => {
        throw new Error("boom");
      }, 1000)
    ).toThrow("boom");
    // id=1 已回滚：resolve 它无副作用，且不产生悬挂的 reject
    expect(() => pc.resolve(responseMsg("12345678", 1, ErrorCode.Success))).not.toThrow();
  });
});
