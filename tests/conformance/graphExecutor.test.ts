import { describe, expect, it } from "vitest";
import { BasicBroker } from "../../src/broker/broker.js";
import { eventMsg } from "../../src/protocol/model.js";
import { evaluateAssertions, executeGraph, observeBrokerNoEvent } from "../../devtools/conformance/graphExecutor.js";

describe("conformance graph executor", () => {
  it("orders dependencies, captures outputs, and resolves later references", async () => {
    const seen: unknown[] = [];
    const context = await executeGraph(
      [
        { id: "identified", role: "observe", captureAs: "session" },
        {
          id: "liveness",
          role: "liveness",
          triggeredBy: "identified",
          jsonrpc: { sid: { ref: "session.sid" } }
        }
      ],
      (_source, resolved) => {
        seen.push(resolved);
        return resolved.id === "identified" ? { sid: "1234abcd" } : resolved;
      }
    );
    expect((seen[1] as { jsonrpc: { sid: string } }).jsonrpc.sid).toBe("1234abcd");
    expect(context.completed).toEqual(new Set(["identified", "liveness"]));
  });

  it("implements bounded no_event observations", async () => {
    const start = performance.now();
    await executeGraph(
      [{ id: "quiet", role: "observe", expect: { no_event: { withinMs: 5 } } }],
      () => undefined,
      {
        observeNoEvent: async (_step, withinMs) => {
          await new Promise((resolve) => setTimeout(resolve, withinMs));
          return true;
        }
      }
    );
    expect(performance.now() - start).toBeGreaterThanOrEqual(4);
  });

  it("fails no_event when the observation hook sees a matching emission", async () => {
    await expect(
      executeGraph(
        [{ id: "quiet", expect: { no_event: { withinMs: 1 } } }],
        () => undefined,
        { observeNoEvent: async () => false }
      )
    ).rejects.toThrow("unexpected event");
  });

  it("observes a clean bounded window through a real broker collector", async () => {
    const broker = new BasicBroker();
    await expect(observeBrokerNoEvent(broker, "audio.algorithmConfigChanged", 5)).resolves.toBe(true);
  });

  it("fails when a matching event is actually dispatched through the broker during the window", async () => {
    const broker = new BasicBroker();
    setTimeout(() => {
      broker.dispatchEvent(eventMsg("12345678", "audio.algorithmConfigChanged", { reason: "test" }));
    }, 1);
    await expect(observeBrokerNoEvent(broker, "audio.algorithmConfigChanged", 20)).resolves.toBe(false);
  });

  it("fails a mismatched capture assertion", async () => {
    const context = await executeGraph(
      [{ id: "identify-step", captureAs: "identified" }],
      () => ({ sid: "1234abcd" })
    );
    expect(() => evaluateAssertions(['identified.sid == ""'], context)).toThrow(
      "assertion failed"
    );
  });

  it("fails a mutated case-level assertion against actual graph output", async () => {
    const context = await executeGraph(
      [{ id: "response" }],
      () => ({ statusCode: 3, requestId: 40 })
    );
    expect(() => evaluateAssertions(["response.statusCode == SUCCESS"], context, {
      SUCCESS: 0
    })).toThrow("assertion failed");
  });

  it("rejects forward capture references before executing the step", async () => {
    await expect(
      executeGraph(
        [{ id: "bad", jsonrpc: { sid: { ref: "future.sid" } } }],
        () => undefined
      )
    ).rejects.toThrow("unknown or forward capture future");
  });

  it.each([
    [[{ id: "same" }, { id: "same" }], "duplicate graph name same"],
    [[{ id: "one", captureAs: "value" }, { id: "two", captureAs: "value" }], "duplicate graph name value"],
    [[{ id: "one", captureAs: "later" }, { id: "later" }], "duplicate graph name later"]
  ] as const)("prevalidates the graph namespace before callbacks", async (steps, message) => {
    let calls = 0;
    await expect(executeGraph(steps, () => { calls += 1; })).rejects.toThrow(message);
    expect(calls).toBe(0);
  });

  it("prevalidates unknown dependency names before callbacks", async () => {
    let calls = 0;
    await expect(executeGraph(
      [{ id: "first" }, { id: "second", triggeredBy: "missing" }],
      () => { calls += 1; }
    )).rejects.toThrow("unknown dependency missing");
    expect(calls).toBe(0);
  });

  it("rejects a reference to a later declared capture before any callback", async () => {
    let calls = 0;
    await expect(executeGraph(
      [
        { id: "first", jsonrpc: { sid: { ref: "session.sid" } } },
        { id: "identified", captureAs: "session" }
      ],
      () => { calls += 1; }
    )).rejects.toThrow("unknown or forward capture session");
    expect(calls).toBe(0);
  });

  it("fails when an adapter reports false", async () => {
    await expect(executeGraph([{ id: "mismatch" }], () => false)).rejects.toThrow(
      "step mismatch failed"
    );
  });

  it("fails when an adapter output does not match the graph expectation", async () => {
    await expect(
      executeGraph(
        [{ id: "response", expect: { rpc: { statusCode: "NOT_SUPPORTED" } } }],
        () => ({ rpc: { statusCode: 0 } })
      )
    ).rejects.toThrow("statusCode mismatch");
  });
});
