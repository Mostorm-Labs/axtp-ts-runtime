import { describe, expect, it, vi } from "vitest";
import { adapterKey, executeSelectedCases, selectCases, validateCapabilityBinding, validateRegistryMethods, type SharedCase } from "../../devtools/conformance/caseDispatch.js";

describe("conformance case dispatch", () => {
  it("fails a newly selected case with an unknown semantic kind", async () => {
    await expect(executeSelectedCases(
      [{ id: "fake.unknown", level: "core", requirement: "required" }],
      () => ({ id: "fake.unknown", level: "core", semantic: { kind: "future_kind" } }),
      {},
      async (_selected, execute) => { await execute(); }
    )).rejects.toThrow("unknown semantic kind future_kind");
  });

  it("skips unsupported scenario cases before adapter classification", async () => {
    const load = vi.fn(() => ({
      id: "stream.video_stream_params_rollback",
      level: "stream",
      scenarios: [{
        name: "reopen_failure_rolls_back",
        steps: [{
          id: "set_params",
          role: "trigger",
          direction: "client_to_server",
          rpc: { op: "REQUEST", method: "cast.setVideoStreamParams" }
        }]
      }]
    }));
    const run = vi.fn();

    await executeSelectedCases(
      [{ id: "stream.video_stream_params_rollback", level: "stream", requirement: "unsupported" }],
      load,
      {},
      run
    );
    expect(load).toHaveBeenCalledTimes(1);
    expect(run).not.toHaveBeenCalled();
  });

  it("classifies legacy cases from their machine-readable shape", () => {
    const shared: SharedCase = { id: "any.binding", level: "capability", steps: [{ id: "lookup", registry_lookup: { capability: "audio.algorithm", methods: [] } }] };
    expect(adapterKey(shared)).toBe("baseline:capability_binding");
  });

  it("exhausts every applicable selected case exactly once", async () => {
    const selected = selectCases(
      { levels: { core: { required_cases: ["a.one", "a.two"] }, extra: { required_cases: ["a.one"] } } },
      { required_levels: ["core"], optional_levels: ["extra"] }
    );
    const executed = vi.fn();
    await executeSelectedCases(
      selected,
      (id) => ({ id, level: "core", semantic: { kind: "invalid_params" } }),
      { "semantic:invalid_params": async () => true },
      async (item, execute) => { executed(item.id, await execute()); }
    );
    expect(executed.mock.calls).toEqual([["a.one", true], ["a.two", true]]);
  });

  it("registry adapter consumes altered YAML methods and expectations", () => {
    const ids = { "audio.getAlgorithmConfig": 0x0901 };
    expect(() => validateRegistryMethods({ methods: ["vendor.changed"] }, { count: { gte: 1 } }, ids)).toThrow("unknown registry method vendor.changed");
    expect(() => validateRegistryMethods({ methods: ["audio.getAlgorithmConfig"] }, { count: { gte: 2 } }, ids)).toThrow("expected at least 2");
  });

  it("method binding validates its own YAML capability, methods, and id", () => {
    expect(() => validateCapabilityBinding(
      "audio.algorithm",
      { capability: "audio.changed", methods: ["audio.getAlgorithmConfig"] },
      { id: 0x0901 },
      { "audio.getAlgorithmConfig": 0x0901 }
    )).toThrow("capability mismatch");
  });
});
