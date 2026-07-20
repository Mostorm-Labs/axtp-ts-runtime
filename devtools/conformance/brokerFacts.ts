import { METHOD_REGISTRY } from "../../src/types/registry.js";
import type { SharedCase } from "./caseDispatch.js";

export function buildBrokerFacts(shared: SharedCase): Record<string, unknown> {
  const facts: Record<string, unknown> = {
    SUCCESS: 0,
    NOT_SUPPORTED: 3,
    OUT_OF_RANGE: 0x0b,
    RPC_METHOD_NOT_FOUND: 0x36,
    true: true,
    false: false,
    null: null,
    session: { state: "identified" },
    capability: { events: [] }
  };

  for (const method of Object.keys(METHOD_REGISTRY)) {
    facts[`registry.method("${method}")`] = {};
  }

  const declaredCapabilities = shared.given?.capability;
  if (typeof declaredCapabilities === "object" && declaredCapabilities !== null) {
    for (const [name, value] of Object.entries(declaredCapabilities)) {
      facts[`capability("${name}")`] = value;
    }
  }

  return facts;
}
