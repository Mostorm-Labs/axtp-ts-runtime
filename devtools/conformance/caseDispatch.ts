import fs from "node:fs";
import path from "node:path";
import { parse } from "yaml";
import type { GraphStep } from "./graphExecutor.js";

export type Requirement = "required" | "optional" | "unsupported";
export interface SelectedCase { id: string; level: string; requirement: Requirement }
export interface SharedCase {
  id: string;
  level: string;
  semantic?: { kind?: string };
  given?: Record<string, unknown>;
  scenarios?: Array<{ name: string; steps: GraphStep[]; assertions?: string[] }>;
  steps?: GraphStep[];
  assertions?: string[];
}
export interface RuntimeProfile {
  required_levels?: string[];
  optional_levels?: string[];
  unsupported_levels?: string[];
}
export type CaseAdapter = (shared: SharedCase) => boolean | Promise<boolean>;

const ADAPTER_DEFINITIONS = {
  "semantic:baseline_handshake": { applicable: true },
  "semantic:advisory_version_handshake": { applicable: true },
  "semantic:invalid_params": { applicable: true },
  "semantic:unknown_method_error": { applicable: true },
  "semantic:registered_method_unavailable": { applicable: true },
  "semantic:registered_feature_degradation": { applicable: true },
  "semantic:profile_degradation": { applicable: true },
  "semantic:unknown_event_receiver_tolerance": { applicable: false },
  "semantic:unsubscribed_event_sender_suppression": { applicable: false },
  "semantic:unsupported_event_sender_suppression": { applicable: false },
  "baseline:registry_methods": { applicable: true },
  "baseline:capability_binding": { applicable: true },
  "baseline:error_shape": { applicable: true },
  "baseline:broker_rpc": { applicable: true },
  "baseline:jsonrpc_session": { applicable: true },
  "baseline:framed_control": { applicable: false },
  "baseline:stream": { applicable: false },
  "baseline:event": { applicable: false }
} as const;

export type AdapterKey = keyof typeof ADAPTER_DEFINITIONS;
export type SemanticKind = AdapterKey extends infer K
  ? K extends `semantic:${infer S}` ? S : never
  : never;
export type ApplicableAdapterKey = {
  [K in AdapterKey]: typeof ADAPTER_DEFINITIONS[K]["applicable"] extends true ? K : never
}[AdapterKey];
export type AdapterRegistry = Record<ApplicableAdapterKey, CaseAdapter>;
export type PartialAdapterRegistry = Partial<AdapterRegistry>;

function isSemanticKind(value: string): value is SemanticKind {
  return Object.hasOwn(ADAPTER_DEFINITIONS, `semantic:${value}`);
}

function isApplicableAdapter(key: AdapterKey): key is ApplicableAdapterKey {
  return ADAPTER_DEFINITIONS[key].applicable;
}

export function validateRegistryMethods(
  lookup: { methods?: string[] },
  expected: { count?: { gte?: number } },
  methodIds: Readonly<Record<string, number>>
): void {
  if (lookup.methods === undefined) throw new Error("registry lookup has no methods");
  for (const method of lookup.methods) {
    if (methodIds[method] === undefined) throw new Error(`unknown registry method ${method}`);
  }
  const minimum = expected.count?.gte;
  if (minimum === undefined) throw new Error("registry expectation has no minimum count");
  if (Object.keys(methodIds).length < minimum) throw new Error(`expected at least ${minimum} registry methods`);
}

export function validateCapabilityBinding(
  declaredCapability: unknown,
  lookup: { capability?: string; methods?: string[] },
  expected: { id?: number },
  methodIds: Readonly<Record<string, number>>
): void {
  if (lookup.capability === undefined || lookup.capability !== declaredCapability) throw new Error("capability mismatch");
  if (lookup.methods === undefined) throw new Error("capability binding has no methods");
  if (expected.id === undefined) throw new Error("capability binding has no expected id");
  const domainId = expected.id >>> 8;
  for (const method of lookup.methods) {
    const id = methodIds[method];
    if (id === undefined) throw new Error(`unknown registry method ${method}`);
    if ((id >>> 8) !== domainId) throw new Error(`method ${method} is outside capability domain`);
  }
}

export function selectCases(
  manifest: { levels: Record<string, { required_cases: string[] }> },
  profile: RuntimeProfile
): SelectedCase[] {
  const required = new Set(profile.required_levels ?? []);
  const optional = new Set(profile.optional_levels ?? []);
  const unsupported = new Set(profile.unsupported_levels ?? []);
  const rank: Record<Requirement, number> = { unsupported: 0, optional: 1, required: 2 };
  const selected = new Map<string, SelectedCase>();
  for (const [level, definition] of Object.entries(manifest.levels)) {
    const requirement: Requirement = required.has(level) ? "required" : optional.has(level) ? "optional" : "unsupported";
    if (requirement === "unsupported" && !unsupported.has(level)) continue;
    for (const id of definition.required_cases) {
      const previous = selected.get(id);
      if (previous === undefined || rank[requirement] > rank[previous.requirement]) {
        selected.set(id, { id, level, requirement });
      }
    }
  }
  return [...selected.values()];
}

export function loadSelectedCases(specPath: string, profilePath: string): SelectedCase[] {
  const manifest = parse(fs.readFileSync(path.join(specPath, "conformance/manifest.yaml"), "utf8")) as {
    levels: Record<string, { required_cases: string[] }>;
  };
  const profile = parse(fs.readFileSync(profilePath, "utf8")) as RuntimeProfile;
  return selectCases(manifest, profile);
}

export function loadSharedCase(specPath: string, id: string): SharedCase {
  const parts = id.split(".");
  if (parts.length !== 2) throw new Error(`invalid conformance case id ${id}`);
  return parse(fs.readFileSync(path.join(specPath, "conformance/cases", parts[0], `${parts[1]}.yaml`), "utf8")) as SharedCase;
}

export function adapterKey(shared: SharedCase): AdapterKey {
  const semantic = shared.semantic?.kind;
  if (semantic !== undefined) {
    if (!isSemanticKind(semantic)) throw new Error(`unknown semantic kind ${semantic}`);
    return `semantic:${semantic}`;
  }
  const steps = shared.steps ?? [];
  if (steps.some((step) => step.registry_lookup !== undefined)) {
    const lookup = steps.find((step) => step.registry_lookup !== undefined)?.registry_lookup as { capability?: unknown } | undefined;
    return lookup?.capability === undefined ? "baseline:registry_methods" : "baseline:capability_binding";
  }
  if (steps.some((step) => step.error !== undefined)) return "baseline:error_shape";
  if (steps.some((step) => step.rpc !== undefined)) return "baseline:broker_rpc";
  if (steps.some((step) => step.jsonrpc !== undefined)) return "baseline:jsonrpc_session";
  if (steps.some((step) => step.control !== undefined)) return "baseline:framed_control";
  if (steps.some((step) => step.stream !== undefined)) return "baseline:stream";
  if (steps.some((step) => step.event !== undefined)) return "baseline:event";
  throw new Error(`unsupported selected case shape for ${shared.id}`);
}

export async function executeSelectedCases(
  selected: readonly SelectedCase[],
  load: (id: string) => SharedCase,
  adapters: Readonly<PartialAdapterRegistry>,
  run: (selected: SelectedCase, execute: () => Promise<boolean>) => Promise<void>
): Promise<void> {
  const classified = selected.flatMap((item) => {
    const shared = load(item.id);
    if (shared.id !== item.id) throw new Error(`case file id ${shared.id} does not match ${item.id}`);
    if (item.requirement === "unsupported") return [];
    const key = adapterKey(shared);
    return [{ item, shared, key }];
  });
  for (const { item, shared, key } of classified) {
    await run(item, async () => {
      if (!isApplicableAdapter(key)) throw new Error(`unsupported selected adapter ${key}`);
      const adapter = adapters[key];
      if (adapter === undefined) throw new Error(`unsupported selected adapter ${key}`);
      return adapter(shared);
    });
  }
}
