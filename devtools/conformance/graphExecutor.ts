export interface GraphStep {
  id: string;
  role?: string;
  captureAs?: string;
  responseTo?: string;
  triggeredBy?: string;
  expect?: { no_event?: { withinMs?: number }; [key: string]: unknown };
  [key: string]: unknown;
}

export interface GraphContext {
  readonly captures: Map<string, unknown>;
  readonly completed: Set<string>;
  readonly outputs: Map<string, unknown>;
}

function contextValue(path: string, context: GraphContext, facts: Record<string, unknown>): unknown {
  if (Object.hasOwn(facts, path)) return facts[path];
  const factPrefix = Object.keys(facts)
    .filter((key) => path.startsWith(`${key}.`))
    .sort((a, b) => b.length - a.length)[0];
  if (factPrefix !== undefined) return field(facts[factPrefix], path.slice(factPrefix.length + 1));
  const [root, ...rest] = path.split(".");
  const source = context.captures.get(root) ?? context.outputs.get(root) ?? facts[root];
  return rest.length === 0 ? source : field(source, rest.join("."));
}

export function evaluateAssertions(
  assertions: readonly string[],
  context: GraphContext,
  facts: Record<string, unknown> = {}
): void {
  for (const assertion of assertions) {
    let match = assertion.match(/^(\S+)\s+(==|!=)\s+("[^"]*"|\S+)$/);
    if (match !== null) {
      const left = contextValue(match[1], context, facts);
      const token = match[3];
      let right: unknown;
      if (token.startsWith('"')) right = token.slice(1, -1);
      else if (/^0x[0-9a-f]+$/i.test(token)) right = Number.parseInt(token.slice(2), 16);
      else if (/^-?\d+(?:\.\d+)?$/.test(token)) right = Number(token);
      else if (token === "true") right = true;
      else if (token === "false") right = false;
      else if (token === "null") right = null;
      else if (Object.hasOwn(STATUS_CODES, token)) right = STATUS_CODES[token];
      else {
        try { right = contextValue(token, context, facts); }
        catch { right = token; }
        if (right === undefined) right = token;
      }
      const ok = match[2] === "==" ? left === right : left !== right;
      if (!ok) throw new Error(`assertion failed: ${assertion}`);
      continue;
    }
    match = assertion.match(/^(\S+)\s+does not contain\s+(\S+)$/);
    if (match !== null) {
      const object = contextValue(match[1], context, facts);
      if (typeof object === "object" && object !== null && match[2] in object) {
        throw new Error(`assertion failed: ${assertion}`);
      }
      continue;
    }
    match = assertion.match(/^(\S+)\s+contains\s+"([^"]+)"$/);
    if (match !== null) {
      const collection = contextValue(match[1], context, facts);
      if (!Array.isArray(collection) || !collection.includes(match[2])) {
        throw new Error(`assertion failed: ${assertion}`);
      }
      continue;
    }
    throw new Error(`unsupported assertion grammar: ${assertion}`);
  }
}

export interface GraphOptions {
  observeNoEvent?: (step: GraphStep, withinMs: number) => boolean | Promise<boolean>;
}

interface BrokerEventSource {
  addEventListener(event: string, handler: (data: unknown) => void): () => void;
}

/** Observe the broker's real inbound event dispatch path for a bounded quiet window. */
export async function observeBrokerNoEvent(
  broker: BrokerEventSource,
  event: string,
  withinMs: number
): Promise<boolean> {
  let observed = false;
  const remove = broker.addEventListener(event, () => { observed = true; });
  try {
    await new Promise((resolve) => setTimeout(resolve, withinMs));
    return !observed;
  } finally {
    remove();
  }
}

const STATUS_CODES: Record<string, number> = {
  SUCCESS: 0,
  NOT_SUPPORTED: 3,
  OUT_OF_RANGE: 0x0b,
  RPC_METHOD_NOT_FOUND: 0x36,
  HELLO: 0,
  IDENTIFY: 2,
  IDENTIFIED: 3,
  REIDENTIFY: 4,
  EVENT: 6,
  REQUEST: 7,
  REQUEST_RESPONSE: 8
};

function assertExpected(actual: unknown, expected: unknown, path = "expect"): void {
  if (typeof expected !== "object" || expected === null) {
    const normalized = typeof expected === "string" && expected in STATUS_CODES
      ? STATUS_CODES[expected]
      : expected;
    if (actual !== normalized) throw new Error(`${path} mismatch: expected ${String(normalized)}, got ${String(actual)}`);
    return;
  }
  const rule = expected as Record<string, unknown>;
  if (rule.type === "string") {
    if (typeof actual !== "string") throw new Error(`${path} must be a string`);
    if (typeof rule.minLength === "number" && actual.length < rule.minLength) {
      throw new Error(`${path} shorter than ${rule.minLength}`);
    }
    return;
  }
  if (rule.type === "object") {
    if (typeof actual !== "object" || actual === null) throw new Error(`${path} must be an object`);
    return;
  }
  if (typeof actual !== "object" || actual === null) throw new Error(`${path} missing object`);
  for (const [key, value] of Object.entries(rule)) {
    if (key === "no_event") continue;
    assertExpected((actual as Record<string, unknown>)[key], value, `${path}.${key}`);
  }
}

function field(value: unknown, path: string): unknown {
  return path.split(".").reduce<unknown>((current, key) => {
    if (typeof current !== "object" || current === null || !(key in current)) {
      throw new Error(`missing capture field ${path}`);
    }
    return (current as Record<string, unknown>)[key];
  }, value);
}

export function resolveReferences(value: unknown, captures: Map<string, unknown>): unknown {
  if (Array.isArray(value)) return value.map((item) => resolveReferences(item, captures));
  if (typeof value !== "object" || value === null) return value;
  const object = value as Record<string, unknown>;
  if (typeof object.ref === "string" && Object.keys(object).length === 1) {
    const [capture, ...parts] = object.ref.split(".");
    if (!captures.has(capture)) throw new Error(`unknown or forward capture ${capture}`);
    return field(captures.get(capture), parts.join("."));
  }
  return Object.fromEntries(
    Object.entries(object).map(([key, item]) => [key, resolveReferences(item, captures)])
  );
}

function collectReferenceRoots(value: unknown, roots: Set<string>): void {
  if (Array.isArray(value)) {
    for (const item of value) collectReferenceRoots(item, roots);
    return;
  }
  if (typeof value !== "object" || value === null) return;
  const object = value as Record<string, unknown>;
  if (typeof object.ref === "string" && Object.keys(object).length === 1) {
    roots.add(object.ref.split(".")[0]);
    return;
  }
  for (const item of Object.values(object)) collectReferenceRoots(item, roots);
}

export async function executeGraph(
  steps: readonly GraphStep[],
  execute: (step: GraphStep, resolved: GraphStep, context: GraphContext) => unknown | Promise<unknown>,
  options: GraphOptions = {}
): Promise<GraphContext> {
  const names = new Set<string>();
  const stepIds = new Set(steps.map((step) => step.id));
  const captureIndexes = new Map<string, number>();
  steps.forEach((step, index) => {
    if (step.captureAs !== undefined) captureIndexes.set(step.captureAs, index);
  });
  for (const [stepIndex, step] of steps.entries()) {
    for (const name of [step.id, step.captureAs]) {
      if (name === undefined) continue;
      if (names.has(name)) throw new Error(`duplicate graph name ${name}`);
      names.add(name);
    }
    const dependency = step.responseTo ?? step.triggeredBy;
    if (dependency !== undefined && !stepIds.has(dependency)) {
      throw new Error(`unknown dependency ${dependency}`);
    }
    const references = new Set<string>();
    collectReferenceRoots(step, references);
    for (const reference of references) {
      const captureIndex = captureIndexes.get(reference);
      if (captureIndex === undefined || captureIndex >= stepIndex) {
        throw new Error(`unknown or forward capture ${reference}`);
      }
    }
  }
  const context: GraphContext = { captures: new Map(), completed: new Set(), outputs: new Map() };
  for (const step of steps) {
    const dependency = step.responseTo ?? step.triggeredBy;
    if (dependency !== undefined && !context.completed.has(dependency)) {
      throw new Error(`step ${step.id} depends on incomplete step ${dependency}`);
    }
    const resolved = resolveReferences(step, context.captures) as GraphStep;
    if (resolved.expect?.no_event !== undefined) {
      if (options.observeNoEvent === undefined) throw new Error(`step ${step.id} has no event observation source`);
      const quiet = await options.observeNoEvent(step, resolved.expect.no_event.withinMs ?? 10);
      if (!quiet) throw new Error(`step ${step.id} observed unexpected event`);
    }
    const output = await execute(step, resolved, context);
    if (output === false) throw new Error(`step ${step.id} failed`);
    if (resolved.expect !== undefined && resolved.expect.no_event === undefined) {
      assertExpected(output, resolved.expect);
    }
    if (step.captureAs !== undefined) {
      context.captures.set(step.captureAs, output);
    }
    context.outputs.set(step.id, output);
    context.completed.add(step.id);
  }
  return context;
}
