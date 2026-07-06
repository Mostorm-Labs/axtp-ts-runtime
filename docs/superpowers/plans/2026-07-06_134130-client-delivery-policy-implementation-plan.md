# AXTP Client Delivery Policy Implementation Plan

> **For Hermes:** Implement this plan task-by-task. Keep the existing safety default (`fail-fast`) intact and preserve backward compatibility for `outbox` and `calls.queue`.

**Spec:** `docs/superpowers/specs/2026-07-06_133214-client-delivery-policy.md`

**Goal:** Add a unified `AxtpClient` `delivery` policy namespace so event outbox behavior, RPC queue options, and default/method-level call policy are configured in one place while `CallOptions` remains the per-call override.

**Architecture:** Add typed delivery policy options to the SDK facade, resolve them through small private helpers in `AxtpClient`, and route existing `emitRaw` / `callRaw` behavior through resolved policies. Do not change wire protocol, endpoint behavior, or automatic replay semantics.

**Tech Stack:** TypeScript, `AxtpClient`, Vitest, pnpm, existing AXTP mock loopback tests.

---

## Current Context

Relevant files:

- `src/sdk/client.ts`
  - owns `ClientOptions`, `ClientOutboxOptions`, `ClientCallsOptions`
  - implements `callRaw()`, `enqueueCall()`, `emitRaw()`, `enqueueEvent()`
- `src/sdk/types.ts`
  - owns public `CallOptions`, `CallOfflinePolicy`, `QueuedCallCoalescePrevious`
- `tests/sdk/sdk.test.ts`
  - already covers legacy outbox, explicit `offlinePolicy`, RPC queue, coalescing, overflow, reconnect exhaustion
- `docs/usage.md`
  - documents current public SDK options and offline behavior
- `src/index.ts`
  - re-exports SDK public surface indirectly; verify no extra export change is needed

Non-goals for this implementation:

- No persistent queue/outbox.
- No automatic method idempotency inference.
- No replay of RPCs already written to an endpoint.
- No stream delivery behavior yet.
- No wildcard method matching in the first pass.
- Do not remove `outbox` or `calls.queue`.

---

## Policy Rules to Preserve

### RPC option precedence

```text
per-call options
> delivery.calls.methods[method]
> delivery.calls.default
> SDK / endpoint defaults
```

### RPC queue option precedence

```text
delivery.calls.queue
> legacy calls.queue
> { maxSize: 1000, overflow: "reject" }
```

### Event delivery precedence

```text
delivery.events
> legacy outbox
> default fail-fast
```

### Safety default

Without `delivery`, current behavior must remain:

- `emit` / `emitRaw` before ready rejects unless legacy `outbox.enabled === true`.
- `call` / `callRaw` before ready rejects unless explicit per-call `offlinePolicy` says otherwise.
- RPC queue capacity defaults to `1000`, overflow defaults to `"reject"`.

---

## Task 1: Add failing tests for `delivery.events`

**Objective:** Prove the new event delivery namespace can replace legacy `outbox` and takes precedence over it.

**Files:**

- Modify: `tests/sdk/sdk.test.ts`

**Step 1: Add test for queueing events via `delivery.events`**

Add near the existing outbox tests:

```ts
it("queues client events emitted before ready when delivery.events offline is queue", async () => {
  let received: unknown;
  const loop = createMockStreamLoopback();
  const server = new AxtpServer(loop.server, {
    logicalRole: "server",
    heartbeatIntervalMs: 60000
  });
  const client = new AxtpClient(loop.client, {
    logicalRole: "client",
    heartbeatIntervalMs: 60000,
    delivery: {
      events: {
        offline: "queue",
        queue: { maxSize: 1000, overflow: "reject" }
      }
    }
  });
  server.onRaw("early", (data) => {
    received = data;
  });

  await server.listen();
  const connected = client.connect();
  const emitted = client.emitRaw("early", { queued: true });

  await connected;
  await emitted;
  await new Promise((r) => setTimeout(r, 30));
  expect(received).toEqual({ queued: true });

  await client.close();
  await server.close();
});
```

**Step 2: Add test for `delivery.events` precedence over legacy `outbox`**

```ts
it("uses delivery.events before legacy outbox options", async () => {
  const loop = createMockStreamLoopback();
  const client = new AxtpClient(loop.client, {
    logicalRole: "client",
    outbox: { enabled: true, maxSize: 1000, overflow: "reject" },
    delivery: {
      events: { offline: "fail-fast" }
    }
  });

  await expect(client.emitRaw("early", { queued: false })).rejects.toMatchObject({
    code: ErrorCode.InvalidState
  });
  await client.close();
});
```

**Step 3: Run targeted tests and verify failure**

```bash
pnpm test tests/sdk/sdk.test.ts
```

Expected before implementation: TypeScript/test compile failure because `delivery` is not in `ClientOptions`.

---

## Task 2: Add failing tests for `delivery.calls.default` and method policy

**Objective:** Prove default and method-level call policy work without per-call `CallOptions`.

**Files:**

- Modify: `tests/sdk/sdk.test.ts`

**Step 1: Add test for `delivery.calls.default.offlinePolicy = "wait-ready"`**

```ts
it("uses delivery.calls.default for wait-ready calls", async () => {
  const loop = createMockStreamLoopback();
  const server = new AxtpServer(loop.server, {
    logicalRole: "server",
    heartbeatIntervalMs: 60000
  });
  const client = new AxtpClient(loop.client, {
    logicalRole: "client",
    heartbeatIntervalMs: 60000,
    delivery: {
      calls: {
        default: { offlinePolicy: "wait-ready", timeoutMs: 5_000 }
      }
    }
  });
  server.handleRaw("add", (_ctx, p) => (p as { a: number }).a + (p as { b: number }).b);

  await server.listen();
  const called = client.callRaw("add", { a: 4, b: 6 });
  await client.connect();
  await expect(called).resolves.toBe(10);

  await client.close();
  await server.close();
});
```

**Step 2: Add test for method policy overriding default**

```ts
it("uses delivery.calls.methods to override call default policy", async () => {
  const loop = createMockStreamLoopback();
  const client = new AxtpClient(loop.client, {
    logicalRole: "client",
    delivery: {
      calls: {
        default: { offlinePolicy: "wait-ready" },
        methods: {
          "dangerous.deleteFile": { offlinePolicy: "fail-fast" }
        }
      }
    }
  });

  await expect(client.callRaw("dangerous.deleteFile", { path: "/tmp/a" })).rejects.toMatchObject({
    code: ErrorCode.InvalidState
  });
  await client.close();
});
```

**Step 3: Add test for per-call override**

```ts
it("uses per-call options before delivery call policy", async () => {
  const loop = createMockStreamLoopback();
  const client = new AxtpClient(loop.client, {
    logicalRole: "client",
    delivery: {
      calls: {
        default: { offlinePolicy: "wait-ready" }
      }
    }
  });

  await expect(
    client.callRaw("device.getInfo", {}, { offlinePolicy: "fail-fast" })
  ).rejects.toMatchObject({ code: ErrorCode.InvalidState });
  await client.close();
});
```

**Step 4: Run targeted tests and verify failure**

```bash
pnpm test tests/sdk/sdk.test.ts
```

Expected before implementation: compile failure because `delivery` types do not exist.

---

## Task 3: Add failing tests for method-level queue/coalesce and queue precedence

**Objective:** Prove method-level queue/coalesce policy and `delivery.calls.queue` precedence.

**Files:**

- Modify: `tests/sdk/sdk.test.ts`

**Step 1: Add method-level coalesce test**

Reuse the existing explicit coalesce test, but move policy into client options:

```ts
it("coalesces queued RPC calls using delivery.calls.methods policy", async () => {
  const loop = createMockStreamLoopback();
  const received: unknown[] = [];
  const server = new AxtpServer(loop.server, {
    logicalRole: "server",
    heartbeatIntervalMs: 60000
  });
  const client = new AxtpClient(loop.client, {
    logicalRole: "client",
    heartbeatIntervalMs: 60000,
    delivery: {
      calls: {
        methods: {
          setName: {
            offlinePolicy: "queue",
            coalesceKey: "setName",
            coalescePrevious: "resolve-with-next"
          }
        }
      }
    }
  });
  server.handleRaw("setName", (_ctx, p) => {
    received.push(p);
    return `applied:${(p as { name: string }).name}`;
  });

  await server.listen();
  const first = client.callRaw("setName", { name: "Room A" });
  const second = client.callRaw("setName", { name: "Room B" });
  const third = client.callRaw("setName", { name: "Room C" });

  await client.connect();

  await expect(first).resolves.toBe("applied:Room C");
  await expect(second).resolves.toBe("applied:Room C");
  await expect(third).resolves.toBe("applied:Room C");
  expect(received).toEqual([{ name: "Room C" }]);

  await client.close();
  await server.close();
});
```

**Step 2: Add call queue precedence test**

```ts
it("uses delivery.calls.queue before legacy calls.queue", async () => {
  const loop = createMockStreamLoopback();
  const client = new AxtpClient(loop.client, {
    logicalRole: "client",
    calls: { queue: { maxSize: 2, overflow: "reject" } },
    delivery: {
      calls: {
        queue: { maxSize: 1, overflow: "reject" }
      }
    }
  });

  const first = client.callRaw("setName", { name: "Room A" }, { offlinePolicy: "queue" });
  await expect(
    client.callRaw("setName", { name: "Room B" }, { offlinePolicy: "queue" })
  ).rejects.toMatchObject({ code: ErrorCode.InvalidState });

  await client.close();
  await expect(first).rejects.toMatchObject({ code: ErrorCode.TransportDisconnected });
});
```

**Step 3: Run targeted tests and verify failure**

```bash
pnpm test tests/sdk/sdk.test.ts
```

Expected before implementation: compile failure because `delivery` types do not exist.

---

## Task 4: Add public delivery option types

**Objective:** Add minimal public types without changing runtime behavior yet.

**Files:**

- Modify: `src/sdk/client.ts`

**Implementation sketch:**

```ts
export interface ClientDeliveryQueueOptions {
  maxSize?: number;
  overflow?: "drop-oldest" | "drop-newest" | "reject";
}

export interface EventDeliveryPolicy {
  offline?: "fail-fast" | "queue";
  queue?: ClientDeliveryQueueOptions;
}

export interface CallDeliveryDefaults {
  timeoutMs?: number;
  offlinePolicy?: CallOfflinePolicy;
}

export interface CallMethodDeliveryPolicy extends CallDeliveryDefaults {
  coalesceKey?: string;
  coalescePrevious?: QueuedCallCoalescePrevious;
}

export interface CallDeliveryPolicy {
  default?: CallDeliveryDefaults;
  queue?: ClientDeliveryQueueOptions;
  methods?: Record<string, CallMethodDeliveryPolicy>;
}

export interface ClientDeliveryOptions {
  events?: EventDeliveryPolicy;
  calls?: CallDeliveryPolicy;
  streams?: unknown;
}
```

Because `CallOfflinePolicy` and `QueuedCallCoalescePrevious` currently live in `src/sdk/types.ts`, update the import at the top of `src/sdk/client.ts` from:

```ts
import type { CallContext, CallOptions, Stream } from "./types.js";
```

to:

```ts
import type {
  CallContext,
  CallOfflinePolicy,
  CallOptions,
  QueuedCallCoalescePrevious,
  Stream
} from "./types.js";
```

Extend `ClientOptions`:

```ts
export interface ClientOptions {
  logicalRole?: LogicalRole;
  defaultTimeoutMs?: number;
  handshakeTimeoutMs?: number;
  heartbeatIntervalMs?: number;
  maxFrameSize?: number;
  reconnect?: ReconnectPolicy;
  /**
   * @deprecated Use delivery.events instead.
   */
  outbox?: ClientOutboxOptions;
  calls?: ClientCallsOptions;
  delivery?: ClientDeliveryOptions;
  diagnostics?: AxtpDiagnostics;
}
```

**Verification:**

```bash
pnpm test tests/sdk/sdk.test.ts
```

Expected after this task: tests compile further but behavior tests still fail because runtime resolution is not implemented.

---

## Task 5: Implement delivery policy resolvers

**Objective:** Centralize policy resolution without changing call/event flow yet.

**Files:**

- Modify: `src/sdk/client.ts`

**Implementation sketch:**

Add near other private helpers:

```ts
type ResolvedEventDeliveryPolicy = {
  offline: "fail-fast" | "queue";
  queue: Required<ClientDeliveryQueueOptions>;
};

private resolveCallOptions(method: string, options?: CallOptions): CallOptions {
  return {
    ...this.options.delivery?.calls?.default,
    ...this.options.delivery?.calls?.methods?.[method],
    ...options
  };
}

private resolveCallQueueOptions(): Required<ClientDeliveryQueueOptions> {
  const queue = this.options.delivery?.calls?.queue ?? this.options.calls?.queue;
  return {
    maxSize: queue?.maxSize ?? 1000,
    overflow: queue?.overflow ?? "reject"
  };
}

private resolveEventDeliveryPolicy(): ResolvedEventDeliveryPolicy {
  const eventPolicy = this.options.delivery?.events;
  const legacyOutbox = this.options.outbox;
  return {
    offline: eventPolicy?.offline ?? (legacyOutbox?.enabled === true ? "queue" : "fail-fast"),
    queue: {
      maxSize: eventPolicy?.queue?.maxSize ?? legacyOutbox?.maxSize ?? 1000,
      overflow: eventPolicy?.queue?.overflow ?? legacyOutbox?.overflow ?? "reject"
    }
  };
}
```

If TypeScript disallows class-private `type` declarations inside the class region, place `ResolvedEventDeliveryPolicy` outside the class near `QueuedCall`.

**Verification:**

```bash
pnpm lint:types
```

Expected: typecheck passes if helpers are syntactically correct.

---

## Task 6: Route `callRaw()` and `enqueueCall()` through resolved delivery policy

**Objective:** Make default/method-level call policy and queue precedence take effect.

**Files:**

- Modify: `src/sdk/client.ts`

**Step 1: Update `callRaw()`**

Replace the current use of `options` in `callRaw()` with `resolvedOptions`:

```ts
async callRaw(method: string, params: unknown, options?: CallOptions): Promise<unknown> {
  const resolvedOptions = this.resolveCallOptions(method, options);

  if (resolvedOptions.offlinePolicy === "wait-ready") {
    const ep = await this.waitForUsable();
    return ep.call(method, params, resolvedOptions.timeoutMs);
  }
  if (resolvedOptions.offlinePolicy === "queue") {
    const ep = this.endpoint;
    if (this.state === "ready" && ep !== undefined)
      return ep.call(method, params, resolvedOptions.timeoutMs);
    return this.enqueueCall(method, params, resolvedOptions);
  }
  const ep = this.requireUsable();
  return ep.call(method, params, resolvedOptions.timeoutMs);
}
```

**Step 2: Update `enqueueCall()` queue source**

Replace:

```ts
const queue = this.options.calls?.queue;
const maxSize = queue?.maxSize ?? 1000;
const overflow = queue?.overflow ?? "reject";
```

with:

```ts
const queue = this.resolveCallQueueOptions();
const maxSize = queue.maxSize;
const overflow = queue.overflow;
```

**Verification:**

```bash
pnpm test tests/sdk/sdk.test.ts
```

Expected: call delivery tests pass; event delivery tests may still fail until Task 7.

---

## Task 7: Route `enqueueEvent()` through resolved delivery policy

**Objective:** Make `delivery.events` replace legacy `outbox` while preserving backward compatibility.

**Files:**

- Modify: `src/sdk/client.ts`

**Implementation sketch:**

Replace the current `enqueueEvent()` policy block:

```ts
const outbox = this.options.outbox;
if (outbox?.enabled !== true) {
  try {
    this.requireUsable();
  } catch (err) {
    return Promise.reject(err);
  }
}
const maxSize = outbox?.maxSize ?? 1000;
const overflow = outbox?.overflow ?? "reject";
```

with:

```ts
const policy = this.resolveEventDeliveryPolicy();
if (policy.offline !== "queue") {
  try {
    this.requireUsable();
  } catch (err) {
    return Promise.reject(err);
  }
}
const maxSize = policy.queue.maxSize;
const overflow = policy.queue.overflow;
```

**Verification:**

```bash
pnpm test tests/sdk/sdk.test.ts
```

Expected: all SDK tests pass.

---

## Task 8: Update usage documentation

**Objective:** Make `delivery` the recommended public API while keeping legacy docs clear.

**Files:**

- Modify: `docs/usage.md`

**Required edits:**

1. In the WebSocket client example, replace the recommended `outbox` snippet with:

```ts
const client = new AxtpClient(new NodeWsClientTransport({ url: "ws://localhost:8080" }), {
  defaultTimeoutMs: 5_000,
  reconnect: { enabled: true, initialDelayMs: 500, maxDelayMs: 5_000, maxAttempts: 5 },
  delivery: {
    events: {
      offline: "queue",
      queue: { maxSize: 1000, overflow: "reject" }
    },
    calls: {
      default: { offlinePolicy: "fail-fast" }
    }
  }
});
```

2. Update `ClientOptions` summary to include `delivery?: ClientDeliveryOptions`.

3. Add type summaries:

```md
`ClientDeliveryOptions`: `{ events?: EventDeliveryPolicy; calls?: CallDeliveryPolicy }`.

`EventDeliveryPolicy`: `{ offline?: "fail-fast" | "queue"; queue?: { maxSize?: number; overflow?: "reject" | "drop-newest" | "drop-oldest" } }`.

`CallDeliveryPolicy`: `{ default?: { timeoutMs?: number; offlinePolicy?: "fail-fast" | "wait-ready" | "queue" }; queue?: { maxSize?: number; overflow?: ... }; methods?: Record<string, CallMethodDeliveryPolicy> }`.
```

4. Mark `outbox` as legacy/deprecated in prose:

```md
`outbox` remains supported as a legacy alias for `delivery.events`, but new code should prefer `delivery.events`.
```

5. Add a default/method-level call policy example:

```ts
const client = new AxtpClient(transport, {
  delivery: {
    calls: {
      default: { offlinePolicy: "wait-ready", timeoutMs: 5_000 },
      methods: {
        "device.factoryReset": { offlinePolicy: "fail-fast" },
        "cast.setAirPlayName": {
          offlinePolicy: "queue",
          coalesceKey: "cast.setAirPlayName",
          coalescePrevious: "resolve-with-next"
        }
      }
    }
  }
});
```

6. Keep the warning that RPC queue/replay is not safe by default and must be declared by users.

**Verification:**

```bash
pnpm format:check docs/usage.md
```

If this repo does not support file-scoped `format:check`, run:

```bash
pnpm format:check
```

---

## Task 9: Full verification

**Objective:** Prove the implementation is type-safe, tested, and buildable.

**Files:**

- No direct edits unless failures reveal issues.

**Commands:**

```bash
pnpm test tests/sdk/sdk.test.ts
pnpm lint:types
pnpm typecheck:tests
pnpm build
```

If `pnpm build` updates generated docs via `devtools/scripts/sync-docs.mjs`, inspect the diff before committing:

```bash
git diff -- docs/ src/ tests/
```

Expected:

- Targeted SDK tests pass.
- Runtime typecheck passes.
- Test typecheck passes.
- Build passes.
- Diff contains only intended source/test/docs changes.

---

## Task 10: Commit implementation

**Objective:** Commit implementation separately from the already committed spec.

**Files likely to stage:**

```bash
git add src/sdk/client.ts src/sdk/types.ts tests/sdk/sdk.test.ts docs/usage.md
```

Only include `src/sdk/types.ts` if actually modified.

**Commit:**

```bash
git commit -m "feat: add client delivery policy options"
```

**Final status check:**

```bash
git status --short --branch
```

Expected:

- Branch: `docs/client-delivery-policy-spec` or a later feature branch if renamed.
- Clean working tree except intentionally untracked local Hermes artifacts.

---

## Review Checklist

- [ ] No behavior change when `delivery` is omitted.
- [ ] `delivery.events` takes precedence over legacy `outbox`.
- [ ] `delivery.calls.queue` takes precedence over legacy `calls.queue`.
- [ ] `delivery.calls.default` applies when no per-call options are passed.
- [ ] `delivery.calls.methods[method]` overrides default policy.
- [ ] Per-call `CallOptions` overrides both method and default policy.
- [ ] RPC queue/coalesce semantics remain unchanged.
- [ ] Documentation explains idempotency / side-effect risks.
- [ ] No wildcard method matching introduced in this pass.
- [ ] Tests and build pass.

---

## Open Questions

1. Should `ClientDeliveryOptions.streams` be omitted entirely for now instead of included as `unknown` reserved field?
   - Recommendation: omit from public type until stream policy has a real spec, unless we explicitly want to reserve the namespace.
2. Should `calls.queue` be marked deprecated immediately?
   - Recommendation: do not deprecate hard yet; say “prefer `delivery.calls.queue` for new code”.
3. Should method policy support wildcard keys later?
   - Recommendation: separate future spec because wildcard ordering/conflict rules need their own tests.
