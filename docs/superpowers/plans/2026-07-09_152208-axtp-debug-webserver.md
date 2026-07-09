# AXTP Diagnostics Debug Webserver Implementation Plan

> **For Hermes:** Implement task-by-task with strict TDD. This project stores superpower plans under `docs/superpowers/plans/`.

**Goal:** Add an optional Node-only AXTP debug webserver that consumes existing `AxtpDiagnostics` entries and exposes a lightweight shadcn-style telemetry UI plus JSON/SSE APIs, so business code can inspect AXTP communication without writing custom loggers.

**Architecture:** Keep runtime diagnostics as the core seam. Add a separate `./debug` subpath with a diagnostics collector, ring buffer, stats aggregation, and an HTTP server serving static HTML/CSS/JS and APIs. Default to production-safe isolation: disabled unless explicitly enabled, localhost binding, payload off by default, and production auth requirement.

**Tech Stack:** TypeScript ESM, Node `http`, existing Vitest test stack, no React/build dependency for first UI version. The UI should visually mimic shadcn/ui primitives (cards, tabs, badges, buttons) using static HTML/CSS to avoid adding frontend tooling.

---

## Current Context

- Existing diagnostics source: `src/diagnostics.ts`
- Public main export currently re-exports diagnostics types but no debug utilities.
- `package.json` currently exports `.` / `./node` / `./protocol` / `./transport` / `./mock` / `./io`.
- First version should not add heavyweight UI dependencies.
- Debug server must be optional and Node-only.
- Production isolation is mandatory.

---

## Proposed Public API

```ts
import { createAxtpDebugServer } from "@axtp/ts-sdk/debug";

const debug = createAxtpDebugServer({
  enabled: process.env.AXTP_DEBUG === "1",
  includePayload: false,
  token: process.env.AXTP_DEBUG_TOKEN
});

const client = new AxtpClient({
  // ...
  diagnostics: debug.diagnostics
});

await debug.listen();
console.log(debug.url);
```

Support lower-level collector:

```ts
import { createAxtpDiagnosticsCollector } from "@axtp/ts-sdk/debug";

const collector = createAxtpDiagnosticsCollector({ capacity: 1000 });
```

---

## Files Likely to Change

- Create: `src/debug.ts`
- Create: `tests/debug/debug.test.ts`
- Modify: `package.json`
- Optionally modify: `tests/sdk/exports.test.ts` or add dedicated export test

---

## Task 1: Add diagnostics collector tests

**Objective:** Lock down ring buffer, stats, filtering, clear, and payload redaction behavior before implementation.

**Files:**
- Create: `tests/debug/debug.test.ts`
- Later create: `src/debug.ts`

**Test cases:**

1. `collector stores entries in insertion order and enforces capacity`
2. `collector computes requests responses events and failures stats`
3. `collector filters events by kind`
4. `collector clear removes entries and resets stats`
5. `collector diagnostics logger does not expose payload unless includePayload is true`

**Commands:**

```bash
pnpm vitest run tests/debug/debug.test.ts
```

Expected RED before implementation: import failure for `../../src/debug.js`.

---

## Task 2: Implement collector

**Objective:** Create a minimal collector that satisfies tests and adapts directly to existing `AxtpDiagnostics`.

**Implementation notes:**

- `createAxtpDiagnosticsCollector(options)` returns:
  - `diagnostics: AxtpDiagnostics`
  - `push(entry: AxtpDiagnosticEntry): void`
  - `entries(filter?): AxtpDiagnosticEntry[]`
  - `stats(): AxtpDebugStats`
  - `clear(): void`
- Ring buffer defaults to `capacity: 1000`.
- `includePayload` should be set on the returned `diagnostics`, not retroactively mutate entries.
- `entries({ kind })` should support `all`, `requests`, `responses`, `events`, `errors`, `raw`.
- Failure classification:
  - `level === "warn" || level === "error"`
  - or `status !== undefined && status !== 0`
- Request classification by event name containing `.request`.
- Response classification by event name containing `.response`.
- Event classification by event name containing `.event`.

**Commands:**

```bash
pnpm vitest run tests/debug/debug.test.ts
```

Expected GREEN for collector tests.

---

## Task 3: Add debug server tests

**Objective:** Verify HTTP APIs, disabled mode, localhost default, auth isolation, clear endpoint, and SSE basics.

**Test cases:**

1. `debug server does not listen when enabled is false`
2. `debug server listens on localhost and serves the telemetry page`
3. `GET /api/events returns recent events`
4. `GET /api/stats returns stats`
5. `POST /api/clear clears collector`
6. `production mode requires token unless unsafeAllowNoAuth is true`
7. `token protects API routes when configured`

**Commands:**

```bash
pnpm vitest run tests/debug/debug.test.ts
```

Expected RED until server is implemented.

---

## Task 4: Implement debug HTTP server

**Objective:** Add a small Node `http` server behind `createAxtpDebugServer`.

**Routes:**

| Method | Path | Behavior |
|---|---|---|
| `GET` | `/` | Serve static Telemetry UI |
| `GET` | `/healthz` | Return `{ ok: true }` |
| `GET` | `/api/events` | Return filtered entries |
| `GET` | `/api/stats` | Return stats |
| `POST` | `/api/clear` | Clear collector |
| `GET` | `/api/events/stream` | SSE stream new entries |

**Security/isolation:**

- Default `enabled: false`.
- Default `host: "127.0.0.1"`.
- Default `includePayload: false`.
- If `process.env.NODE_ENV === "production"` and enabled without token and without `unsafeAllowNoAuth`, throw on `listen()`.
- If `token` exists, require either:
  - `Authorization: Bearer <token>`
  - or query param `?token=<token>`

---

## Task 5: Implement lightweight shadcn-style UI

**Objective:** Serve a simple Telemetry page like the provided reference image.

**UI structure:**

```text
AXTP Telemetry
Live runtime communication logger for AXTP.

[ REQUESTS ] [ RESPONSES ] [ EVENTS ] [ FAILURES ]

[ All ] [ Requests ] [ Responses ] [ Events ] [ Errors ] [ Raw ]    [ Pause ] [ Copy ] [ Clear ]

IN   rpc.in.request             REQUEST   17:52:51
     method=cast.start · requestId=12 · sid=abc123
```

**Implementation approach:**

- Inline HTML/CSS/JS string in `src/debug.ts` for first version.
- Use SSE for live updates with a polling fallback if needed later.
- CSS should mimic shadcn style: cards, muted background, rounded tabs, badges, clean typography.
- Each log row click expands JSON details.
- Raw tab shows JSON-oriented rows.

---

## Task 6: Add package export

**Objective:** Make debug server available via `@axtp/ts-sdk/debug` without polluting main runtime import.

**Files:**
- Modify: `package.json`

Add:

```json
"./debug": {
  "types": "./dist/debug.d.ts",
  "default": "./dist/debug.js"
}
```

Do **not** re-export debug server from `src/index.ts` unless explicitly desired later.

---

## Task 7: Verify build and full tests

**Objective:** Prove implementation works through real project tooling.

**Commands:**

```bash
pnpm vitest run tests/debug/debug.test.ts
pnpm test
pnpm build
```

Expected:

- Debug-specific tests pass.
- Full suite passes.
- Build emits `dist/debug.js` and `dist/debug.d.ts`.

---

## Risks / Tradeoffs

- Static HTML avoids adding React/Vite/shadcn dependencies but is not literally shadcn/ui. It should visually follow shadcn style for v1.
- Node `http` server keeps dependencies low, but advanced auth/session handling is intentionally out of scope.
- In-memory ring buffer is enough for debug; persistent logs are intentionally out of scope.
- `status !== 0` assumes `ErrorCode.Success` is `0`; tests should confirm against existing behavior if needed.
- SSE connection cleanup must be tested enough to avoid leaking listeners.

---

## Acceptance Criteria

- Business code can pass `debug.diagnostics` to AXTP runtime and open a local web UI.
- UI shows request/response/event/error counts.
- UI filters logs by tabs.
- UI has Pause, Copy, Clear.
- APIs expose events and stats.
- Production mode refuses unauthenticated debug server by default.
- Payloads remain hidden unless `includePayload: true` is explicitly set.
- `./debug` is exported as a separate subpath.
- Tests and build pass.
