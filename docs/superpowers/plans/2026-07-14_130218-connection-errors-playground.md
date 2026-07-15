# AXTP Connection Errors and Real Playground Implementation Plan

> **For Hermes:** Implement task-by-task with strict TDD. This repository stores plans under `docs/superpowers/plans/`.

**Goal:** Make fatal AXTP handshake failures observable and actionable through the SDK and debug webserver, and add a real-network playground that can test an external AXTP service without launching Launcher.

**Architecture:** Preserve `AxtpClient.onError` as the application-facing error channel and enrich diagnostics with structured failure fields. Treat fatal handshake validation errors as terminal for the current endpoint, preserve the root cause through `connect()`, and retry only transport/disconnect failures. Add Node-only scripts under `devtools/playground/` that import built `dist` exports and exercise real WebSocket connections, the debug webserver, a real loopback server, and an intentionally incompatible peer.

**Tech Stack:** TypeScript ESM, Node.js, Web Streams, `ws`, Vitest, Node `http` debug server, built `dist` package exports.

---

## Base and PR topology

- Repository: `Mostorm-Labs/axtp-ts-runtime`
- Base: latest `origin/main` at branch creation
- Head: `feat/connection-errors-playground`
- Delivery: one PR with `base=main`; do not base it on historical diagnostics/debug branches.

## Task 1: Lock down fatal handshake error propagation

**Files:**

- Modify: `tests/sdk/sdk.test.ts`
- Modify: `tests/transport/ws.test.ts` or create a focused real-WS test file if clearer
- Modify: `src/sdk/client.ts`
- Modify: `src/endpoint/endpoint.ts`

**Steps:**

1. Add a failing test where a real or deterministic peer sends an incompatible `Hello`.
2. Assert `client.onError` receives the original `AxtpError`.
3. Assert `client.connect()` rejects with the same code/message rather than timing out or returning reconnect exhaustion.
4. Assert the failed endpoint closes and does not remain indefinitely in `connecting`.
5. Implement the smallest lifecycle changes to pass.

## Task 2: Add structured error diagnostics

**Files:**

- Modify: `src/diagnostics.ts`
- Modify: `src/core/core.ts`
- Modify: `src/endpoint/endpoint.ts` and/or `src/sdk/client.ts`
- Modify: `tests/sdk/sdk.test.ts`
- Modify: `tests/debug/debug.test.ts`

**Steps:**

1. Add a failing test for a `handshake.error` diagnostic entry.
2. Define stable optional fields such as `code`, `message`, `phase`, and `retryable` on `AxtpDiagnosticEntry`.
3. Emit an error-level diagnostic at the boundary where the handshake validation result becomes a runtime error.
4. Verify the debug collector classifies it under failures/errors without requiring payload capture.

## Task 3: Define retry and handshake timeout behavior

**Files:**

- Modify: `src/sdk/client.ts`
- Modify: `src/endpoint/reconnect.ts` only if coordinator API changes are necessary
- Modify: `tests/sdk/sdk.test.ts`

**Steps:**

1. Add a failing test proving deterministic protocol/handshake errors are not retried.
2. Add a failing test proving `ClientOptions.handshakeTimeoutMs` bounds each established transport's handshake.
3. Preserve retry for transport connection/disconnection failures.
4. Preserve the last/root error if reconnect attempts are exhausted.
5. Implement and run focused tests after each vertical slice.

## Task 4: Add the external-service playground

**Files:**

- Create: `devtools/playground/connect.mjs`
- Create supporting shared module(s) under `devtools/playground/` only when duplication justifies it
- Modify: `package.json`
- Modify: `docs/usage.md`

**Behavior:**

- Default target: `ws://127.0.0.1:7020`.
- Start `createAxtpDebugServer()` on localhost and print its URL.
- Construct real `NodeWsClientTransport` + `AxtpClient` from built `dist` exports.
- Print state transitions and full structured errors.
- Support interactive `state`, `errors`, `call <method> <json>`, `emit <event> <json>`, `reconnect`, `clear`, and `quit` commands.
- Handle `SIGINT` and close client/debug server cleanly.

## Task 5: Add deterministic real-network playground modes

**Files:**

- Create: `devtools/playground/loopback.mjs`
- Create: `devtools/playground/incompatible-peer.mjs`
- Modify: `package.json`
- Modify: `docs/usage.md`

**Behavior:**

- `loopback`: real `NodeWsServerTransport` and `NodeWsClientTransport`, completed handshake, real RPC call, debug telemetry.
- `incompatible-peer`: real `ws` server sends an incompatible `Hello` and proves the SDK exposes the exact failure.
- Scripts import `dist`, so `pnpm build` is a prerequisite and published-package shape is exercised.

## Task 6: Verification and PR

**Commands:**

```bash
pnpm vitest run <focused tests>
pnpm test
pnpm lint:types
pnpm typecheck:tests
pnpm build
pnpm playground:loopback
pnpm playground:incompatible
```

For the external mode, run a bounded smoke against `ws://127.0.0.1:7020`; report whether the service is reachable and the actual handshake result without claiming external-service success if it is absent.

Then:

1. Review `git diff --check` and the complete diff against `origin/main`.
2. Commit with Conventional Commits.
3. Push `feat/connection-errors-playground`.
4. Create one GitHub PR explicitly using `--base main`.
5. Read the created PR back and verify base is `main` and head is `feat/connection-errors-playground`.

## Acceptance criteria

- A version mismatch is visible through `AxtpClient.onError`, `connect()` rejection, diagnostics, and debugWebServer.
- Fatal handshake failures do not hang indefinitely or waste retries intended for transient failures.
- `handshakeTimeoutMs` is effective.
- One command can connect to `127.0.0.1:7020` with real WebSocket transport and telemetry.
- Deterministic real-WS loopback and incompatible-version modes are runnable from the repository.
- Full tests, type checks, build, and playground smoke checks pass or blockers are reported exactly.
- Exactly one PR is opened with base `main`.
