# `@axtp/ts-sdk` — Usage Quick Reference

> AI-friendly quick reference for the published npm package.
> For the **full AXTP wire protocol** (every method, event, schema, error code, capability, and transport profile), see **[`./protocol.md`](./protocol.md)** in this package.

## What it is

TypeScript runtime / SDK for **AXTP** — a request/response + event + streaming wire protocol that runs over any byte/message stream (TCP framed-binary, WebSocket unframed-JSON, …). Three internal layers, exposed through convenient facades:

- **Core** — framing, codec, handshake, heartbeat.
- **Broker** — method/event routing and handler dispatch.
- **Endpoint** — glues one transport to a Core+Broker, drives the stream pipes, owns lifecycle.
- **Facades** — `AxtpClient` (single connection) and `AxtpServer` (many connections) for typical apps; `AxtpEndpoint` as an advanced building block.

## Install

`@axtp/ts-sdk` is published to a **private Verdaccio registry**. Configure your `.npmrc`:

```ini
@axtp:registry=https://your-verdaccio/
//your-verdaccio/:_authToken=${VERDACCIO_TOKEN}
```

```bash
pnpm add @axtp/ts-sdk
```

## Subpath exports

| Import                   | What you get                                                                                                                                                                                                                                                             |
| ------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `@axtp/ts-sdk`           | **Main entry.** Facades (`AxtpClient`/`AxtpServer`/`AxtpEndpoint`/`Stream`), registries, errors, `EventStream`. Re-exports everything below.                                                                                                                             |
| `@axtp/ts-sdk/node`      | Node transports: TCP framed (`NodeTcpClientTransport`/`NodeTcpServerTransport`) and WebSocket unframed-JSON (`NodeWsClientTransport`/`NodeWsServerTransport`). Browser builds should avoid this entry.                                                                   |
| `@axtp/ts-sdk/transport` | Transport contracts & profile capability model: `StreamTransport`, `StreamClientTransport`, `StreamServerTransport`, `LogicalRole`, `PhysicalRole`, `TransportProfile`, `supportsControl`/`supportsStream`/`keepaliveMode`, `framedBinaryProfile`/`unframedJsonProfile`. |
| `@axtp/ts-sdk/protocol`  | Low-level payload model for advanced users: `PayloadType`, `ControlOpcode`, `RpcOp`, `RpcEncoding`, frame/message types, and payload factories (`helloMsg`, `requestMsg`, `responseMsg`, `eventMsg`, `identifyMsg`).                                                     |
| `@axtp/ts-sdk/mock`      | In-memory loopback for tests: `createMockStreamLoopback()`.                                                                                                                                                                                                              |
| `@axtp/ts-sdk/io`        | Byte helpers: `toBytes`, `bytesToHex`, `hexToBytes`, `concatBytes`, `bytesToText`, type `Bytes`.                                                                                                                                                                         |
| `@axtp/ts-sdk/debug`     | Optional Node-only diagnostics webserver: `createAxtpDebugServer()` and `createAxtpDiagnosticsCollector()`. Use this entry only in debug tooling or controlled environments.                                                                                             |

## Quick start

The typed API (`call`/`handle`/`emit`/`on`) infers params and payloads from the spec registry (`MethodName`/`EventName`). For dynamic or custom names, use the `*Raw` variants (`callRaw`/`handleRaw`/`emitRaw`/`onRaw`).

### Client + Server (in-memory loopback)

The smallest end-to-end example — no network, no I/O. Run it under `vitest` or any ESM runner.

```ts
import { AxtpClient, AxtpServer } from "@axtp/ts-sdk";
import { createMockStreamLoopback } from "@axtp/ts-sdk/mock";

const loop = createMockStreamLoopback();

// --- server side ---
const server = new AxtpServer(loop.server);
server.handle("device.getInfo", (_ctx, _params) => ({ version: "1.0.0" }));
server.on("device.stateChanged", (payload) => console.log("server saw:", payload));
await server.listen();

// --- client side ---
const client = new AxtpClient(loop.client);
client.onDisconnect.subscribe(({ remote }) => console.log("disconnected, remote:", remote));
await client.connect();

const info = await client.call("device.getInfo", {}); // => { version: "1.0.0" }

client.on("device.stateChanged", (p) => console.log("client saw:", p));
await client.emit("device.stateChanged", { online: true });

await client.close();
await server.close();
```

### Node WebSocket (real transport)

Client with auto-reconnect:

```ts
import { AxtpClient } from "@axtp/ts-sdk";
import { NodeWsClientTransport } from "@axtp/ts-sdk/node";

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
await client.connect();
```

Server (broadcasts + targeted send):

```ts
import { AxtpServer } from "@axtp/ts-sdk";
import { NodeWsServerTransport } from "@axtp/ts-sdk/node";

const server = new AxtpServer(new NodeWsServerTransport({ port: 8080 }));
server.handle("device.getInfo", (_ctx) => ({ version: "1.0.0" }));
server.onConnect.subscribe((ep) => console.log("endpoint ready, sid:", ep.sid));

await server.listen();
// Broadcast to all ready endpoints:
// await server.emit("device.stateChanged", { online: true });
// ...or target one endpoint by its server-assigned id:
// await server.emitTo(server.getId(endpoint)!, "device.stateChanged", { online: true });
```

### AXTP Telemetry debug server

Use the optional Node-only debug entry when you want a local web UI for AXTP communication logs without wiring your own logger. The debug server consumes the SDK's structured `diagnostics` events and serves a lightweight telemetry panel with counters, filters, raw JSON expansion, copy/clear controls, and SSE live updates.

```ts
import { AxtpClient } from "@axtp/ts-sdk";
import { createAxtpDebugServer } from "@axtp/ts-sdk/debug";
import { NodeWsClientTransport } from "@axtp/ts-sdk/node";

const debug = createAxtpDebugServer({
  enabled: process.env.AXTP_DEBUG === "1",
  // Default is 127.0.0.1. Use 0.0.0.0 only for controlled LAN debugging.
  host: "127.0.0.1",
  port: 0,
  // Debug mode includes payloads by default. Disable if payloads may contain secrets.
  includePayload: true,
  // Required when NODE_ENV=production unless unsafeAllowNoAuth is explicitly true.
  token: process.env.AXTP_DEBUG_TOKEN
});

const client = new AxtpClient(new NodeWsClientTransport({ url: "ws://localhost:8080" }), {
  diagnostics: debug.diagnostics
});

await debug.listen();
await client.connect();

console.log("AXTP telemetry:", debug.url);
```

You can attach the same diagnostics object on server side:

```ts
const server = new AxtpServer(new NodeWsServerTransport({ port: 8080 }), {
  diagnostics: debug.diagnostics
});
```

Security defaults:

- `enabled` defaults to `false`; opt in explicitly.
- `host` defaults to `127.0.0.1` to avoid accidental LAN exposure.
- `includePayload` defaults to `true` for debug visibility; set `includePayload: false` to hide payload data.
- In `NODE_ENV=production`, a `token` is required unless you explicitly pass `unsafeAllowNoAuth: true`.
- If auth is configured, pass the token in the query string (`?token=...`) or with an `Authorization` bearer header.

### Real WebSocket playground

Build the package first so the playground exercises the same `dist` entry points that consumers import:

```bash
npm run build
```

Connect directly to a running AXTP service (defaults to `ws://127.0.0.1:7020`):

```bash
npm run playground
# or pass another target after --
npm run playground -- ws://127.0.0.1:7020
```

The command starts the diagnostics webserver, prints its Telemetry URL, performs a real `NodeWsClientTransport` connection, and opens a small interactive shell. Available commands are `state`, `errors`, `call <method> <json>`, `emit <event> <json>`, `reconnect`, `clear`, and `quit`. If the initial connection fails, the shell and Telemetry page remain available so the target can be fixed or started before running `reconnect`.

Two deterministic smoke modes are also available:

```bash
# Real Node WebSocket server + client, handshake, RPC, and telemetry
npm run playground:loopback

# Real WebSocket peer advertising AXTP 0.12.0 to verify version-error visibility
npm run playground:incompatible
```

The playground is development tooling under `devtools/playground/`; it is not exported as SDK runtime API.

### Bidirectional streaming

```ts
// Client opens a stream (framed transports only, e.g. TCP):
const { response, stream } = await client.openStream("audio.stream", { deviceId: 1 });
stream.onChunk((data, cursor) => console.log("chunk bytes:", data, "cursor:", cursor));
stream.onClose((reason) => console.log("stream closed:", reason));
// stream.send(new Uint8Array([...]));  // send to peer
// stream.close();

// Server receives the stream:
server.onStream("audio.stream", async (_params, stream) => {
  stream.onChunk((data) => {
    /* handle incoming */
  });
  return { accepted: true }; // becomes the openStream response
});
```

## API reference

### `AxtpClient` (single connection)

Construct with a `StreamClientTransport` (from `@axtp/ts-sdk/node` or `@axtp/ts-sdk/mock`).

| Member        | Signature                                                                                                                      |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| `connect`     | `connect(timeoutMs?: number): Promise<void>` — wait for first handshake-ready                                                  |
| `close`       | `close(): Promise<void>` — close and stop reconnecting                                                                         |
| `call`        | `call<K extends MethodName>(method: K, params: MethodRequest<K>, options?: CallOptions): Promise<MethodResponse<K>>`           |
| `callRaw`     | `callRaw(method: string, params: unknown, options?: CallOptions): Promise<unknown>`                                            |
| `handle`      | `handle<K>(method: K, handler: (ctx: CallContext, params: MethodRequest<K>) => MethodResponse<K> \| Promise<...>): () => void` |
| `handleRaw`   | `handleRaw(method: string, handler: UntypedMethodHandler): () => void`                                                         |
| `emit`        | `emit<K extends EventName>(event: K, payload: EventPayload<K>): Promise<void>`                                                 |
| `emitRaw`     | `emitRaw(event: string, payload: unknown): Promise<void>`                                                                      |
| `on`          | `on<K>(event: K, handler: (payload: EventPayload<K>) => void): () => void`                                                     |
| `onRaw`       | `onRaw(event: string, handler: UntypedEventHandler): () => void`                                                               |
| `openStream`  | `openStream(method: string, params: unknown, options?: CallOptions): Promise<{ streamId; response; stream }>`                  |
| `onStream`    | `onStream(method: string, handler: (params, stream) => unknown \| Promise<unknown>): () => void`                               |
| getters       | `sid`, `isReady`, `isClosed`                                                                                                   |
| event streams | `onStateChange`, `onConnect`, `onDisconnect({remote})`, `onReconnect({attempt})`, `onReconnectFailed`, `onError`               |

`ClientOptions`: `logicalRole?`, `defaultTimeoutMs?`, `handshakeTimeoutMs?`, `heartbeatIntervalMs?`, `maxFrameSize?`, `reconnect?: ReconnectPolicy`, `delivery?: ClientDeliveryOptions`.

`ClientDeliveryOptions`: `{ events?: EventDeliveryPolicy; calls?: CallDeliveryPolicy }`.

`EventDeliveryPolicy`: `{ offline?: "fail-fast" | "queue"; queue?: { maxSize?: number; overflow?: "reject" | "drop-newest" | "drop-oldest" } }`.

`CallDeliveryPolicy`: `{ default?: { timeoutMs?: number; offlinePolicy?: "fail-fast" | "wait-ready" | "queue" }; queue?: { maxSize?: number; overflow?: "reject" | "drop-newest" | "drop-oldest" }; methods?: Record<string, { timeoutMs?: number; offlinePolicy?: "fail-fast" | "wait-ready" | "queue"; coalesceKey?: string; coalescePrevious?: "resolve-with-next" | "reject" | "drop" }> }`.

`ReconnectPolicy`: `{ enabled: boolean; initialDelayMs?; maxDelayMs?; maxAttempts?; multiplier?; jitter? }`.

#### Offline send behavior

By default, client sends are fail-fast: `call`/`emit` require a ready connection and throw/reject while connecting or reconnecting.

Enable the in-memory event outbox when UI or app code may emit before the socket is ready:

```ts
const client = new AxtpClient(transport, {
  reconnect: { enabled: true },
  delivery: {
    events: {
      offline: "queue",
      queue: { maxSize: 1000, overflow: "reject" }
    }
  }
});

const connecting = client.connect();
await client.emitRaw("device.stateChanged", { online: true }); // queued until ready
await connecting;
```

Event delivery queue notes:

- It applies to `emit`/`emitRaw` only.
- It is in-memory only; process restart loses queued events.
- The returned promise resolves when the SDK queues/drops/flushes the event to a ready endpoint, **not** when the peer acknowledges delivery. AXTP events are still fire-and-forget unless your application adds its own ack.
- Overflow strategies:
  - `"reject"` rejects the new event when full.
  - `"drop-newest"` resolves the new event without enqueueing it.
  - `"drop-oldest"` drops the oldest queued event and enqueues the new one.

For RPC calls, automatic replay is intentionally not the default because a disconnected response does not prove the peer did not execute the request. Use one of three explicit policies:

- `"fail-fast"` (default): require a ready endpoint and reject while connecting/reconnecting. Use this for unsafe or non-idempotent methods.
- `"wait-ready"`: wait for the next ready endpoint and then send the call exactly once. Use this for safe/idempotent queries.
- `"queue"`: enqueue a call that has not been written to an endpoint yet, optionally coalescing queued state-setting calls before the next ready endpoint.

For safe/idempotent calls, opt into waiting for readiness before the call is sent. Use `delivery.calls.default` or `delivery.calls.methods` when many calls should share the same policy:

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

const info = await client.callRaw("device.getInfo", {});
```

Single-call `CallOptions` still work and override method/default delivery policy:

```ts
const info = await client.callRaw(
  "device.getInfo",
  {},
  {
    offlinePolicy: "wait-ready",
    timeoutMs: 5_000
  }
);
```

`offlinePolicy: "wait-ready"` waits for the next ready endpoint and then sends the call exactly once. `timeoutMs` applies to the RPC after ready; it does not include the time spent waiting for a connection.

For state-setting RPC calls where only the latest queued value should be applied after reconnect, use `offlinePolicy: "queue"` with a caller-defined `coalesceKey`:

```ts
await client.callRaw(
  "cast.setAirPlayName",
  { displayName: name, apply: "immediate" },
  {
    offlinePolicy: "queue",
    coalesceKey: "cast.setAirPlayName",
    coalescePrevious: "resolve-with-next"
  }
);
```

RPC queue notes:

- Queueing is opt-in per call or per declared delivery call policy; default behavior remains fail-fast.
- Queueing only applies before a call is written to an endpoint. Calls already sent to an endpoint are not replayed automatically after disconnect.
- Queued calls flush FIFO after the client becomes ready. If the client leaves ready while flushing, unsent calls remain queued.
- Calls with the same `coalesceKey` replace the previous queued call. The SDK never infers which method names are safe to coalesce.
- `coalescePrevious` controls the replaced call's promise:
  - `"resolve-with-next"` resolves/rejects previous promises with the newer call result.
  - `"reject"` rejects previous promises with an `AxtpError`.
  - `"drop"` resolves previous promises with `undefined` without sending them.
- `close()` and reconnect exhaustion reject all queued calls, so queued call promises do not hang indefinitely.
- `timeoutMs` applies to the RPC after ready; it does not include offline queue time.

### `AxtpServer` (many connections)

Construct with a `StreamServerTransport`. Handlers registered via `handle`/`on` apply to **every** accepted endpoint.

| Member               | Signature                                                                         |
| -------------------- | --------------------------------------------------------------------------------- |
| `listen`             | `listen(): Promise<void>`                                                         |
| `close`              | `close(): Promise<void>`                                                          |
| `call`               | `call<K>(id: number, method: K, params, options?): Promise<MethodResponse<K>>`    |
| `callRaw`            | `callRaw(id, method: string, params, options?): Promise<unknown>`                 |
| `emit`               | `emit<K>(event: K, payload, filter?: (ep) => boolean): Promise<void>` — broadcast |
| `emitRaw`            | `emitRaw(event: string, payload, filter?): Promise<void>`                         |
| `emitTo`             | `emitTo<K>(id: number, event: K, payload): Promise<void>` — targeted              |
| `emitToRaw`          | `emitToRaw(id, event: string, payload): Promise<void>`                            |
| `handle`/`handleRaw` | register a method handler (global)                                                |
| `on`/`onRaw`         | register an event handler (global)                                                |
| lookups              | `getEndpoint(id)`, `getEndpointBySid(sid)`, `getEndpoints()`, `getId(endpoint)`   |
| event streams        | `onConnect(AxtpEndpoint)`, `onDisconnect(AxtpEndpoint)`, `onError`, `onClose`     |

`ServerOptions`: `logicalRole?`, `defaultTimeoutMs?`, `heartbeatIntervalMs?`, `maxFrameSize?`.

### `AxtpEndpoint` (advanced building block)

One transport + Core + Broker. Use directly when you need full control (custom server wiring, conformance tooling, etc.).

| Member                                              | Notes                                       |
| --------------------------------------------------- | ------------------------------------------- |
| `start()`                                           | wire the transport and begin handshake      |
| `close(remote?, terminate?)`                        | abort pipes and close the transport         |
| `call`/`emit`/`handle`/`on`/`openStream`/`onStream` | same shape as the facades' untyped variants |
| getters                                             | `sid`, `state`, `isReady`, `core`, `broker` |
| event streams                                       | `onReady`, `onClose({remote})`, `onError`   |

### `Stream`

| Member    | Signature                                                    |
| --------- | ------------------------------------------------------------ |
| `onChunk` | `onChunk((data: Bytes, cursor: bigint) => void): () => void` |
| `onClose` | `onClose((reason?: string) => void): () => void`             |
| `send`    | `send(data: Bytes, cursor?: bigint): void`                   |
| `close`   | `close(): void`                                              |
| getters   | `streamId`, `isClosed`, `stats: { chunks, bytes }`           |

### Registries, errors, events

```ts
import {
  METHOD_REGISTRY,
  EVENT_REGISTRY,
  registry,
  computeEventMasks,
  isEventSubscribed,
  AxtpError,
  ErrorCode,
  EventStream,
  connectionClosedError,
  notReadyError
} from "@axtp/ts-sdk";
```

- `METHOD_REGISTRY` / `EVENT_REGISTRY` / `registry` — the single source of truth for method/event ids, names, schemas (generated from the spec; see `./protocol.md`).
- `computeEventMasks(names)` — turn a set of subscribed event names into the wire event-mask; `isEventSubscribed(name)` — membership check.
- `AxtpError` — every failed `call`/`handle` rejects with this; carries `code: ErrorCode` and optional `requestId`. `ErrorCode` is the full spec error enum.
- `EventStream<T>` — the observer primitive used by all `onXxx` members above; consume with `.subscribe(cb)` (returns an unsubscribe fn) and `.close()`.

### Key types (selected)

`CallContext` (passed to method handlers): `{ requestId: number; sid: string; id?: number; emit(event, payload); emitRaw(event, payload) }` — `id` is the server-assigned endpoint id (only when running under `AxtpServer`), letting a handler issue targeted `server.emitTo(id, ...)` / `server.callRaw(id, ...)`.

`CallOptions`: `{ timeoutMs?: number; offlinePolicy?: "fail-fast" | "wait-ready" | "queue"; coalesceKey?: string; coalescePrevious?: "resolve-with-next" | "reject" | "drop" }`.

## See also

- **[`./protocol.md`](./protocol.md)** — the complete AXTP protocol reference (methods, events, schemas, error codes, capabilities, transport profiles). Read this to know what names/ids you can pass to `call`/`emit`/`on`.
