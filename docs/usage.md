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
  reconnect: { enabled: true, initialDelayMs: 500, maxDelayMs: 5_000, maxAttempts: 5 }
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

`ClientOptions`: `logicalRole?`, `defaultTimeoutMs?`, `handshakeTimeoutMs?`, `heartbeatIntervalMs?`, `maxFrameSize?`, `reconnect?: ReconnectPolicy`.

`ReconnectPolicy`: `{ enabled: boolean; initialDelayMs?; maxDelayMs?; maxAttempts?; multiplier?; jitter? }`.

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

`CallOptions`: `{ timeoutMs?: number }`.

## See also

- **[`./protocol.md`](./protocol.md)** — the complete AXTP protocol reference (methods, events, schemas, error codes, capabilities, transport profiles). Read this to know what names/ids you can pass to `call`/`emit`/`on`.
