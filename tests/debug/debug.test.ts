import { afterEach, describe, expect, it } from "vitest";
import type { AxtpDiagnosticEntry } from "../../src/diagnostics.js";
import {
  createAxtpDebugServer,
  createAxtpDiagnosticsCollector
} from "../../src/debug.js";

function entry(overrides: Partial<AxtpDiagnosticEntry>): AxtpDiagnosticEntry {
  return {
    ts: Date.now(),
    level: "debug",
    scope: "core",
    event: "rpc.in.event",
    ...overrides
  };
}

describe("AXTP debug diagnostics collector", () => {
  it("stores entries in insertion order and enforces capacity", () => {
    const collector = createAxtpDiagnosticsCollector({ capacity: 2 });

    collector.push(entry({ event: "rpc.in.request", requestId: 1 }));
    collector.push(entry({ event: "rpc.in.event", name: "first" }));
    collector.push(entry({ event: "rpc.out.response", requestId: 1 }));

    expect(collector.entries().map((item) => item.event)).toEqual([
      "rpc.in.event",
      "rpc.out.response"
    ]);
  });

  it("computes requests responses events and failures stats", () => {
    const collector = createAxtpDiagnosticsCollector();

    collector.push(entry({ event: "rpc.out.request", requestId: 1 }));
    collector.push(entry({ event: "rpc.in.response", requestId: 1, status: 0 }));
    collector.push(entry({ event: "rpc.in.event", name: "cast.flowControlChanged" }));
    collector.push(entry({ level: "warn", event: "broker.event.dispatch", known: false }));
    collector.push(entry({ event: "rpc.in.response", requestId: 2, status: 1 }));

    expect(collector.stats()).toMatchObject({
      total: 5,
      requests: 1,
      responses: 2,
      events: 2,
      failures: 2
    });
  });

  it("filters events by kind", () => {
    const collector = createAxtpDiagnosticsCollector();

    collector.push(entry({ event: "rpc.out.request", requestId: 1 }));
    collector.push(entry({ event: "rpc.in.response", requestId: 1 }));
    collector.push(entry({ event: "rpc.in.event", name: "cast.flowControlChanged" }));
    collector.push(entry({ level: "error", event: "runtime.error" }));

    expect(collector.entries({ kind: "requests" }).map((item) => item.event)).toEqual([
      "rpc.out.request"
    ]);
    expect(collector.entries({ kind: "responses" }).map((item) => item.event)).toEqual([
      "rpc.in.response"
    ]);
    expect(collector.entries({ kind: "events" }).map((item) => item.event)).toEqual([
      "rpc.in.event"
    ]);
    expect(collector.entries({ kind: "errors" }).map((item) => item.event)).toEqual([
      "runtime.error"
    ]);
  });

  it("clear removes entries and resets stats", () => {
    const collector = createAxtpDiagnosticsCollector();

    collector.push(entry({ event: "rpc.out.request" }));
    collector.clear();

    expect(collector.entries()).toEqual([]);
    expect(collector.stats()).toMatchObject({ total: 0, requests: 0, responses: 0, events: 0, failures: 0 });
  });

  it("diagnostics logger includes payload by default for debug visibility and can be disabled", () => {
    const visibleByDefault = createAxtpDiagnosticsCollector();
    const hidden = createAxtpDiagnosticsCollector({ includePayload: false });

    expect(visibleByDefault.diagnostics.includePayload).toBe(true);
    expect(hidden.diagnostics.includePayload).toBe(false);

    visibleByDefault.diagnostics.logger(entry({ event: "rpc.in.request", data: { secret: "shown" } }));
    hidden.diagnostics.logger(entry({ event: "rpc.in.request", data: { secret: "hidden" } }));

    expect(visibleByDefault.entries()[0]?.data).toEqual({ secret: "shown" });
    expect(hidden.entries()[0]?.data).toEqual({ secret: "hidden" });
  });
});

describe("AXTP debug server", () => {
  const servers: Array<{ close: () => Promise<void> }> = [];

  afterEach(async () => {
    while (servers.length > 0) {
      await servers.pop()?.close();
    }
    delete process.env.NODE_ENV;
  });

  async function listen(server: ReturnType<typeof createAxtpDebugServer>) {
    await server.listen();
    servers.push(server);
    return server;
  }

  it("does not listen when enabled is false", async () => {
    const server = createAxtpDebugServer({ enabled: false });

    await server.listen();

    expect(server.url).toBeUndefined();
  });

  it("listens on localhost and serves the telemetry page", async () => {
    const server = await listen(createAxtpDebugServer({ enabled: true, port: 0 }));

    expect(server.url).toMatch(/^http:\/\/127\.0\.0\.1:\d+$/);

    const response = await fetch(`${server.url}/`);
    const html = await response.text();

    expect(response.status).toBe(200);
    expect(html).toContain("AXTP Telemetry");
    expect(html).toContain("Requests");
    expect(html).toContain("Events");
  });

  it("serves events and stats APIs", async () => {
    const server = await listen(createAxtpDebugServer({ enabled: true, port: 0 }));

    server.collector.push(entry({ event: "rpc.out.request", requestId: 1 }));
    server.collector.push(entry({ event: "rpc.in.event", name: "cast.flowControlChanged" }));

    const eventsResponse = await fetch(`${server.url}/api/events?kind=events`);
    const events = (await eventsResponse.json()) as { entries: AxtpDiagnosticEntry[] };
    const statsResponse = await fetch(`${server.url}/api/stats`);
    const stats = (await statsResponse.json()) as { events: number; requests: number };

    expect(events.entries).toHaveLength(1);
    expect(events.entries[0]?.name).toBe("cast.flowControlChanged");
    expect(stats).toMatchObject({ requests: 1, events: 1 });
  });

  it("clear endpoint clears collector", async () => {
    const server = await listen(createAxtpDebugServer({ enabled: true, port: 0 }));
    server.collector.push(entry({ event: "rpc.out.request" }));

    const response = await fetch(`${server.url}/api/clear`, { method: "POST" });

    expect(response.status).toBe(200);
    expect(server.collector.entries()).toEqual([]);
  });

  it("production mode requires token unless explicitly unsafe", async () => {
    process.env.NODE_ENV = "production";
    const server = createAxtpDebugServer({ enabled: true, port: 0 });

    await expect(server.listen()).rejects.toThrow(/token/i);
  });

  it("token protects API routes when configured", async () => {
    const server = await listen(createAxtpDebugServer({ enabled: true, port: 0, token: "secret" }));

    const denied = await fetch(`${server.url}/api/stats`);
    const allowed = await fetch(`${server.url}/api/stats`, {
      headers: { Authorization: "Bearer secret" }
    });

    expect(denied.status).toBe(401);
    expect(allowed.status).toBe(200);
  });
});
