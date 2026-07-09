// Optional Node-only AXTP diagnostics debug server.
// Import from @axtp/ts-sdk/debug so core runtime users do not pull in HTTP server code.

import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";
import type { AxtpDiagnosticEntry, AxtpDiagnostics } from "./diagnostics.js";

export type AxtpDebugEventKind = "all" | "requests" | "responses" | "events" | "errors" | "raw";

export interface AxtpDebugStats {
  readonly total: number;
  readonly requests: number;
  readonly responses: number;
  readonly events: number;
  readonly failures: number;
  readonly sessions: number;
}

export interface AxtpDebugEventsFilter {
  readonly kind?: AxtpDebugEventKind;
}

export interface AxtpDiagnosticsCollectorOptions {
  /** Maximum number of entries retained in memory. Defaults to 1000. */
  readonly capacity?: number;
  /** Forwarded to AxtpDiagnostics. Payload capture still happens at runtime via diagnosticData(). */
  readonly includePayload?: boolean;
  /** Minimum diagnostics level. Defaults to debug. */
  readonly level?: AxtpDiagnostics["level"];
}

export interface AxtpDiagnosticsCollector {
  readonly diagnostics: AxtpDiagnostics;
  push(entry: AxtpDiagnosticEntry): void;
  entries(filter?: AxtpDebugEventsFilter): AxtpDiagnosticEntry[];
  stats(): AxtpDebugStats;
  clear(): void;
  subscribe(listener: (entry: AxtpDiagnosticEntry) => void): () => void;
}

export interface AxtpDebugServerOptions extends AxtpDiagnosticsCollectorOptions {
  /** Explicit opt-in. Defaults to false. */
  readonly enabled?: boolean;
  /** Defaults to 127.0.0.1 to avoid accidental LAN exposure. */
  readonly host?: string;
  /** Defaults to 0 when enabled, letting the OS choose an available port. */
  readonly port?: number;
  /** Bearer/query token protecting APIs and SSE. Required in production unless unsafeAllowNoAuth is true. */
  readonly token?: string;
  /** Escape hatch for controlled production environments. Prefer token instead. */
  readonly unsafeAllowNoAuth?: boolean;
  readonly collector?: AxtpDiagnosticsCollector;
}

export interface AxtpDebugServer {
  readonly diagnostics: AxtpDiagnostics;
  readonly collector: AxtpDiagnosticsCollector;
  readonly url: string | undefined;
  listen(): Promise<void>;
  close(): Promise<void>;
}

const DEFAULT_CAPACITY = 1000;
const SUCCESS_STATUS = 0;

export function createAxtpDiagnosticsCollector(
  options: AxtpDiagnosticsCollectorOptions = {}
): AxtpDiagnosticsCollector {
  const capacity = Math.max(1, options.capacity ?? DEFAULT_CAPACITY);
  const buffer: AxtpDiagnosticEntry[] = [];
  const listeners = new Set<(entry: AxtpDiagnosticEntry) => void>();

  const collector: AxtpDiagnosticsCollector = {
    diagnostics: {
      enabled: true,
      level: options.level ?? "debug",
      includePayload: options.includePayload !== false,
      logger: (entry) => collector.push(entry)
    },
    push(entry) {
      buffer.push(entry);
      while (buffer.length > capacity) buffer.shift();
      for (const listener of listeners) listener(entry);
    },
    entries(filter = {}) {
      const kind = filter.kind ?? "all";
      if (kind === "all" || kind === "raw") return [...buffer];
      return buffer.filter((item) => entryKindMatches(item, kind));
    },
    stats() {
      const sessions = new Set<string>();
      let requests = 0;
      let responses = 0;
      let events = 0;
      let failures = 0;

      for (const item of buffer) {
        if (item.sid !== undefined) sessions.add(item.sid);
        if (isRequest(item)) requests += 1;
        if (isResponse(item)) responses += 1;
        if (isEvent(item)) events += 1;
        if (isFailure(item)) failures += 1;
      }

      return { total: buffer.length, requests, responses, events, failures, sessions: sessions.size };
    },
    clear() {
      buffer.length = 0;
    },
    subscribe(listener) {
      listeners.add(listener);
      return () => listeners.delete(listener);
    }
  };

  return collector;
}

export function createAxtpDebugServer(options: AxtpDebugServerOptions = {}): AxtpDebugServer {
  const enabled = options.enabled === true;
  const host = options.host ?? "127.0.0.1";
  const port = options.port ?? 0;
  const collector = options.collector ?? createAxtpDiagnosticsCollector(options);
  const token = options.token;
  let server: Server | undefined;
  let urlValue: string | undefined;

  return {
    diagnostics: collector.diagnostics,
    collector,
    get url() {
      return urlValue;
    },
    async listen() {
      if (!enabled) return;
      if (server !== undefined) return;
      if (process.env.NODE_ENV === "production" && token === undefined && options.unsafeAllowNoAuth !== true) {
        throw new Error("AXTP debug server requires token in production");
      }

      server = createServer((req, res) => {
        void handleRequest(req, res, collector, token);
      });

      await new Promise<void>((resolve, reject) => {
        const current = server;
        if (current === undefined) return reject(new Error("server was not created"));
        current.once("error", reject);
        current.listen(port, host, () => {
          current.off("error", reject);
          const address = current.address() as AddressInfo;
          urlValue = `http://${host}:${address.port}`;
          resolve();
        });
      });
    },
    async close() {
      const current = server;
      server = undefined;
      urlValue = undefined;
      if (current === undefined) return;
      await new Promise<void>((resolve, reject) => {
        current.close((err) => (err === undefined ? resolve() : reject(err)));
      });
    }
  };
}

function entryKindMatches(entry: AxtpDiagnosticEntry, kind: AxtpDebugEventKind): boolean {
  switch (kind) {
    case "requests":
      return isRequest(entry);
    case "responses":
      return isResponse(entry);
    case "events":
      return isEvent(entry) && !isFailure(entry);
    case "errors":
      return isFailure(entry);
    case "all":
    case "raw":
      return true;
  }
}

function isRequest(entry: AxtpDiagnosticEntry): boolean {
  return entry.event.includes(".request");
}

function isResponse(entry: AxtpDiagnosticEntry): boolean {
  return entry.event.includes(".response");
}

function isEvent(entry: AxtpDiagnosticEntry): boolean {
  return entry.event.includes(".event");
}

function isFailure(entry: AxtpDiagnosticEntry): boolean {
  return entry.level === "warn" || entry.level === "error" || (entry.status !== undefined && entry.status !== SUCCESS_STATUS);
}

async function handleRequest(
  req: IncomingMessage,
  res: ServerResponse,
  collector: AxtpDiagnosticsCollector,
  token: string | undefined
): Promise<void> {
  const url = new URL(req.url ?? "/", "http://127.0.0.1");

  if (url.pathname.startsWith("/api/") && !isAuthorized(req, url, token)) {
    writeJson(res, 401, { error: "unauthorized" });
    return;
  }

  if (req.method === "GET" && url.pathname === "/") {
    writeText(res, 200, telemetryHtml(token), "text/html; charset=utf-8");
    return;
  }

  if (req.method === "GET" && url.pathname === "/healthz") {
    writeJson(res, 200, { ok: true });
    return;
  }

  if (req.method === "GET" && url.pathname === "/api/events") {
    const kind = parseKind(url.searchParams.get("kind"));
    writeJson(res, 200, { entries: collector.entries({ kind }) });
    return;
  }

  if (req.method === "GET" && url.pathname === "/api/stats") {
    writeJson(res, 200, collector.stats());
    return;
  }

  if (req.method === "POST" && url.pathname === "/api/clear") {
    collector.clear();
    writeJson(res, 200, { ok: true });
    return;
  }

  if (req.method === "GET" && url.pathname === "/api/events/stream") {
    writeSse(req, res, collector);
    return;
  }

  writeJson(res, 404, { error: "not found" });
}

function parseKind(value: string | null): AxtpDebugEventKind {
  switch (value) {
    case "requests":
    case "responses":
    case "events":
    case "errors":
    case "raw":
      return value;
    default:
      return "all";
  }
}

function isAuthorized(req: IncomingMessage, url: URL, token: string | undefined): boolean {
  if (token === undefined) return true;
  if (url.searchParams.get("token") === token) return true;
  return req.headers.authorization === `Bearer ${token}`;
}

function writeJson(res: ServerResponse, status: number, value: unknown): void {
  const body = JSON.stringify(value);
  res.writeHead(status, {
    "content-type": "application/json; charset=utf-8",
    "content-length": Buffer.byteLength(body)
  });
  res.end(body);
}

function writeText(res: ServerResponse, status: number, body: string, contentType: string): void {
  res.writeHead(status, {
    "content-type": contentType,
    "content-length": Buffer.byteLength(body)
  });
  res.end(body);
}

function writeSse(
  req: IncomingMessage,
  res: ServerResponse,
  collector: AxtpDiagnosticsCollector
): void {
  res.writeHead(200, {
    "content-type": "text/event-stream; charset=utf-8",
    "cache-control": "no-cache, no-transform",
    connection: "keep-alive"
  });
  res.write(`event: snapshot\ndata: ${JSON.stringify({ entries: collector.entries(), stats: collector.stats() })}\n\n`);
  const unsubscribe = collector.subscribe((entry) => {
    res.write(`event: entry\ndata: ${JSON.stringify(entry)}\n\n`);
  });
  req.on("close", unsubscribe);
}

function telemetryHtml(token: string | undefined): string {
  const tokenQuery = token === undefined ? "" : `?token=${encodeURIComponent(token)}`;
  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>AXTP Telemetry</title>
  <style>
    :root { color-scheme: light; --bg: #f8fafc; --card: #ffffff; --muted: #64748b; --border: #e2e8f0; --text: #0f172a; --active: #0f172a; --danger: #dc2626; --warn: #d97706; }
    * { box-sizing: border-box; }
    body { margin: 0; background: var(--bg); color: var(--text); font: 14px/1.45 ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
    main { max-width: 1100px; margin: 0 auto; padding: 28px; }
    header { display: flex; align-items: flex-start; justify-content: space-between; gap: 16px; margin-bottom: 18px; }
    h1 { margin: 0; font-size: 28px; letter-spacing: -0.03em; }
    p { margin: 4px 0 0; color: var(--muted); }
    .actions, .tabs, .stats { display: flex; gap: 8px; flex-wrap: wrap; }
    button { border: 1px solid var(--border); background: var(--card); color: var(--text); border-radius: 9px; padding: 8px 11px; cursor: pointer; }
    button.active { background: var(--active); color: white; border-color: var(--active); }
    .stats { display: grid; grid-template-columns: repeat(4, minmax(120px, 1fr)); margin: 18px 0; }
    .stat { border: 1px solid var(--border); background: #f1f5f9; border-radius: 14px; padding: 14px; }
    .stat-label { color: var(--muted); font-size: 11px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.08em; }
    .stat-value { font-size: 28px; font-weight: 750; margin-top: 4px; }
    .toolbar { display: flex; align-items: center; justify-content: space-between; gap: 12px; margin: 16px 0; }
    .list { display: grid; gap: 8px; }
    .row { border: 1px solid var(--border); background: var(--card); border-radius: 14px; padding: 12px; cursor: pointer; }
    .row.warn { border-color: #fed7aa; background: #fff7ed; }
    .row.error { border-color: #fecaca; background: #fef2f2; }
    .line { display: grid; grid-template-columns: auto 1fr auto; gap: 12px; align-items: start; }
    .badge { display: inline-flex; align-items: center; justify-content: center; border-radius: 999px; padding: 3px 8px; font-size: 11px; font-weight: 800; background: #e2e8f0; color: #334155; min-width: 36px; }
    .badge.out { background: #dbeafe; color: #1d4ed8; }
    .badge.err { background: #fee2e2; color: #b91c1c; }
    .title { font-weight: 700; }
    .meta { color: var(--muted); font-size: 12px; margin-top: 2px; }
    .right { text-align: right; color: var(--muted); font-size: 12px; text-transform: uppercase; font-weight: 700; }
    pre { display: none; margin: 10px 0 0 48px; padding: 12px; border-radius: 10px; overflow: auto; background: #0f172a; color: #e2e8f0; font-size: 12px; }
    .row.open pre { display: block; }
    .empty { border: 1px dashed var(--border); border-radius: 14px; padding: 28px; text-align: center; color: var(--muted); background: rgba(255,255,255,.6); }
  </style>
</head>
<body>
<main>
  <header>
    <div><h1>AXTP Telemetry</h1><p>Live runtime communication logger for AXTP.</p></div>
    <div class="actions"><button id="pause">Pause</button><button id="copy">Copy</button><button id="clear">Clear</button></div>
  </header>
  <section class="stats">
    <div class="stat"><div class="stat-label">Requests</div><div id="requests" class="stat-value">0</div></div>
    <div class="stat"><div class="stat-label">Responses</div><div id="responses" class="stat-value">0</div></div>
    <div class="stat"><div class="stat-label">Events</div><div id="events" class="stat-value">0</div></div>
    <div class="stat"><div class="stat-label">Failures</div><div id="failures" class="stat-value">0</div></div>
  </section>
  <section class="toolbar">
    <div class="tabs" id="tabs"></div>
  </section>
  <section class="list" id="list"><div class="empty">Waiting for AXTP diagnostics...</div></section>
</main>
<script>
const tokenQuery = ${JSON.stringify(tokenQuery)};
const kinds = ["all", "requests", "responses", "events", "errors", "raw"];
let currentKind = "all";
let paused = false;
let entries = [];
const tabs = document.getElementById("tabs");
for (const kind of kinds) {
  const button = document.createElement("button");
  button.textContent = kind[0].toUpperCase() + kind.slice(1);
  button.onclick = () => { currentKind = kind; updateTabs(); load(); };
  button.dataset.kind = kind;
  tabs.appendChild(button);
}
function updateTabs(){ for (const b of tabs.children) b.classList.toggle("active", b.dataset.kind === currentKind); }
function entryType(e){ if(e.level === "warn" || e.level === "error" || (e.status !== undefined && e.status !== 0)) return "ERROR"; if(e.event.includes(".request")) return "REQUEST"; if(e.event.includes(".response")) return "RESPONSE"; if(e.event.includes(".event")) return "EVENT"; if(e.event.includes("stream")) return "STREAM"; return "RAW"; }
function summary(e){ return [e.name, e.requestId !== undefined ? "requestId="+e.requestId : "", e.sid ? "sid="+e.sid : "", e.status !== undefined ? "status="+e.status : "", e.bytes !== undefined ? "bytes="+e.bytes : "", e.scope ? "scope="+e.scope : ""].filter(Boolean).join(" · "); }
function time(e){ return new Date(e.ts).toLocaleTimeString(); }
function render(){
  const list = document.getElementById("list");
  if(entries.length === 0){ list.innerHTML = '<div class="empty">Waiting for AXTP diagnostics...</div>'; return; }
  list.innerHTML = "";
  for(const e of entries.slice().reverse()){
    const type = entryType(e);
    const row = document.createElement("div");
    row.className = "row" + (type === "ERROR" ? " error" : e.level === "warn" ? " warn" : "");
    const dir = e.direction ? e.direction.toUpperCase() : type === "ERROR" ? "ERR" : "LOG";
    row.innerHTML = '<div class="line"><span class="badge '+(dir === "OUT" ? "out" : type === "ERROR" ? "err" : "")+'">'+dir+'</span><div><div class="title">'+e.event+'</div><div class="meta">'+(summary(e) || "diagnostic entry")+'</div></div><div class="right"><div>'+type+'</div><div>'+time(e)+'</div></div></div><pre>'+JSON.stringify(e, null, 2)+'</pre>';
    row.onclick = () => row.classList.toggle("open");
    list.appendChild(row);
  }
}
async function load(){
  const [eventsRes, statsRes] = await Promise.all([fetch('/api/events?kind='+currentKind+tokenQuery.replace(/^\\?/, '&')), fetch('/api/stats'+tokenQuery)]);
  entries = (await eventsRes.json()).entries;
  const stats = await statsRes.json();
  for (const key of ["requests", "responses", "events", "failures"]) document.getElementById(key).textContent = stats[key] ?? 0;
  render();
}
document.getElementById("pause").onclick = (event) => { paused = !paused; event.currentTarget.textContent = paused ? "Resume" : "Pause"; };
document.getElementById("copy").onclick = () => navigator.clipboard?.writeText(JSON.stringify(entries, null, 2));
document.getElementById("clear").onclick = async () => { await fetch('/api/clear'+tokenQuery, { method: 'POST' }); await load(); };
updateTabs(); load();
const es = new EventSource('/api/events/stream'+tokenQuery);
es.addEventListener('snapshot', () => { if(!paused) load(); });
es.addEventListener('entry', () => { if(!paused) load(); });
</script>
</body>
</html>`;
}
