import { createInterface } from "node:readline";
import { AxtpClient } from "../../dist/index.js";
import { createAxtpDebugServer } from "../../dist/debug.js";
import { NodeWsClientTransport } from "../../dist/node.js";

export function parseJson(text, fallback = {}) {
  if (text.trim() === "") return fallback;
  return JSON.parse(text);
}

export function formatError(error) {
  if (error instanceof Error) {
    return {
      name: error.name,
      code: error.code,
      message: error.message,
      cause: error.cause instanceof Error ? error.cause.message : error.cause
    };
  }
  return { message: String(error) };
}

export async function createPlaygroundClient({ url, reconnect = false, includePayload = true }) {
  const debug = createAxtpDebugServer({
    enabled: true,
    host: "127.0.0.1",
    port: 0,
    includePayload
  });
  await debug.listen();

  const errors = [];
  let client;

  const createClient = () => {
    const next = new AxtpClient(new NodeWsClientTransport({ url }), {
      logicalRole: "client",
      handshakeTimeoutMs: 5_000,
      reconnect: reconnect
        ? {
            enabled: true,
            initialDelayMs: 1_000,
            maxDelayMs: 5_000,
            maxAttempts: Number.POSITIVE_INFINITY
          }
        : { enabled: false },
      diagnostics: debug.diagnostics
    });
    next.onStateChange.subscribe((state) => console.log(`[state] ${state}`));
    next.onError.subscribe((error) => {
      const formatted = formatError(error);
      errors.push(formatted);
      console.error("[error]", formatted);
    });
    client = next;
    return next;
  };

  createClient();

  return {
    debug,
    errors,
    get client() {
      return client;
    },
    async connect(timeoutMs = 10_000) {
      await client.connect(timeoutMs);
    },
    async reconnect(timeoutMs = 10_000) {
      await client.close();
      createClient();
      await client.connect(timeoutMs);
    },
    async close() {
      await client.close();
      await debug.close();
    }
  };
}

export function startInteractiveShell(playground) {
  const rl = createInterface({ input: process.stdin, output: process.stdout, prompt: "axtp> " });
  console.log(
    "Commands: state | errors | call <method> <json> | emit <event> <json> | reconnect | clear | quit"
  );
  rl.prompt();

  rl.on("line", async (line) => {
    try {
      const [command = "", name = "", ...rest] = line.trim().split(/\s+/);
      const json = rest.join(" ");
      switch (command) {
        case "state":
          console.log({
            ready: playground.client.isReady,
            closed: playground.client.isClosed,
            sid: playground.client.sid
          });
          break;
        case "errors":
          console.log(playground.errors);
          break;
        case "call":
          console.log(await playground.client.callRaw(name, parseJson(json)));
          break;
        case "emit":
          await playground.client.emitRaw(name, parseJson(json));
          console.log("sent");
          break;
        case "reconnect":
          await playground.reconnect();
          console.log("reconnected");
          break;
        case "clear":
          playground.debug.collector.clear();
          playground.errors.length = 0;
          console.log("cleared");
          break;
        case "quit":
        case "exit":
          rl.close();
          return;
        case "":
          break;
        default:
          console.log("Unknown command");
      }
    } catch (error) {
      console.error("[command error]", formatError(error));
    }
    rl.prompt();
  });

  return new Promise((resolve) => rl.once("close", resolve));
}
