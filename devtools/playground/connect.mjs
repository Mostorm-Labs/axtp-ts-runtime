import { createPlaygroundClient, formatError, startInteractiveShell } from "./shared.mjs";

const url = process.argv[2] ?? "ws://127.0.0.1:7020";
const playground = await createPlaygroundClient({ url, reconnect: false, includePayload: true });
let closing = false;

async function close() {
  if (closing) return;
  closing = true;
  await playground.close();
}

process.once("SIGINT", () => {
  void close().finally(() => process.exit(130));
});
process.once("SIGTERM", () => {
  void close().finally(() => process.exit(143));
});

console.log(`AXTP target: ${url}`);
console.log(`Telemetry:   ${playground.debug.url}`);

try {
  await playground.connect();
  console.log(`Connected:   sid=${playground.client.sid}`);
} catch (error) {
  console.error("Initial connection failed:", formatError(error));
  console.log("The shell remains available; start/fix the service and run `reconnect`.");
}

await startInteractiveShell(playground);
await close();
