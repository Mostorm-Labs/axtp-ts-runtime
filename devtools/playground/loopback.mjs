import { AxtpServer } from "../../dist/index.js";
import { NodeWsServerTransport } from "../../dist/node.js";
import { createPlaygroundClient } from "./shared.mjs";

const transport = new NodeWsServerTransport({ host: "127.0.0.1", port: 0 });
const server = new AxtpServer(transport, { logicalRole: "server", heartbeatIntervalMs: 60_000 });
server.handleRaw("playground.add", (_context, params) => {
  const { a, b } = params;
  return { sum: a + b };
});
await server.listen();

const url = `ws://127.0.0.1:${transport.boundPort}`;
let playground;

try {
  playground = await createPlaygroundClient({ url, includePayload: true });
  await playground.connect();
  const result = await playground.client.callRaw("playground.add", { a: 2, b: 3 });
  if (result?.sum !== 5) throw new Error(`unexpected RPC result: ${JSON.stringify(result)}`);
  const events = playground.debug.collector.entries();
  if (!events.some((entry) => entry.event === "handshake.ready")) {
    throw new Error("debug collector did not record handshake.ready");
  }
  if (
    !events.some((entry) => entry.event === "rpc.out.request" && entry.name === "playground.add")
  ) {
    throw new Error("debug collector did not record the RPC request");
  }
  console.log(`PASS real WebSocket loopback: ${JSON.stringify(result)}`);
  console.log(`Telemetry captured ${events.length} entries at ${playground.debug.url}`);
} finally {
  await playground?.close();
  await server.close();
}
