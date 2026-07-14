import { WebSocketServer } from "ws";
import { ErrorCode } from "../../dist/index.js";
import { RpcOp } from "../../dist/protocol.js";
import { createPlaygroundClient } from "./shared.mjs";

const peer = new WebSocketServer({ host: "127.0.0.1", port: 0 });
await new Promise((resolve, reject) => {
  peer.once("listening", resolve);
  peer.once("error", reject);
});
const address = peer.address();
if (typeof address !== "object" || address === null) throw new Error("missing peer address");

peer.on("connection", (socket) => {
  socket.send(JSON.stringify({ sid: "", op: RpcOp.Hello, d: { axtpVersion: "0.12.0" } }));
});

let playground;

try {
  playground = await createPlaygroundClient({
    url: `ws://127.0.0.1:${address.port}`,
    includePayload: true
  });
  let caught;
  try {
    await playground.connect(2_000);
  } catch (error) {
    caught = error;
  }
  if (caught?.code !== ErrorCode.RpcPayloadInvalid || !caught.message.includes("0.12.0")) {
    throw new Error(`unexpected connection result: ${caught?.stack ?? String(caught)}`);
  }
  const diagnostic = playground.debug.collector
    .entries({ kind: "errors" })
    .find((entry) => entry.event === "handshake.error");
  if (diagnostic?.message !== "unsupported or missing axtpVersion: 0.12.0") {
    throw new Error(`missing structured handshake diagnostic: ${JSON.stringify(diagnostic)}`);
  }
  console.log(`PASS incompatible peer surfaced: ${caught.message}`);
  console.log(`Diagnostic: ${JSON.stringify(diagnostic)}`);
} finally {
  await playground?.close();
  for (const socket of peer.clients) socket.terminate();
  await new Promise((resolve) => peer.close(resolve));
}
