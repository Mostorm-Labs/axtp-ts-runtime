// B2: RpcExchange 静默 catch 改为 onError 上报（可观测性）。
import { afterEach, describe, expect, it } from "vitest";
import { AxtpSession } from "../../src/session/session.js";
import { createMockTransportPair } from "../../src/transport/mock/mockTransport.js";
import { framedBinaryProfile } from "../../src/transport/contract.js";
import { type AxtpError, ErrorCode } from "../../src/types/error.js";

function settle(ms = 20): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

const created: AxtpSession[] = [];
afterEach(() => {
  for (const s of created) s.close();
  created.length = 0;
});

async function makePair(): Promise<{ client: AxtpSession; server: AxtpSession }> {
  const { left, right } = createMockTransportPair(framedBinaryProfile());
  const server = new AxtpSession(() => Promise.resolve(right), {
    physicalRole: "server",
    logicalRole: "server"
  });
  const client = new AxtpSession(() => Promise.resolve(left), {
    physicalRole: "client",
    logicalRole: "client"
  });
  created.push(client, server);
  await Promise.all([
    new Promise<void>((r) => client.onReady.subscribe(() => r())),
    new Promise<void>((r) => server.onReady.subscribe(() => r()))
  ]);
  return { client, server };
}

describe("B2: RpcExchange handler 抛错上报 onError", () => {
  it("method handler 抛错 -> server.onError 上报 + client 收到 RpcExecutionFailed", async () => {
    const { client, server } = await makePair();
    const serverErrors: AxtpError[] = [];
    server.onError.subscribe((e) => serverErrors.push(e));

    server.handle("audio.getAlgorithmConfig", (() => {
      throw new Error("boom");
    }) as never);

    await expect(client.call("audio.getAlgorithmConfig", {})).rejects.toMatchObject({
      code: ErrorCode.RpcExecutionFailed
    });
    await settle(10); // 等 dispatchRequest 的 catch 上报 onError
    expect(serverErrors.some((e) => e.code === ErrorCode.RpcExecutionFailed)).toBe(true);
  });
});
