// UnframedWireAdapter：unframed-json（WebSocket）codec。每条 message 即一个 JSON envelope。
// 仅 RPC（无 CONTROL/STREAM）。移植自 UnframedJsonLink 的 codec 部分。

import { describe, expect, it } from "vitest";
import { concatBytes } from "../../../src/io/bytes.js";
import { requestMsg, type RpcMessage } from "../../../src/protocol/model.js";
import { UnframedWireAdapter } from "../../../src/core/wire/unframed.js";
import type { WireSink } from "../../../src/core/wire/adapter.js";

function capture() {
  const rpc: RpcMessage[] = [];
  let errs = 0;
  const sink: WireSink = {
    onControl: () => {},
    onRpc: (m) => rpc.push(m),
    onStream: () => {},
    onError: () => (errs += 1)
  };
  return {
    sink,
    rpc,
    get errs() {
      return errs;
    }
  };
}

describe("UnframedWireAdapter", () => {
  it("encodeRpc → feedBytes 回环：解出原 Request", () => {
    const a = new UnframedWireAdapter();
    const msg = requestMsg("12345678", 3, "network.getIp", { iface: "eth0" });
    const c = capture();
    a.feedBytes(concatBytes(a.encodeRpc(msg)), c.sink);
    expect(c.rpc).toHaveLength(1);
    expect(c.rpc[0]).toMatchObject({ method: msg.method, requestId: 3 });
  });

  it("malformed JSON → onError", () => {
    const a = new UnframedWireAdapter();
    const c = capture();
    a.feedBytes(new TextEncoder().encode("not json"), c.sink);
    expect(c.errs).toBe(1);
  });
});
