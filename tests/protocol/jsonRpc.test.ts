import { describe, expect, it } from "vitest";
import { decodeJsonRpc, encodeJsonRpc } from "../../src/protocol/codec/jsonRpc.js";
import { responseMsg, RpcOp, type ResponsePayload } from "../../src/protocol/model.js";
import { ErrorCode } from "../../src/types/error.js";

describe("JSON RPC response status", () => {
  it("encodes RequestResponse status as an object with ok and code", () => {
    const encoded = encodeJsonRpc(responseMsg("12345678", 1, ErrorCode.Success, {}));
    const wire = JSON.parse(new TextDecoder().decode(encoded)) as {
      d: { status: unknown };
    };

    expect(wire.d.status).toEqual({ ok: true, code: ErrorCode.Success });
  });

  it("decodes object status to the internal ErrorCode number", () => {
    const decoded = decodeJsonRpc(
      JSON.stringify({
        sid: "12345678",
        op: RpcOp.RequestResponse,
        d: { id: 1, status: { ok: true, code: ErrorCode.Success }, result: {} }
      })
    ) as ResponsePayload | undefined;

    expect(decoded?.status).toBe(ErrorCode.Success);
  });

  it("rejects deprecated numeric response status", () => {
    const decoded = decodeJsonRpc(
      JSON.stringify({
        sid: "12345678",
        op: RpcOp.RequestResponse,
        d: { id: 1, status: ErrorCode.Success, result: {} }
      })
    );

    expect(decoded).toBeUndefined();
  });
});
