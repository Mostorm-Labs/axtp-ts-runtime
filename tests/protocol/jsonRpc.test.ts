import { describe, expect, it } from "vitest";
import { decodeJsonRpc, encodeJsonRpc } from "../../src/protocol/codec/jsonRpc.js";
import { helloMsg, responseMsg, RpcOp, type HelloPayload, type ReidentifyPayload, type ResponsePayload } from "../../src/protocol/model.js";
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

  it("preserves failure response result details", () => {
    const details = { message: "displayName is required", field: "displayName" };
    const encoded = encodeJsonRpc(responseMsg("12345678", 3, ErrorCode.InvalidArgument, details));
    const wire = JSON.parse(new TextDecoder().decode(encoded)) as {
      d: { result?: unknown };
    };

    expect(wire.d.result).toEqual(details);

    const decoded = decodeJsonRpc(encoded) as ResponsePayload | undefined;
    expect(decoded?.result).toEqual(details);
  });
});

describe("JSON RPC Reidentify event masks", () => {
  it.each([
    [undefined, false],
    ["", true],
    ["deadbeef", true]
  ] as const)("round-trips eventMasks=%j", (eventMasks, present) => {
    const source: ReidentifyPayload = eventMasks === undefined
      ? { op: RpcOp.Reidentify, sid: "12345678" }
      : { op: RpcOp.Reidentify, sid: "12345678", eventMasks };
    const encoded = encodeJsonRpc(source);
    const wire = JSON.parse(new TextDecoder().decode(encoded)) as { d: Record<string, unknown> };
    expect(Object.hasOwn(wire.d, "eventMasks")).toBe(present);
    expect(decodeJsonRpc(encoded)).toEqual(source);
  });
});

describe("JSON RPC advisory Hello version", () => {
  it("preserves an absent axtpVersion as undefined and omits it when re-encoding", () => {
    const decoded = decodeJsonRpc(JSON.stringify({ sid: "", op: RpcOp.Hello, d: {} })) as
      | HelloPayload
      | undefined;
    expect(decoded?.axtpVersion).toBeUndefined();
    const wire = JSON.parse(new TextDecoder().decode(encodeJsonRpc(decoded as HelloPayload))) as {
      d: Record<string, unknown>;
    };
    expect(wire.d).not.toHaveProperty("axtpVersion");
    expect(helloMsg("").axtpVersion).toBeUndefined();
  });
});
