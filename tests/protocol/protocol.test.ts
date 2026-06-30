import { describe, expect, it, vi } from "vitest";
import { Heartbeat } from "../../src/connection/heartbeat.js";
import { hexToBytes, toBytes } from "../../src/io/bytes.js";
import {
  decodeControl,
  defaultOpenParams,
  encodeAccept,
  encodeHeartbeat,
  encodeHeartbeatAck,
  encodeOpen,
  encodeRejectedAccept
} from "../../src/protocol/codec/control.js";
import {
  FrameDecoder,
  FrameEncoder,
  MessageFragmenter,
  MessageReassembler
} from "../../src/protocol/codec/frame.js";
import { PayloadDecoder } from "../../src/protocol/codec/payload.js";
import { decodeJsonRpc, encodeJsonRpc } from "../../src/protocol/codec/jsonRpc.js";
import { decodeStream, encodeStream, kStreamHeaderSize } from "../../src/protocol/codec/stream.js";
import {
  ControlOpcode,
  PayloadType,
  RpcOp
} from "../../src/protocol/generated/axtp_ids_generated.js";
import { AXTP_SPEC_VERSION } from "../../src/protocol/generated/axtpVersion.js";
import type { Frame, Message } from "../../src/protocol/model.js";
import { helloMsg, identifyMsg, identifiedMsg, responseMsg } from "../../src/protocol/model.js";
import { Handshake } from "../../src/session/handshake/handshake.js";
import { RpcDispatcher } from "../../src/session/rpc/rpcDispatcher.js";
import { AxtpError, ErrorCode } from "../../src/types/error.js";
import {
  buildErrorResponseJson,
  buildHelloJson,
  buildIdentifiedJson,
  buildIdentifyJson,
  buildRequestJson,
  buildResponseJson
} from "../helpers/jsonRpcBuilders.js";

describe("CONTROL codec 6 TLV", () => {
  it("OPEN 编码含全部必需 TLV 且可解码", () => {
    const params = defaultOpenParams(4096, 1000);
    const bytes = encodeOpen(1, params);
    const decoded = decodeControl(bytes);
    expect(decoded.opcode).toBe(ControlOpcode.Open);
    expect(decoded.controlId).toBe(1);
    expect(decoded.statusCode).toBe(0);
    expect(decoded.tlv.maxFrameSize).toBe(4096);
    expect(decoded.tlv.supportedPayloadTypes).toBe(0x07);
    expect(decoded.tlv.supportedRpcEncodings).toBe(0x01); // JSON only
    expect(decoded.tlv.heartbeatIntervalMs).toBe(1000);
    expect(decoded.tlv.ackMode).toBe(0x00); // NONE
  });

  it("ACCEPT 成功含 selectedRpcEncoding", () => {
    const bytes = encodeAccept(1, { ...defaultOpenParams(), selectedRpcEncoding: 0x01 });
    const decoded = decodeControl(bytes);
    expect(decoded.opcode).toBe(ControlOpcode.Accept);
    expect(decoded.tlv.selectedRpcEncoding).toBe(0x01);
  });

  it("reject = 带非零 statusCode 的 ACCEPT（无 REJECT opcode）", () => {
    const bytes = encodeRejectedAccept(1, ErrorCode.ControlOpenRejected);
    const decoded = decodeControl(bytes);
    expect(decoded.opcode).toBe(ControlOpcode.Accept); // 仍是 ACCEPT
    expect(decoded.statusCode).toBe(ErrorCode.ControlOpenRejected);
  });

  it("HEARTBEAT/HEARTBEAT_ACK 无 body", () => {
    const hb = decodeControl(encodeHeartbeat(3));
    const ack = decodeControl(encodeHeartbeatAck(3));
    expect(hb.opcode).toBe(ControlOpcode.Heartbeat);
    expect(hb.controlId).toBe(3);
    expect(ack.opcode).toBe(ControlOpcode.HeartbeatAck);
    expect(ack.controlId).toBe(3); // 回显
  });

  it("TLV 编码精确字节（heartbeatIntervalMs=1000 编为 uint16 => 0a 02 03 e8，与 spec 样例一致）", () => {
    const bytes = encodeOpen(1, defaultOpenParams(4096, 1000));
    const hex = [...bytes].map((b) => b.toString(16).padStart(2, "0")).join("");
    expect(hex).toContain("0a0203e8");
  });

  it("TLV extended length marker (0xFF) 被跳过，不破坏后续 TLV（spec 40-codec.md:88）", () => {
    // opcode=OPEN(01) controlId=0001 statusCode=0000 + TLV body:
    //   unknown tag 0x99 用 extended marker: 99 FF 0003 414243
    //   maxFrameSize: 04 02 10 00 (=4096)
    const bytes = hexToBytes("0100010000" + "99FF0003414243" + "04021000");
    const decoded = decodeControl(bytes);
    expect(decoded.opcode).toBe(ControlOpcode.Open);
    expect(decoded.tlv.maxFrameSize).toBe(0x1000);
  });
});

describe("STREAM codec 16B header", () => {
  it("conformance 向量：streamId=9, seqId=1, cursor=1000000 => 000000090000000100000000000F4240", () => {
    const data = new Uint8Array([0xaa, 0xbb, 0xcc]);
    const bytes = encodeStream({ streamId: 9, seqId: 1, cursor: 1000000n, data });
    const headerHex = [...bytes.slice(0, kStreamHeaderSize)]
      .map((b) => b.toString(16).padStart(2, "0"))
      .join("");
    expect(headerHex).toBe("000000090000000100000000000f4240");
  });

  it("解码回放 streamId/seqId/cursor/data", () => {
    const data = new Uint8Array([1, 2]);
    const sp = decodeStream(encodeStream({ streamId: 42, seqId: 7, cursor: 9999n, data }));
    expect(sp).toBeDefined();
    if (sp === undefined) return;
    expect(sp.streamId).toBe(42);
    expect(sp.seqId).toBe(7);
    expect(sp.cursor).toBe(9999n);
    expect([...sp.data]).toEqual([1, 2]);
  });

  it("header 不足 16B 返回 undefined", () => {
    expect(decodeStream(new Uint8Array(10))).toBeUndefined();
  });
});

describe("Frame codec", () => {
  it("编码 -> 解码 round trip（单帧）", () => {
    const encoder = new FrameEncoder();
    const fragmenter = new MessageFragmenter();
    const message: Message = {
      messageId: 0,
      payloadType: PayloadType.Rpc,
      body: new Uint8Array([1, 2, 3])
    };
    const frame = fragmenter.fragment(message)[0];
    const bytes = encoder.encode(frame);

    const frames: Frame[] = [];
    const decoder = new FrameDecoder({ onFrame: (f) => frames.push(f) });
    decoder.onBytes(bytes);
    expect(frames.length).toBe(1);
    expect([...frames[0].payload]).toEqual([1, 2, 3]);
    expect(frames[0].header.payloadType).toBe(PayloadType.Rpc);
  });

  it("CRC 损坏被丢弃", () => {
    const encoder = new FrameEncoder();
    const fragmenter = new MessageFragmenter();
    const message: Message = {
      messageId: 0,
      payloadType: PayloadType.Rpc,
      body: new Uint8Array([1, 2, 3])
    };
    const bytes = encoder.encode(fragmenter.fragment(message)[0]);
    // 篡改 payload 中间字节（不动 header/crc，使 CRC 校验失败）
    const corrupted = bytes.slice();
    corrupted[14] ^= 0xff;
    const frames: Frame[] = [];
    const decoder = new FrameDecoder({ onFrame: (f) => frames.push(f) });
    decoder.onBytes(corrupted);
    expect(frames.length).toBe(0);
  });

  it("分片重组（多帧）", () => {
    const encoder = new FrameEncoder();
    const fragmenter = new MessageFragmenter(20); // 小 frame 触发分片
    const bigBody = new Uint8Array(50);
    for (let i = 0; i < bigBody.length; i++) bigBody[i] = i;
    const message: Message = { messageId: 0, payloadType: PayloadType.Stream, body: bigBody };
    const frames = fragmenter.fragment(message);
    expect(frames.length).toBeGreaterThan(1);

    const messages: Message[] = [];
    const reassembler = new MessageReassembler({ onMessage: (m) => messages.push(m) });
    const decoder = new FrameDecoder(reassembler);
    for (const f of frames) decoder.onBytes(encoder.encode(f));
    expect(messages.length).toBe(1);
    expect([...messages[0].body]).toEqual([...bigBody]);
  });
});

describe("PayloadDecoder onError（A3：decode 失败上报，不再静默）", () => {
  it("非 JSON rpcEncoding 上报 onError（RpcEncodingUnsupported）", () => {
    const errors: AxtpError[] = [];
    const decoder = new PayloadDecoder({
      onControl: () => {},
      onRpc: () => {},
      onStream: () => {},
      onError: (e) => errors.push(e)
    });
    decoder.onMessage(PayloadType.Rpc, new Uint8Array([0x04, 0x7a, 0x7a])); // 0x04=JSON_BINARY
    expect(errors).toHaveLength(1);
    expect(errors[0].code).toBe(ErrorCode.RpcEncodingUnsupported);
  });

  it("malformed JSON envelope 上报 onError（RpcPayloadInvalid）", () => {
    const errors: AxtpError[] = [];
    const decoder = new PayloadDecoder({
      onControl: () => {},
      onRpc: () => {},
      onStream: () => {},
      onError: (e) => errors.push(e)
    });
    decoder.onMessage(PayloadType.Rpc, new Uint8Array([0x01, 0x7a, 0x7a])); // JSON + "zz"
    expect(errors).toHaveLength(1);
    expect(errors[0].code).toBe(ErrorCode.RpcPayloadInvalid);
  });
});

describe("JSON-RPC codec", () => {
  it("Hello envelope", () => {
    const bytes = buildHelloJson();
    const p = decodeJsonRpc(bytes);
    expect(p).toBeDefined();
    if (p === undefined) return;
    expect(p).toMatchObject({ op: RpcOp.Hello, axtpVersion: AXTP_SPEC_VERSION });
  });

  it("Identify 含 randomSeed + eventMasks", () => {
    const bytes = buildIdentifyJson(0x12345678, "090101");
    const p = decodeJsonRpc(bytes);
    expect(p).toBeDefined();
    if (p === undefined) return;
    expect(p).toMatchObject({ op: RpcOp.Identify, randomSeed: 0x12345678, eventMasks: "090101" });
  });

  it("Identified.d = {} 且 sid 在外层（对齐 conformance）", () => {
    const bytes = buildIdentifiedJson("12345678");
    const obj = JSON.parse(new TextDecoder().decode(bytes));
    expect(obj.sid).toBe("12345678");
    expect(obj.op).toBe(RpcOp.Identified);
    expect(obj.d).toEqual({});
  });

  it("Request/Response round trip（method 为字符串名）", () => {
    const req = buildRequestJson(1, "audio.getAlgorithmConfig", {}, "");
    const p = decodeJsonRpc(req);
    expect(p).toBeDefined();
    if (p === undefined) return;
    expect(p).toMatchObject({ op: RpcOp.Request, requestId: 1, method: "audio.getAlgorithmConfig" });

    const resp = buildResponseJson(1, { ok: true }, "12345678");
    const rp = decodeJsonRpc(resp);
    expect(rp).toBeDefined();
    if (rp === undefined) return;
    expect(rp).toMatchObject({ requestId: 1, status: ErrorCode.Success, result: { ok: true } });
  });

  it("Error Response status = uint errorCode", () => {
    const err = buildErrorResponseJson(2, ErrorCode.RpcMethodNotFound, "12345678");
    const obj = JSON.parse(new TextDecoder().decode(err));
    expect(obj.d.id).toBe(2);
    expect(obj.d.status).toBe(0x0036); // RPC_METHOD_NOT_FOUND uint
  });

  it("无效 JSON 返回 undefined（不抛异常）", () => {
    expect(decodeJsonRpc(hexToBytes("7a7a"))).toBeUndefined();
  });
});

describe("RpcMessage codec（判别联合，与旧 codec 逐字节等价）", () => {
  it("Hello：一等字段 axtpVersion，round-trip 字节不变", () => {
    const wire = buildHelloJson();
    const msg = decodeJsonRpc(wire);
    expect(msg).toBeDefined();
    if (msg === undefined) return;
    expect(msg.op).toBe(RpcOp.Hello);
    expect(msg).toMatchObject({ sid: "", axtpVersion: AXTP_SPEC_VERSION });
    expect([...encodeJsonRpc(msg)]).toEqual([...wire]);
  });

  it("Identify：一等字段 randomSeed/eventMasks，round-trip 字节不变", () => {
    const wire = buildIdentifyJson(0x12345678, "090101");
    const msg = decodeJsonRpc(wire);
    expect(msg).toBeDefined();
    if (msg === undefined) return;
    expect(msg).toMatchObject({ op: RpcOp.Identify, randomSeed: 0x12345678, eventMasks: "090101" });
    expect([...encodeJsonRpc(msg)]).toEqual([...wire]);
  });

  it("Identify 无 eventMasks：round-trip 字节不变", () => {
    const wire = buildIdentifyJson(0x11112222, "");
    const msg = decodeJsonRpc(wire);
    expect(msg).toBeDefined();
    if (msg === undefined) return;
    expect(msg).toMatchObject({ randomSeed: 0x11112222 });
    expect((msg as { eventMasks?: string }).eventMasks).toBeUndefined();
    expect([...encodeJsonRpc(msg)]).toEqual([...wire]);
  });

  it("Identified：d={}，sid 在外层，round-trip 字节不变", () => {
    const wire = buildIdentifiedJson("12345678");
    const msg = decodeJsonRpc(wire);
    expect(msg).toBeDefined();
    if (msg === undefined) return;
    expect(msg).toMatchObject({ op: RpcOp.Identified, sid: "12345678" });
    expect([...encodeJsonRpc(msg)]).toEqual([...wire]);
  });

  it("Event：一等字段 eventName/data，round-trip 字节不变", () => {
    const wire = toBytes(
      JSON.stringify({
        sid: "12345678",
        op: RpcOp.Event,
        d: { event: "audio.algorithmConfigChanged", data: { v: 1 } }
      })
    );
    const msg = decodeJsonRpc(wire);
    expect(msg).toBeDefined();
    if (msg === undefined) return;
    expect(msg).toMatchObject({ eventName: "audio.algorithmConfigChanged", data: { v: 1 } });
    expect([...encodeJsonRpc(msg)]).toEqual([...wire]);
  });

  it("Request：一等字段 method/params，round-trip 字节不变", () => {
    const wire = buildRequestJson(7, "audio.getAlgorithmConfig", { x: 1 }, "");
    const msg = decodeJsonRpc(wire);
    expect(msg).toBeDefined();
    if (msg === undefined) return;
    expect(msg).toMatchObject({
      requestId: 7,
      method: "audio.getAlgorithmConfig",
      params: { x: 1 }
    });
    expect([...encodeJsonRpc(msg)]).toEqual([...wire]);
  });

  it("Response：status/result 一等字段，round-trip 字节不变", () => {
    const wire = buildResponseJson(7, { ok: true }, "12345678");
    const msg = decodeJsonRpc(wire);
    expect(msg).toBeDefined();
    if (msg === undefined) return;
    expect(msg).toMatchObject({ requestId: 7, status: ErrorCode.Success, result: { ok: true } });
    expect([...encodeJsonRpc(msg)]).toEqual([...wire]);
  });

  it("error response（非 success）：无 result，round-trip 字节不变", () => {
    const wire = buildErrorResponseJson(7, ErrorCode.RpcMethodNotFound, "12345678");
    const msg = decodeJsonRpc(wire);
    expect(msg).toBeDefined();
    if (msg === undefined) return;
    expect(msg).toMatchObject({ status: ErrorCode.RpcMethodNotFound });
    expect([...encodeJsonRpc(msg)]).toEqual([...wire]);
  });

  it("无效 JSON 返回 undefined（不抛）", () => {
    expect(decodeJsonRpc(hexToBytes("7a7a"))).toBeUndefined();
  });
});

describe("RpcDispatcher", () => {
  it("request -> resolve（事件驱动，无 poll）", async () => {
    const dispatcher = new RpcDispatcher();
    const { requestId, promise } = dispatcher.request((id) => {
      // 模拟响应到达
      setTimeout(() => {
        dispatcher.resolve(responseMsg("", id, ErrorCode.Success));
      }, 0);
    }, 1000);
    const payload = await promise;
    expect(payload.requestId).toBe(requestId);
    expect(payload.status).toBe(ErrorCode.Success);
  });

  it("超时 reject（RpcResponseTimeout）", async () => {
    const dispatcher = new RpcDispatcher();
    const { promise } = dispatcher.request(() => {}, 50);
    await expect(promise).rejects.toMatchObject({ code: ErrorCode.RpcResponseTimeout });
  });

  it("rejectAll 让所有 pending 失败", async () => {
    const dispatcher = new RpcDispatcher();
    const r1 = dispatcher.request(() => {}, 5000).promise;
    const r2 = dispatcher.request(() => {}, 5000).promise;
    dispatcher.rejectAll(new AxtpError(ErrorCode.TransportDisconnected, "closed"));
    await expect(r1).rejects.toBeTruthy();
    await expect(r2).rejects.toBeTruthy();
    // size() removed
  });
});

describe("Handshake 状态机", () => {
  it("server: 收 Identify -> 生成 sid -> Identified（d={}）", () => {
    const server = new Handshake("server", 0xabc);
    server.onLinkReady();
    const result = server.handle(identifyMsg("", 0x12345678));
    expect(result.becameReady).toBe(true);
    expect(result.outbound).toBeDefined();
    const outbound = result.outbound;
    if (outbound === undefined) return;
    expect(outbound).toMatchObject({ op: RpcOp.Identified });
    expect(outbound.sid).toMatch(/^[0-9a-f]{8}$/);
    expect(server.sid).toMatch(/^[0-9a-f]{8}$/);
  });

  it("sid 不等于 randomSeed（混合本地状态）", () => {
    const server = new Handshake("server", 0x111);
    server.onLinkReady();
    const _result = server.handle(identifyMsg("", 0x99999999));
    expect(server.sid).not.toBe("99999999");
    expect(parseInt(server.sid, 16)).not.toBe(0x99999999);
  });

  it("client: Hello -> Identify -> Identified", () => {
    const client = new Handshake("client");
    // Hello
    const hello = decodeJsonRpc(buildHelloJson());
    expect(hello).toBeDefined();
    if (hello === undefined) return;
    const r1 = client.handle(hello);
    expect(r1.outbound?.op).toBe(RpcOp.Identify);
    expect(client.state).toBe("FRAMING_READY");
    // Identified
    const identified = decodeJsonRpc(buildIdentifiedJson("abcdef01"));
    expect(identified).toBeDefined();
    if (identified === undefined) return;
    const r2 = client.handle(identified);
    expect(r2.becameReady).toBe(true);
    expect(client.sid).toBe("abcdef01");
    expect(client.isReady).toBe(true);
  });

  it("非法 sid 被拒（client）", () => {
    const client = new Handshake("client");
    const hello = decodeJsonRpc(buildHelloJson());
    expect(hello).toBeDefined();
    if (hello === undefined) return;
    client.handle(hello);
    const r = client.handle(identifiedMsg("xyz"));
    expect(r.error).toBeDefined();
    expect(client.isReady).toBe(false);
  });

  it("axtpVersion 主版本非 1 被拒（错误码 RpcPayloadInvalid）", () => {
    const client = new Handshake("client");
    const hello = helloMsg("", "2.0.0");
    const r = client.handle(hello);
    expect(r.error?.code).toBe(ErrorCode.RpcPayloadInvalid);
    expect(r.becameReady).toBe(false);
  });

  it("axtpVersion='1'（纯主版本号）被接受（不再因 startsWith('1.') 误拒）", () => {
    const client = new Handshake("client");
    const hello = helloMsg("", "1");
    const r = client.handle(hello);
    expect(r.error).toBeUndefined();
    expect(r.outbound?.op).toBe(RpcOp.Identify);
  });

  it("zero sid (00000000) 被拒（spec 20-core.md:211）", () => {
    const client = new Handshake("client");
    const hello = decodeJsonRpc(buildHelloJson());
    expect(hello).toBeDefined();
    if (hello === undefined) return;
    client.handle(hello);
    const r = client.handle(identifiedMsg("00000000"));
    expect(r.error).toBeDefined();
    expect(client.isReady).toBe(false);
  });
});

describe("Heartbeat", () => {
  it("intervalMs 触发 onTick", () => {
    vi.useFakeTimers();
    const tick = vi.fn();
    const hb = new Heartbeat({
      intervalMs: 1000,
      timeoutMs: 2000,
      onTick: tick,
      onTimeout: () => {}
    });
    hb.start();
    vi.advanceTimersByTime(1000);
    expect(tick).toHaveBeenCalledTimes(1);
    hb.stop();
    vi.useRealTimers();
  });

  it("reset 只取消 timeout（D2: tick 保持固定节拍）", () => {
    vi.useFakeTimers();
    const tick = vi.fn();
    const timeout = vi.fn();
    const hb = new Heartbeat({
      intervalMs: 1000,
      timeoutMs: 2000,
      onTick: tick,
      onTimeout: timeout
    });
    hb.start();
    // tick 触发
    vi.advanceTimersByTime(1000);
    expect(tick).toHaveBeenCalledTimes(1);
    // reset（收到 ack）只取消 timeout，tick 计时器不受影响
    hb.reset();
    // 下一个 tick 在 1000ms 后触发（固定间隔，不被 reset 推迟）
    vi.advanceTimersByTime(1000);
    expect(tick).toHaveBeenCalledTimes(2);
    expect(timeout).not.toHaveBeenCalled(); // timeout 被 reset 取消
    hb.stop();
    vi.useRealTimers();
  });

  it("超时无响应触发 onTimeout 并停止", () => {
    vi.useFakeTimers();
    const timeout = vi.fn();
    const hb = new Heartbeat({
      intervalMs: 1000,
      timeoutMs: 500,
      onTick: () => {},
      onTimeout: timeout
    });
    hb.start();
    vi.advanceTimersByTime(1000); // tick
    vi.advanceTimersByTime(500); // timeout 触发
    expect(timeout).toHaveBeenCalledTimes(1);
    expect(hb.isRunning).toBe(false);
    vi.useRealTimers();
  });
});
