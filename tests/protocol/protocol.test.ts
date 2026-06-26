import { describe, expect, it, vi } from "vitest";
import { hexToBytes } from "../../src/io/bytes.js";
import {
  decodeControl,
  defaultOpenParams,
  encodeAccept,
  encodeHeartbeat,
  encodeHeartbeatAck,
  encodeOpen,
  encodeReject
} from "../../src/protocol/codec/control.js";
import {
  FrameDecoder,
  FrameEncoder,
  MessageFragmenter,
  MessageReassembler
} from "../../src/protocol/codec/frame.js";
import {
  buildErrorResponseJson,
  buildHelloJson,
  buildIdentifiedJson,
  buildIdentifyJson,
  buildRequestJson,
  buildResponseJson,
  decodeJsonRpc
} from "../../src/protocol/codec/jsonRpc.js";
import { decodeStream, encodeStream, kStreamHeaderSize } from "../../src/protocol/codec/stream.js";
import { Handshake } from "../../src/protocol/engine/handshake.js";
import { Heartbeat } from "../../src/protocol/engine/heartbeat.js";
import { RpcDispatcher } from "../../src/protocol/engine/rpcDispatcher.js";
import {
  ControlOpcode,
  PayloadType,
  RpcOp
} from "../../src/protocol/generated/axtp_ids_generated.js";
import type { Frame, Message } from "../../src/protocol/model.js";
import { rpcPayload } from "../../src/protocol/model.js";
import { AxtpError, ErrorCode } from "../../src/types/error.js";

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
    const bytes = encodeReject(1, ErrorCode.ControlOpenRejected);
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

  it("TLV 编码精确字节（heartbeatIntervalMs=1000 编为 uint32 => 0a 04 00 00 03 e8）", () => {
    const bytes = encodeOpen(1, defaultOpenParams(4096, 1000));
    const hex = [...bytes].map((b) => b.toString(16).padStart(2, "0")).join("");
    expect(hex).toContain("0a04000003e8");
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

describe("JSON-RPC codec", () => {
  it("Hello envelope", () => {
    const bytes = buildHelloJson();
    const p = decodeJsonRpc(bytes);
    expect(p).toBeDefined();
    if (p === undefined) return;
    expect(p.op).toBe(RpcOp.Hello);
    expect(JSON.parse(new TextDecoder().decode(p.body)).axtpVersion).toBe("1.0.0");
  });

  it("Identify 含 randomSeed + eventMasks", () => {
    const bytes = buildIdentifyJson(0x12345678, "090101");
    const p = decodeJsonRpc(bytes);
    expect(p).toBeDefined();
    if (p === undefined) return;
    expect(p.op).toBe(RpcOp.Identify);
    expect(p.meta.randomSeed).toBe(0x12345678);
    expect(p.meta.jsonEventMasks).toBe("090101");
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
    expect(p.op).toBe(RpcOp.Request);
    expect(p.requestId).toBe(1);
    expect(p.meta.jsonMethodOrEventName).toBe("audio.getAlgorithmConfig");

    const resp = buildResponseJson(1, { ok: true }, "12345678");
    const rp = decodeJsonRpc(resp);
    expect(rp).toBeDefined();
    if (rp === undefined) return;
    expect(rp.requestId).toBe(1);
    expect(rp.statusCode).toBe(ErrorCode.Success);
    expect(JSON.parse(new TextDecoder().decode(rp.body)).ok).toBe(true);
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

describe("RpcDispatcher", () => {
  it("request -> resolve（事件驱动，无 poll）", async () => {
    const dispatcher = new RpcDispatcher();
    const { requestId, promise } = dispatcher.request((id) => {
      // 模拟响应到达
      setTimeout(() => {
        dispatcher.resolve(
          rpcPayload({ op: RpcOp.RequestResponse, requestId: id, statusCode: ErrorCode.Success })
        );
      }, 0);
    }, 1000);
    const payload = await promise;
    expect(payload.requestId).toBe(requestId);
    expect(payload.statusCode).toBe(ErrorCode.Success);
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
    expect(dispatcher.size()).toBe(0);
  });
});

describe("Handshake 状态机", () => {
  it("server: 收 Identify -> 生成 sid -> Identified（d={}）", () => {
    const server = new Handshake("server", 0xabc);
    server.onLinkReady();
    const identify = rpcPayload({
      op: RpcOp.Identify,
      body: new TextEncoder().encode(JSON.stringify({ randomSeed: 0x12345678 }))
    });
    const result = server.handle(identify);
    expect(result.becameReady).toBe(true);
    expect(result.outbound).toBeDefined();
    const outbound = result.outbound;
    if (outbound === undefined) return;
    expect(outbound.op).toBe(RpcOp.Identified);
    expect(outbound.jsonSid).toMatch(/^[0-9a-f]{8}$/);
    expect(server.sid).toMatch(/^[0-9a-f]{8}$/);
    // d = {}
    expect(JSON.parse(new TextDecoder().decode(outbound.body))).toEqual({});
  });

  it("sid 不等于 randomSeed（混合本地状态）", () => {
    const server = new Handshake("server", 0x111);
    server.onLinkReady();
    const _result = server.handle(
      rpcPayload({
        op: RpcOp.Identify,
        body: new TextEncoder().encode(JSON.stringify({ randomSeed: 0x99999999 }))
      })
    );
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
    const r = client.handle(rpcPayload({ op: RpcOp.Identified, jsonSid: "xyz" }));
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

  it("reset 重置计时（收到 ack/pong）", () => {
    vi.useFakeTimers();
    const tick = vi.fn();
    const hb = new Heartbeat({
      intervalMs: 1000,
      timeoutMs: 2000,
      onTick: tick,
      onTimeout: () => {}
    });
    hb.start();
    vi.advanceTimersByTime(900);
    hb.reset(); // 收到 ack，重置
    vi.advanceTimersByTime(900);
    expect(tick).not.toHaveBeenCalled(); // 还未到 interval
    vi.advanceTimersByTime(100);
    expect(tick).toHaveBeenCalledTimes(1);
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
