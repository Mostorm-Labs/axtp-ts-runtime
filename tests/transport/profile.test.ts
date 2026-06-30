import { describe, expect, it } from "vitest";
import { RpcEncoding } from "../../src/protocol/model.js";
import {
  framedBinaryProfile,
  keepaliveMode,
  supportsControl,
  supportsStream,
  unframedJsonProfile
} from "../../src/transport/profile.js";

describe("TransportProfile 工厂", () => {
  it("framedBinaryProfile: frameMode=standard-framed, rpcEncodings=[Json]", () => {
    const p = framedBinaryProfile("AXTP-TCP");
    expect(p.frameMode).toBe("standard-framed");
    expect([...p.rpcEncodings]).toEqual([RpcEncoding.Json]);
    expect(p.profileId).toBe("AXTP-TCP");
  });

  it("framedBinaryProfile: profileId 可选（自定义 framed transport 省略）", () => {
    expect(framedBinaryProfile().profileId).toBeUndefined();
    expect(framedBinaryProfile().frameMode).toBe("standard-framed");
  });

  it("unframedJsonProfile: frameMode=unframed-json, profileId=AXTP-WS-JSON", () => {
    const p = unframedJsonProfile();
    expect(p.frameMode).toBe("unframed-json");
    expect([...p.rpcEncodings]).toEqual([RpcEncoding.Json]);
    expect(p.profileId).toBe("AXTP-WS-JSON");
  });
});

describe("TransportProfile 派生访问器", () => {
  it("supportsControl: 仅 standard-framed 为 true", () => {
    expect(supportsControl(framedBinaryProfile())).toBe(true);
    expect(supportsControl(unframedJsonProfile())).toBe(false);
    expect(supportsControl(unframedJsonProfile("AXTP-WS-CLOUD-REVERSE"))).toBe(false);
  });

  it("supportsStream: 与 supportsControl 同源（仅 standard-framed）", () => {
    expect(supportsStream(framedBinaryProfile())).toBe(true);
    expect(supportsStream(unframedJsonProfile())).toBe(false);
    expect(supportsStream(unframedJsonProfile("AXTP-WS-CLOUD-REVERSE"))).toBe(false);
  });

  it("keepaliveMode: framed=control-heartbeat, unframed*=native-keepalive", () => {
    expect(keepaliveMode(framedBinaryProfile())).toBe("control-heartbeat");
    expect(keepaliveMode(unframedJsonProfile())).toBe("native-keepalive");
  });
});

// 锁定用户决策：cloud-reverse 与 ws-json 在 wire 上同构，不单列 frameMode。
describe("cloud-reverse 拓扑不产生独立 frameMode", () => {
  it("unframedJsonProfile('AXTP-WS-CLOUD-REVERSE') 的 frameMode 仍是 unframed-json", () => {
    const cloudReverse = unframedJsonProfile("AXTP-WS-CLOUD-REVERSE");
    expect(cloudReverse.frameMode).toBe("unframed-json");
    expect(cloudReverse.profileId).toBe("AXTP-WS-CLOUD-REVERSE");
  });

  it("cloud-reverse 与 ws-json 的派生能力完全相同（拓扑差异由角色表达，非 frameMode）", () => {
    const wsJson = unframedJsonProfile();
    const cloudReverse = unframedJsonProfile("AXTP-WS-CLOUD-REVERSE");
    expect(supportsControl(wsJson)).toBe(supportsControl(cloudReverse));
    expect(supportsStream(wsJson)).toBe(supportsStream(cloudReverse));
    expect(keepaliveMode(wsJson)).toBe(keepaliveMode(cloudReverse));
  });
});
