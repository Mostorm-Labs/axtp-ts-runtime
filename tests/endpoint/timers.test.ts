// Heartbeat：心跳编排器（endpoint/timers）。移植自 connection/heartbeat.ts。
// D2: tick 固定节拍（不被 ack 推迟）；ack 只取消 timeout；超时 → onTimeout + stop。

import { describe, expect, it, vi, beforeEach, afterEach } from "vitest";
import { Heartbeat } from "../../src/endpoint/timers.js";

describe("Heartbeat", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it("start 后按 intervalMs 固定节拍 onTick", () => {
    const ticks: number[] = [];
    const hb = new Heartbeat({
      intervalMs: 100,
      timeoutMs: 1000,
      onTick: () => ticks.push(1),
      onTimeout: () => {}
    });
    hb.start();
    vi.advanceTimersByTime(350);
    expect(ticks.length).toBe(3); // 100/200/300
    hb.stop();
  });

  it("无 reset：超过 timeoutMs 触发 onTimeout 并 stop（tick 不再推迟 timeout）", () => {
    let timeouts = 0;
    let ticks = 0;
    const hb = new Heartbeat({
      intervalMs: 100,
      timeoutMs: 200,
      onTick: () => (ticks += 1),
      onTimeout: () => (timeouts += 1)
    });
    hb.start();
    vi.advanceTimersByTime(1000);
    expect(timeouts).toBe(1);
    const ticksAtTimeout = ticks;
    vi.advanceTimersByTime(1000);
    expect(ticks).toBe(ticksAtTimeout); // stop 后不再 tick
  });

  it("reset 取消 timeout：持续 reset 则不超时", () => {
    let timeouts = 0;
    const hb = new Heartbeat({
      intervalMs: 100,
      timeoutMs: 200,
      onTick: () => hb.reset(),
      onTimeout: () => (timeouts += 1)
    });
    hb.start();
    vi.advanceTimersByTime(1000);
    expect(timeouts).toBe(0);
    hb.stop();
  });

  it("reset 后停止 ack：超过 timeoutMs 仍触发 onTimeout（dead-peer 检测不失效）", () => {
    let timeouts = 0;
    let ack = true;
    const hb = new Heartbeat({
      intervalMs: 100,
      timeoutMs: 200,
      onTick: () => {
        if (ack) hb.reset();
      },
      onTimeout: () => (timeouts += 1)
    });
    hb.start();
    vi.advanceTimersByTime(500); // 持续 ack → reset 重设 deadline，不超时
    expect(timeouts).toBe(0);
    ack = false; // 停止 ack
    vi.advanceTimersByTime(500); // 不再 reset → timeoutMs 后应触发
    expect(timeouts).toBe(1);
  });

  it("stop 后不再 tick", () => {
    let ticks = 0;
    const hb = new Heartbeat({
      intervalMs: 100,
      timeoutMs: 1000,
      onTick: () => (ticks += 1),
      onTimeout: () => {}
    });
    hb.start();
    vi.advanceTimersByTime(250);
    const before = ticks;
    hb.stop();
    vi.advanceTimersByTime(500);
    expect(ticks).toBe(before);
  });
});
