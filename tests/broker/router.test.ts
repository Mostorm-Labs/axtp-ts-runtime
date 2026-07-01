// HandlerRouter：method/event handler 路由（broker/router）。
// 行为对齐原 session/handler/handlerRouter.ts：local Map + 可选 global source fallback；
// 按 name 索引（规避 method id 与 event id 共享数字空间）。

import { describe, expect, it } from "vitest";
import { HandlerRouter } from "../../src/broker/router.js";
import type { GlobalHandlerSource } from "../../src/broker/context.js";

describe("HandlerRouter — method handler", () => {
  it("setMethod/getMethod：注册后可查；返回 unsub 可注销", () => {
    const r = new HandlerRouter();
    expect(r.getMethod("m")).toBeUndefined();
    const h = (): number => 1;
    const unsub = r.setMethod("m", h);
    expect(r.getMethod("m")).toBe(h);
    unsub();
    expect(r.getMethod("m")).toBeUndefined();
  });

  it("覆盖注册后，旧 unsub 只在 handler 仍匹配时注销（不误删新 handler）", () => {
    const r = new HandlerRouter();
    const h1 = (): number => 1;
    const h2 = (): number => 2;
    const u1 = r.setMethod("m", h1);
    r.setMethod("m", h2);
    expect(r.getMethod("m")).toBe(h2);
    u1(); // h1 已被覆盖，不应删 h2
    expect(r.getMethod("m")).toBe(h2);
  });

  it("global fallback：local 缺省时查 global；local 优先", () => {
    const gm = (): string => "global";
    const gs: GlobalHandlerSource = { getMethod: () => gm, getEventListeners: () => undefined };
    const r = new HandlerRouter(gs);
    expect(r.getMethod("x")).toBe(gm);
    const lm = (): string => "local";
    r.setMethod("x", lm);
    expect(r.getMethod("x")).toBe(lm);
  });
});

describe("HandlerRouter — event handler", () => {
  it("addEventListener：多 handler；unsub 删单个", () => {
    const r = new HandlerRouter();
    let n = 0;
    const u1 = r.addEventListener("e", () => (n += 1));
    r.addEventListener("e", () => (n += 10));
    const hs = r.getEventHandlers("e");
    expect(hs.size).toBe(2);
    for (const h of hs) h(undefined);
    expect(n).toBe(11);
    u1();
    expect(r.getEventHandlers("e").size).toBe(1);
  });

  it("getEventHandlers：local + global 合并", () => {
    const ge = (): void => {};
    const gs: GlobalHandlerSource = {
      getMethod: () => undefined,
      getEventListeners: (name) => (name === "ge" ? new Set([ge]) : undefined)
    };
    const r = new HandlerRouter(gs);
    r.addEventListener("ge", () => {});
    expect(r.getEventHandlers("ge").size).toBe(2);
  });

  it("无 handler 时 getEventHandlers 返回空集合", () => {
    expect(new HandlerRouter().getEventHandlers("none").size).toBe(0);
  });
});
