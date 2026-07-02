// 公共导出面快照：锁住主入口 + 各 subpath 的导出集合，防未来漂移。
import { describe, expect, it } from "vitest";
import * as main from "../../src/index.js";
import * as ioEntry from "../../src/io.js";
import * as mockEntry from "../../src/mock.js";
import * as nodeEntry from "../../src/node.js";
import * as protocolEntry from "../../src/protocol.js";
import * as transportEntry from "../../src/transport.js";

describe("public exports: 主入口聚合 SDK 核心 + 子入口", () => {
  it("主入口导出 SDK 核心", () => {
    expect(main.AxtpClient).toBeDefined();
    expect(main.AxtpServer).toBeDefined();
    expect(main.AxtpEndpoint).toBeDefined();
    expect(main.Stream).toBeDefined();
    expect(main.AxtpError).toBeDefined();
    expect(main.ErrorCode).toBeDefined();
    expect(main.EventStream).toBeDefined();
    expect(main.registry).toBeDefined();
    expect(main.computeEventMasks).toBeDefined();
  });

  it("主入口聚合全部子入口", () => {
    expect(main.NodeTcpClientTransport).toBeDefined();
    expect(main.NodeWsServerTransport).toBeDefined();
    expect(main.ControlOpcode).toBeDefined();
    expect(main.PayloadType).toBeDefined();
    expect(main.requestMsg).toBeDefined();
    expect(main.framedBinaryProfile).toBeDefined();
    expect(main.createMockStreamLoopback).toBeDefined();
    expect(main.toBytes).toBeDefined();
  });

  it("./node 仅 Node stream 传输，不含 SDK 核心", () => {
    expect(nodeEntry.NodeTcpClientTransport).toBeDefined();
    expect(nodeEntry.NodeWsServerTransport).toBeDefined();
    expect((nodeEntry as Record<string, unknown>).AxtpClient).toBeUndefined();
  });

  it("./protocol 子入口", () => {
    expect(protocolEntry.ControlOpcode).toBeDefined();
    expect(protocolEntry.requestMsg).toBeDefined();
    expect(protocolEntry.helloMsg).toBeDefined();
    expect((protocolEntry as Record<string, unknown>).AxtpClient).toBeUndefined();
  });

  it("./transport 子入口", () => {
    expect(transportEntry.framedBinaryProfile).toBeDefined();
  });

  it("./mock 子入口", () => {
    expect(mockEntry.createMockStreamLoopback).toBeDefined();
  });

  it("./io 子入口", () => {
    expect(ioEntry.toBytes).toBeDefined();
    expect(ioEntry.concatBytes).toBeDefined();
  });
});
