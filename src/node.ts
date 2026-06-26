// @axtp/runtime Node 入口（含 Node 传输实现）。
// 通用 API 从 ./index 重新导出，额外导出 TCP/WS transport。

export * from "./index.js";

// Node 传输
export {
  NodeTcpClientTransport,
  NodeTcpServerTransport
} from "./transport/tcp/nodeTcpTransport.js";
export { NodeWsClientTransport, NodeWsServerTransport } from "./transport/ws/nodeWsTransport.js";
export type { NativePingCapable } from "./transport/ws/nodeWsTransport.js";
export { hasNativePing } from "./transport/ws/nodeWsTransport.js";

// Mock（测试/开发用）
export {
  MockClientTransport,
  MockServerTransport,
  MockTransport,
  createMockTransportPair
} from "./transport/mock/mockTransport.js";
