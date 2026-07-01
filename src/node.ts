// @axtp/ts-sdk/node：Node.js 传输子入口（TCP + WebSocket）。
// 浏览器环境只引主入口或 ./transport 契约，避免拉入 node:net / ws。
export {
  NodeTcpClientTransport,
  NodeTcpServerTransport
} from "./transport/tcp/nodeTcpTransport.js";
export type { TcpOptions } from "./transport/tcp/nodeTcpTransport.js";
export { NodeWsClientTransport, NodeWsServerTransport } from "./transport/ws/nodeWsTransport.js";
export type { WsClientOptions, WsServerOptions } from "./transport/ws/nodeWsTransport.js";
