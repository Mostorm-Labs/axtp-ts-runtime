// @axtp/ts-sdk/node：Node.js stream 传输子入口（TCP framed + WS unframed）。
// 浏览器环境只引主入口或 ./transport 契约，避免拉入 node:net / ws。
export {
  NodeTcpStreamClientTransport,
  NodeTcpStreamServerTransport
} from "./transport/tcp/nodeTcpStreamTransport.js";
export type { TcpStreamOptions } from "./transport/tcp/nodeTcpStreamTransport.js";
export {
  NodeWsStreamClientTransport,
  NodeWsStreamServerTransport
} from "./transport/ws/nodeWsStreamTransport.js";
export type {
  WsStreamClientOptions,
  WsStreamServerOptions
} from "./transport/ws/nodeWsStreamTransport.js";
