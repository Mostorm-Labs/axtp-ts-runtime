// gateServerError：server listen 的错误分流。
// pre-listen 错误（如 EADDRINUSE）经 reject 上抛；post-listen 错误（accept 期 ECONNRESET/EMFILE 等）
// 经 onError 上报——reject 对已 resolve 的 Promise 是 no-op，必须经 onError 显式上抛，否则被静默吞掉。
// 消除 TCP/WS server 重复的 `if (!listening) reject else onError` 模式。

import { AxtpError, ErrorCode } from "../../types/error.js";

export function gateServerError(opts: {
  isListening: () => boolean;
  reject: (err: Error) => void;
  onError: (err: AxtpError) => void;
}): (err: Error) => void {
  return (err) => {
    if (!opts.isListening()) {
      opts.reject(err);
      return;
    }
    opts.onError(new AxtpError(ErrorCode.TransportDisconnected, err.message, err));
  };
}
