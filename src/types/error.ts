// AxtpError: 统一的错误对象，携带 spec 的 ErrorCode。
// 所有 call/handle 失败一律 reject AxtpError。

import { ErrorCode } from "../protocol/generated/axtp_ids_generated.js";

export { ErrorCode };

export class AxtpError extends Error {
  readonly code: ErrorCode;
  readonly requestId: number | undefined;

  constructor(code: ErrorCode, message: string, cause?: unknown, requestId?: number) {
    super(message, cause !== undefined ? { cause } : undefined);
    this.name = "AxtpError";
    this.code = code;
    this.requestId = requestId;
  }
}

/** 便捷工厂：连接已断开。 */
export function connectionClosedError(reason: string): AxtpError {
  return new AxtpError(ErrorCode.TransportDisconnected, `connection closed: ${reason}`);
}

/** 便捷工厂：尚未就绪。 */
export function notReadyError(reason: string): AxtpError {
  return new AxtpError(ErrorCode.InvalidState, `session not ready: ${reason}`);
}
