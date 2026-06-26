// AxtpError: 统一的错误对象，携带 spec 的 ErrorCode。
// 所有 call/handle 失败一律 reject AxtpError，删除旧 lastError() 侧信道。

import { ErrorCode } from "../protocol/generated/axtp_ids_generated.js";

export { ErrorCode };

export interface AxtpErrorOptions {
  readonly code: ErrorCode;
  readonly message: string;
  readonly cause?: unknown;
  readonly requestId?: number;
}

export class AxtpError extends Error {
  readonly code: ErrorCode;
  readonly requestId: number | undefined;

  constructor(code: ErrorCode, message: string, cause?: unknown, requestId?: number) {
    super(message);
    this.name = "AxtpError";
    this.code = code;
    this.requestId = requestId;
    // 保持 cause 链可观测（ES2022 Error.cause）
    if (cause !== undefined) (this as { cause?: unknown }).cause = cause;
  }

  static fromOptions(opts: AxtpErrorOptions): AxtpError {
    return new AxtpError(opts.code, opts.message, opts.cause, opts.requestId);
  }

  /** 是否可重试（用于重连/退避决策），由 ErrorCode 语义决定。 */
  isRetryable(): boolean {
    switch (this.code) {
      case ErrorCode.RpcResponseTimeout:
      case ErrorCode.ControlHeartbeatTimeout:
      case ErrorCode.TransportDisconnected:
      case ErrorCode.Timeout:
      case ErrorCode.Busy:
      case ErrorCode.Unavailable:
        return true;
      default:
        return false;
    }
  }
}

/** 便捷工厂：连接已断开（本地错误，pending call 在断连时统一 reject）。 */
export function connectionClosedError(reason: string): AxtpError {
  return new AxtpError(ErrorCode.TransportDisconnected, `connection closed: ${reason}`);
}

/** 便捷工厂：尚未就绪（重连中 / 未 app-ready 时 call）。 */
export function notReadyError(reason: string): AxtpError {
  return new AxtpError(ErrorCode.InvalidState, `session not ready: ${reason}`);
}
