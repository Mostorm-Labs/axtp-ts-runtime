// diagnostics.ts：可插拔 runtime trace，用于排查事件监听、收发和分发路径。
// 默认不输出；用户传入 diagnostics.logger 后按 level 过滤后回调。

export type AxtpDiagnosticLevel = "debug" | "info" | "warn" | "error";

export type AxtpDiagnosticScope =
  | "client"
  | "server"
  | "endpoint"
  | "core"
  | "broker"
  | "transport"
  | "wire";

export interface AxtpDiagnosticEntry {
  readonly ts: number;
  readonly level: AxtpDiagnosticLevel;
  readonly scope: AxtpDiagnosticScope;
  readonly event: string;
  readonly sid?: string;
  readonly endpointId?: number;
  readonly direction?: "in" | "out";
  readonly name?: string;
  readonly op?: string;
  readonly requestId?: number;
  readonly streamId?: number;
  readonly controlId?: number;
  readonly bytes?: number;
  readonly status?: number;
  readonly known?: boolean;
  readonly handlerCount?: number;
  readonly data?: unknown;
}

export interface AxtpDiagnostics {
  /** 默认 true；显式 false 可关闭已传入的 diagnostics。 */
  readonly enabled?: boolean;
  /** 最低输出级别；默认 debug。 */
  readonly level?: AxtpDiagnosticLevel;
  /** 是否把 payload/params 放入 data；默认 false，避免日志泄露敏感信息。 */
  readonly includePayload?: boolean;
  readonly logger: (entry: AxtpDiagnosticEntry) => void;
}

const LEVEL_ORDER: Record<AxtpDiagnosticLevel, number> = {
  debug: 10,
  info: 20,
  warn: 30,
  error: 40
};

export function emitDiagnostic(
  diagnostics: AxtpDiagnostics | undefined,
  entry: Omit<AxtpDiagnosticEntry, "ts">
): void {
  if (diagnostics === undefined || diagnostics.enabled === false) return;
  const minLevel = diagnostics.level ?? "debug";
  if (LEVEL_ORDER[entry.level] < LEVEL_ORDER[minLevel]) return;
  try {
    diagnostics.logger({ ts: Date.now(), ...entry });
  } catch {
    /* diagnostics must not affect runtime behavior */
  }
}

export function diagnosticData(
  diagnostics: AxtpDiagnostics | undefined,
  data: unknown
): unknown | undefined {
  return diagnostics?.includePayload === true ? data : undefined;
}
