import type { RxCommandDescriptor } from "./types.js";

/** 子进程错误分类常量。 */
export const RxErrorKind = {
  Configuration: "configuration",
  Spawn: "spawn",
  Runtime: "runtime",
  Abort: "abort",
  Timeout: "timeout",
  OutputLimit: "output-limit",
  MessageLimit: "message-limit",
  Stdin: "stdin",
  Exit: "exit",
  Signal: "signal",
} as const;

/** 子进程错误的分类。 */
export type RxChildProcessErrorKind =
  (typeof RxErrorKind)[keyof typeof RxErrorKind];

const ERROR_KINDS = new Set<string>(Object.values(RxErrorKind));

/** 错误发生时已经观察到的部分执行状态。 */
export interface RxErrorResult {
  readonly command: RxCommandDescriptor;
  readonly pid?: number;
  readonly stdout: Buffer;
  readonly stderr: Buffer;
  readonly code: number | null;
  readonly signal: NodeJS.Signals | null;
  readonly durationMs: number;
}

/** `RxChildProcessError` 的构造参数。 */
export interface RxChildProcessErrorOptions {
  readonly kind: RxChildProcessErrorKind;
  readonly command?: RxCommandDescriptor;
  readonly result?: RxErrorResult;
  readonly cause?: unknown;
}

/**
 * 库内所有可预期失败使用的类型化错误。
 *
 * `kind` 用于稳定分类，`result` 在进程已经启动时携带诊断输出。
 * 诊断输出的 stdout/stderr 始终是 Buffer，低层快照每个通道最多保留 1 MiB。
 */
export class RxChildProcessError extends Error {
  override readonly name = "RxChildProcessError";
  readonly kind: RxChildProcessErrorKind;
  readonly command?: RxCommandDescriptor;
  readonly result?: RxErrorResult;
  override readonly cause?: unknown;

  constructor(message: string, options: RxChildProcessErrorOptions) {
    super(
      message,
      options.cause === undefined ? undefined : { cause: options.cause },
    );
    this.kind = options.kind;
    this.command = options.command;
    this.result = options.result;
    this.cause = options.cause;
  }
}

/** 判断任意值是否为合法错误分类。 */
export function isChildProcessErrorKind(
  value: unknown,
): value is RxChildProcessErrorKind {
  return typeof value === "string" && ERROR_KINDS.has(value);
}

/**
 * 判断任意错误是否由本库产生，可选校验具体分类。
 *
 * 判断不依赖原型链，因此适用于依赖重复安装或 ESM/CJS 混合加载场景。
 */
export function isRxChildProcessError(
  error: unknown,
  kind?: RxChildProcessErrorKind,
): error is RxChildProcessError {
  if (typeof error !== "object" || error === null) {
    return false;
  }

  const candidate = error as {
    readonly name?: unknown;
    readonly kind?: unknown;
    readonly message?: unknown;
  };
  const matchesType =
    error instanceof RxChildProcessError ||
    (candidate.name === "RxChildProcessError" &&
      isChildProcessErrorKind(candidate.kind) &&
      typeof candidate.message === "string");

  return matchesType && (kind === undefined || candidate.kind === kind);
}
