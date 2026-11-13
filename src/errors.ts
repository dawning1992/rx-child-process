import type {
  RxCommandDescriptor,
  RxOutputEvent,
  RxProcessEvent,
} from "./types.js";

/** 子进程错误的分类。 */
export type RxChildProcessErrorKind =
  | "configuration"
  | "spawn"
  | "runtime"
  | "abort"
  | "timeout"
  | "output-limit"
  | "message-limit"
  | "stdin"
  | "exit"
  | "signal";

/** 错误发生时已经观察到的部分执行状态。 */
export interface RxErrorResult {
  readonly command: RxCommandDescriptor;
  readonly pid?: number;
  readonly stdout: Buffer;
  readonly stderr: Buffer;
  readonly code: number | null;
  readonly signal: NodeJS.Signals | null;
  readonly messages: readonly unknown[];
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
 * `kind` 用于稳定分类，`result` 在进程已经启动时携带有界部分输出。
 */
export class RxChildProcessError extends Error {
  override readonly name = "RxChildProcessError";
  readonly kind: RxChildProcessErrorKind;
  readonly command?: RxCommandDescriptor;
  readonly result?: RxErrorResult;
  override readonly cause?: unknown;

  constructor(message: string, options: RxChildProcessErrorOptions) {
    super(message, options.cause === undefined ? undefined : { cause: options.cause });
    this.kind = options.kind;
    this.command = options.command;
    this.result = options.result;
    this.cause = options.cause;
  }
}

/** 判断事件是否由指定通道产生。 */
export function isOutputEvent<
  TMessage,
  TOutput extends Buffer | string,
>(
  event: RxProcessEvent<TMessage, TOutput>,
): event is RxOutputEvent<TOutput> {
  return event.type === "stdout" || event.type === "stderr";
}
