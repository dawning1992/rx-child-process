import { performance } from "node:perf_hooks";
import { Observable, type Subscription } from "rxjs";
import {
  RxChildProcessError,
  isRxChildProcessError,
} from "./errors.js";
import type {
  RxCollectOptions,
  RxCommandDescriptor,
  RxCommandResult,
  RxProcessEvent,
} from "./types.js";

const DEFAULT_MAX_MESSAGES = 1_000;
const UNKNOWN_COMMAND: RxCommandDescriptor = {
  file: "<unknown>",
  args: [],
  shell: false,
};

interface AggregateOutput {
  readonly buffers: Buffer[];
  readonly strings: string[];
  mode: "buffer" | "string" | undefined;
  bytes: number;
}

/**
 * 将低层进程事件聚合为单一结果。
 *
 * 该 Observable 只发射一次结果并立即完成。上游如果已经进入 error 通道，
 * 错误会原样传播；聚合器只在低层正常完成时应用自身的退出失败策略。
 */
export function rxCollect<
  TMessage = unknown,
  TOutput extends Buffer | string = Buffer,
>(
  source: Observable<RxProcessEvent<TMessage, TOutput>>,
  options: RxCollectOptions = {},
): Observable<RxCommandResult<TMessage, TOutput>> {
  return new Observable<RxCommandResult<TMessage, TOutput>>((subscriber) => {
    const failOnNonZeroExit = options.failOnNonZeroExit ?? true;
    const failOnSignal = options.failOnSignal ?? true;
    let maxMessages: number;
    try {
      maxMessages = normalizeMaxMessages(options.maxMessages);
    } catch (error) {
      subscriber.error(
        new RxChildProcessError("无效的 rxCollect 配置", {
          kind: "configuration",
          cause: error,
        }),
      );
      return undefined;
    }
    const startedAt = performance.now();
    const stdout = createAggregate();
    const stderr = createAggregate();
    const messages: TMessage[] = [];

    let command = UNKNOWN_COMMAND;
    let pid: number | undefined;
    let encoding: BufferEncoding = "utf8";
    let code: number | null = null;
    let signal: NodeJS.Signals | null = null;
    let durationMs: number | undefined;
    let closeScheduled = false;
    let terminated = false;
    let upstream: Subscription | undefined;

    const elapsed = (): number =>
      durationMs ?? Math.max(0, performance.now() - startedAt);

    const buildOutput = (state: AggregateOutput): TOutput => {
      if (state.mode === "string") {
        return state.strings.join("") as TOutput;
      }
      return Buffer.concat(state.buffers, state.bytes) as TOutput;
    };

    const buildResult = (): RxCommandResult<TMessage, TOutput> => ({
      command,
      pid,
      stdout: buildOutput(stdout),
      stderr: buildOutput(stderr),
      code,
      signal,
      messages: [...messages],
      durationMs: elapsed(),
    });

    const buildErrorResult = () => ({
      command,
      pid,
      stdout:
        stdout.mode === "string"
          ? Buffer.from(stdout.strings.join(""), encoding)
          : Buffer.concat(stdout.buffers, stdout.bytes),
      stderr:
        stderr.mode === "string"
          ? Buffer.from(stderr.strings.join(""), encoding)
          : Buffer.concat(stderr.buffers, stderr.bytes),
      code,
      signal,
      durationMs: elapsed(),
    });

    const stop = (): void => {
      terminated = true;
      upstream?.unsubscribe();
    };

    const fail = (
      kind: "configuration" | "message-limit" | "runtime",
      message: string,
      cause?: unknown,
    ): void => {
      const error = new RxChildProcessError(message, {
        kind,
        command,
        result: buildErrorResult(),
        cause,
      });
      stop();
      subscriber.error(error);
    };

    const finish = (): void => {
      const result = buildResult();
      const failedByCode =
        failOnNonZeroExit && code !== null && code !== 0;
      const failedBySignal = failOnSignal && signal !== null;
      terminated = true;
      upstream?.unsubscribe();

      if (failedByCode || failedBySignal) {
        subscriber.error(
          new RxChildProcessError(
            failedByCode
              ? `子进程以退出码 ${code} 结束`
              : `子进程被信号 ${signal} 终止`,
            {
              kind: failedByCode ? "exit" : "signal",
              command,
              result: buildErrorResult(),
            },
          ),
        );
        return;
      }

      subscriber.next(result);
      subscriber.complete();
    };

    const appendChunk = (state: AggregateOutput, chunk: TOutput): boolean => {
      const mode = typeof chunk === "string" ? "string" : "buffer";
      if (state.mode !== undefined && state.mode !== mode) {
        fail("configuration", "上游输出混用了 Buffer 和 string 数据块");
        return false;
      }
      state.mode = mode;

      if (mode === "string") {
        const value = chunk as string;
        state.strings.push(value);
        state.bytes += Buffer.byteLength(value, encoding);
      } else {
        const value = chunk as Buffer;
        state.buffers.push(value);
        state.bytes += value.length;
      }
      return true;
    };

    upstream = source.subscribe({
      next(event) {
        if (terminated) {
          return;
        }

        switch (event.type) {
          case "spawn":
            command = event.command;
            pid = event.pid;
            encoding =
              event.encoding === null ? "utf8" : event.encoding;
            break;
          case "stdout":
            appendChunk(stdout, event.chunk);
            break;
          case "stderr":
            appendChunk(stderr, event.chunk);
            break;
          case "message":
            if (
              Number.isFinite(maxMessages) &&
              messages.length >= maxMessages
            ) {
              fail(
                "message-limit",
                `IPC 消息数量超过 ${maxMessages} 条限制`,
              );
              return;
            }
            messages.push(event.message);
            break;
          case "exit":
            code = event.code;
            signal = event.signal;
            break;
          case "close":
            code = event.code;
            signal = event.signal;
            durationMs = event.durationMs;
            closeScheduled = true;
            queueMicrotask(() => {
              if (closeScheduled) {
                finish();
              }
            });
            break;
          case "disconnect":
            break;
        }
      },
      error(error) {
        if (terminated) {
          return;
        }
        terminated = true;
        if (isRxChildProcessError(error)) {
          subscriber.error(error);
          return;
        }
        subscriber.error(
          new RxChildProcessError("子进程事件流发生错误", {
            kind: "runtime",
            command,
            result: buildErrorResult(),
            cause: error,
          }),
        );
      },
      complete() {
        if (terminated) {
          return;
        }
        closeScheduled = false;
        finish();
      },
    });

    if (terminated) {
      upstream.unsubscribe();
    }

    return () => {
      closeScheduled = false;
      terminated = true;
      upstream?.unsubscribe();
    };
  });
}

function createAggregate(): AggregateOutput {
  return {
    buffers: [],
    strings: [],
    mode: undefined,
    bytes: 0,
  };
}

function normalizeMaxMessages(value: number | undefined): number {
  if (value === undefined) {
    return DEFAULT_MAX_MESSAGES;
  }
  if (value === 0 || value === Number.POSITIVE_INFINITY) {
    return Number.POSITIVE_INFINITY;
  }
  if (!Number.isFinite(value) || value < 0) {
    throw new TypeError("maxMessages 必须是非负数、0 或 Infinity");
  }
  return Math.floor(value);
}
