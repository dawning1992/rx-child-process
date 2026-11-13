import type { ChildProcess } from "node:child_process";
import { performance } from "node:perf_hooks";
import { Readable, type Writable } from "node:stream";
import { StringDecoder } from "node:string_decoder";
import {
  Observable,
  Subscription,
  isObservable,
  type Subscriber,
} from "rxjs";
import { RxChildProcessError } from "./errors.js";
import type {
  RxBaseOptions,
  RxBatchOptions,
  RxCommandDescriptor,
  RxProcessEvent,
  RxProcessInput,
  RxSendHandle,
  RxSpawnEvent,
} from "./types.js";
import { terminateProcess } from "./termination.js";

const DEFAULT_TIMEOUT_MS = 30_000;
const DEFAULT_KILL_GRACE_MS = 2_000;
const DEFAULT_MAX_OUTPUT_BYTES = 10 * 1024 * 1024;
const ERROR_CAPTURE_BYTES = 1024 * 1024;
const DEFAULT_MAX_STDIN_QUEUE_BYTES = 1024 * 1024;
const DEFAULT_BATCH_MAX_BYTES = 64 * 1024;
const DEFAULT_BATCH_INTERVAL_MS = 10;

const REGULATED_OPTION_KEYS = [
  "signal",
  "timeout",
  "timeoutMs",
  "killSignal",
  "killGraceMs",
  "killTree",
  "detached",
  "maxBuffer",
  "maxStdoutBytes",
  "maxStderrBytes",
  "input",
  "maxStdinQueueBytes",
  "encoding",
  "batching",
  "failOnNonZeroExit",
  "failOnSignal",
  "nodeArgs",
] as const;

/** 单个执行入口向共享运行时提供的启动描述。 */
export interface ExecuteRequest {
  /** 命令元数据，会写入 spawn 事件和错误。 */
  readonly command: RxCommandDescriptor;
  /** 用户选项，既包含受监管选项，也包含需要透传给 Node 的字段。 */
  readonly options: RxBaseOptions & Record<string, unknown>;
  /** 创建 Node 子进程；运行时已经补充 detached 和 windowsHide。 */
  readonly launch: (nativeOptions: Record<string, unknown>) => ChildProcess;
  /** 未提供 input 时是否自动关闭 stdin。 */
  readonly autoEndStdin: boolean;
  /** 未显式配置 stdio 时 stdin 是否为 pipe。 */
  readonly stdinPipeByDefault: boolean;
}

interface NormalizedOptions {
  readonly signal?: AbortSignal;
  readonly timeoutMs: number;
  readonly killGraceMs: number;
  readonly killTree: boolean;
  readonly maxStdoutBytes: number;
  readonly maxStderrBytes: number;
  readonly input?: RxProcessInput;
  readonly maxStdinQueueBytes: number;
  readonly encoding: BufferEncoding | null;
  readonly batching?: Required<RxBatchOptions>;
  readonly failOnNonZeroExit: boolean;
  readonly failOnSignal: boolean;
}

interface CaptureState {
  readonly chunks: Buffer[];
  totalBytes: number;
  bytes: number;
}

interface ChannelState {
  readonly name: "stdout" | "stderr";
  readonly maxBytes: number;
  readonly capture: CaptureState;
  readonly decoder?: StringDecoder;
  readonly batchParts: Array<Buffer | string>;
  batchBytes: number;
  batchTimer?: NodeJS.Timeout;
}

interface QueuedInput {
  readonly chunk: string | Buffer;
  readonly bytes: number;
}

/**
 * 创建冷 Observable 并管理一个受监管的 Node.js 子进程。
 *
 * 每次订阅独立启动进程；终止、监听器和定时器均绑定到该订阅。
 */
export function execute<
  TMessage = unknown,
  TOutput extends Buffer | string = Buffer,
>(
  request: ExecuteRequest,
): Observable<RxProcessEvent<TMessage, TOutput>> {
  return new Observable<RxProcessEvent<TMessage, TOutput>>((subscriber) => {
    let normalized: NormalizedOptions;

    try {
      normalized = normalizeOptions(request.options);
      validateStdio(request, normalized);
    } catch (error) {
      subscriber.error(
        configurationError(
          request.command,
          error,
          "Invalid child process configuration",
        ),
      );
      return undefined;
    }

    if (normalized.signal?.aborted) {
      subscriber.error(
        new RxChildProcessError("Child process was aborted before startup", {
          kind: "abort",
          command: request.command,
          cause: normalized.signal.reason,
        }),
      );
      return undefined;
    }

    const startedAt = performance.now();
    const stdoutCapture: CaptureState = {
      chunks: [],
      totalBytes: 0,
      bytes: 0,
    };
    const stderrCapture: CaptureState = {
      chunks: [],
      totalBytes: 0,
      bytes: 0,
    };
    const cleanups: Array<() => void> = [];
    let child: ChildProcess | undefined;
    let finished = false;
    let processClosed = false;
    let spawnSeen = false;
    let exitCode: number | null = null;
    let exitSignal: NodeJS.Signals | null = null;
    let timeoutTimer: NodeJS.Timeout | undefined;
    let cleaned = false;
    let terminationRequested = false;

    const addCleanup = (cleanup: () => void): void => {
      if (cleaned) {
        try {
          cleanup();
        } catch {
          // 迟到的清理函数也必须独立执行。
        }
        return;
      }
      cleanups.push(cleanup);
    };

    const runCleanup = (): void => {
      if (cleaned) {
        return;
      }
      cleaned = true;
      for (let index = cleanups.length - 1; index >= 0; index -= 1) {
        try {
          cleanups[index]?.();
        } catch {
          // 清理阶段必须继续释放其余资源。
        }
      }
      cleanups.length = 0;
    };

    const requestTermination = (): void => {
      if (terminationRequested || !child || processClosed) {
        return;
      }
      terminationRequested = true;
      terminateProcess({
        child,
        killTree: normalized.killTree,
        graceMs: normalized.killGraceMs,
      });
    };

    const durationMs = (): number =>
      Math.max(0, performance.now() - startedAt);

    const snapshot = () => ({
      command: request.command,
      pid: child?.pid,
      stdout: Buffer.concat(stdoutCapture.chunks, stdoutCapture.bytes),
      stderr: Buffer.concat(stderrCapture.chunks, stderrCapture.bytes),
      code: exitCode,
      signal: exitSignal,
      durationMs: durationMs(),
    });

    const fail = (
      kind:
        | "spawn"
        | "runtime"
        | "abort"
        | "timeout"
        | "output-limit"
        | "stdin",
      message: string,
      cause?: unknown,
    ): void => {
      if (finished || subscriber.closed) {
        return;
      }
      finished = true;
      const error = new RxChildProcessError(message, {
        kind,
        command: request.command,
        result: snapshot(),
        cause,
      });
      runCleanup();
      requestTermination();
      subscriber.error(error);
    };

    const createChannel = (
      name: "stdout" | "stderr",
      capture: CaptureState,
      maxBytes: number,
    ): ChannelState => ({
      name,
      maxBytes,
      capture,
      decoder: normalized.encoding
        ? new StringDecoder(normalized.encoding)
        : undefined,
      batchParts: [],
      batchBytes: 0,
    });

    const flushChannel = (channel: ChannelState): void => {
      if (channel.batchTimer) {
        clearTimeout(channel.batchTimer);
        channel.batchTimer = undefined;
      }
      if (channel.batchParts.length === 0 || subscriber.closed || finished) {
        channel.batchParts.length = 0;
        channel.batchBytes = 0;
        return;
      }

      const chunk = normalized.encoding
        ? (channel.batchParts as string[]).join("")
        : Buffer.concat(channel.batchParts as Buffer[], channel.batchBytes);

      channel.batchParts.length = 0;
      channel.batchBytes = 0;

      if (chunk.length > 0) {
        emitOutputEvent(channel.name, chunk as TOutput);
      }
    };

    const appendBatch = (
      channel: ChannelState,
      part: Buffer | string,
      rawBytes: number,
    ): void => {
      if (part.length === 0) {
        return;
      }
      channel.batchParts.push(part);
      channel.batchBytes += rawBytes;

      if (
        !normalized.batching ||
        channel.batchBytes >= normalized.batching.maxBytes
      ) {
        flushChannel(channel);
        return;
      }

      if (
        channel.batchTimer ||
        normalized.batching.flushIntervalMs <= 0 ||
        !Number.isFinite(normalized.batching.flushIntervalMs)
      ) {
        return;
      }

      channel.batchTimer = setTimeout(
        () => flushChannel(channel),
        normalized.batching.flushIntervalMs,
      );
      channel.batchTimer.unref();
    };

    const emitOutputEvent = (
      type: "stdout" | "stderr",
      chunk: TOutput,
    ): void => {
      if (!subscriber.closed && !finished) {
        subscriber.next({ type, chunk });
      }
    };

    const onData = (channel: ChannelState, value: Buffer | string): void => {
      if (finished || subscriber.closed) {
        return;
      }

      const raw = Buffer.isBuffer(value) ? value : Buffer.from(value);
      if (
        Number.isFinite(channel.maxBytes) &&
        channel.capture.totalBytes + raw.length > channel.maxBytes
      ) {
        fail(
          "output-limit",
          `${channel.name} output exceeded the ${channel.maxBytes}-byte limit`,
        );
        return;
      }

      channel.capture.totalBytes += raw.length;
      const captureRemaining = ERROR_CAPTURE_BYTES - channel.capture.bytes;
      if (captureRemaining > 0) {
        const captured = raw.subarray(0, captureRemaining);
        channel.capture.chunks.push(captured);
        channel.capture.bytes += captured.length;
      }

      const part = channel.decoder
        ? channel.decoder.write(raw)
        : (raw as TOutput);

      if (normalized.batching) {
        appendBatch(channel, part, raw.length);
      } else if (part.length > 0) {
        emitOutputEvent(channel.name, part as TOutput);
      }
    };

    const finalizeChannel = (channel: ChannelState): void => {
      if (channel.decoder) {
        const tail = channel.decoder.end();
        if (tail.length > 0) {
          if (normalized.batching) {
            channel.batchParts.push(tail);
          } else {
            emitOutputEvent(channel.name, tail as TOutput);
          }
        }
      }

      if (normalized.batching) {
        flushChannel(channel);
      }
    };

    const nativeOptions = extractNativeOptions(request.options);
    nativeOptions.detached =
      process.platform === "win32" ? false : normalized.killTree;
    nativeOptions.windowsHide = nativeOptions.windowsHide ?? true;

    try {
      child = request.launch(nativeOptions);
    } catch (error) {
      subscriber.error(
        new RxChildProcessError("Failed to create child process", {
          kind: "spawn",
          command: request.command,
          cause: error,
        }),
      );
      return undefined;
    }

    const currentChild = child;
    const stdoutChannel = createChannel(
      "stdout",
      stdoutCapture,
      normalized.maxStdoutBytes,
    );
    const stderrChannel = createChannel(
      "stderr",
      stderrCapture,
      normalized.maxStderrBytes,
    );

    const onSpawn = (): void => {
      spawnSeen = true;
      if (!finished && !subscriber.closed) {
        const pid = currentChild.pid;
        if (pid === undefined) {
          fail("spawn", "Child process did not receive a PID after startup");
          return;
        }
        const event: RxSpawnEvent = {
          type: "spawn",
          pid,
          child: currentChild,
          command: request.command,
          encoding: normalized.encoding,
        };
        subscriber.next(event);
      }
    };
    const onError = (error: Error): void => {
      fail(
        spawnSeen ? "runtime" : "spawn",
        spawnSeen
          ? "Child process reported a runtime error"
          : "Child process failed to start",
        error,
      );
    };
    const onExit = (
      code: number | null,
      signal: NodeJS.Signals | null,
    ): void => {
      exitCode = code;
      exitSignal = signal;
      if (!finished && !subscriber.closed) {
        subscriber.next({ type: "exit", code, signal });
      }
    };
    const onMessage = (message: unknown, sendHandle?: unknown): void => {
      if (!finished && !subscriber.closed) {
        subscriber.next({
          type: "message",
          message: message as TMessage,
          sendHandle: sendHandle as RxSendHandle | undefined,
        });
      }
    };
    const onDisconnect = (): void => {
      if (!finished && !subscriber.closed) {
        subscriber.next({ type: "disconnect" });
      }
    };
    const onClose = (
      code: number | null,
      signal: NodeJS.Signals | null,
    ): void => {
      if (finished) {
        return;
      }
      exitCode = code;
      exitSignal = signal;
      processClosed = true;

      if (timeoutTimer) {
        clearTimeout(timeoutTimer);
        timeoutTimer = undefined;
      }
      finalizeChannel(stdoutChannel);
      finalizeChannel(stderrChannel);

      finished = true;
      const closeEvent = {
        type: "close" as const,
        code,
        signal,
        durationMs: durationMs(),
      };
      runCleanup();
      subscriber.next(closeEvent);

      const failedByCode =
        normalized.failOnNonZeroExit && code !== null && code !== 0;
      const failedBySignal =
        normalized.failOnSignal && signal !== null;

      if (failedByCode || failedBySignal) {
        subscriber.error(
          new RxChildProcessError(
            failedByCode
              ? `Child process exited with code ${code}`
              : `Child process terminated by signal ${signal}`,
            {
              kind: failedByCode ? "exit" : "signal",
              command: request.command,
              result: snapshot(),
            },
          ),
        );
        return;
      }

      subscriber.complete();
    };

    currentChild.once("spawn", onSpawn);
    currentChild.once("error", onError);
    currentChild.once("exit", onExit);
    currentChild.once("close", onClose);
    addCleanup(() => currentChild.off("spawn", onSpawn));
    addCleanup(() => currentChild.off("error", onError));
    addCleanup(() => currentChild.off("exit", onExit));
    addCleanup(() => currentChild.off("close", onClose));

    if (currentChild.stdout) {
      const onStdout = (value: Buffer | string): void =>
        onData(stdoutChannel, value);
      currentChild.stdout.on("data", onStdout);
      addCleanup(() => currentChild.stdout?.off("data", onStdout));
    }

    if (currentChild.stderr) {
      const onStderr = (value: Buffer | string): void =>
        onData(stderrChannel, value);
      currentChild.stderr.on("data", onStderr);
      addCleanup(() => currentChild.stderr?.off("data", onStderr));
    }

    if (currentChild.connected || currentChild.channel) {
      currentChild.on("message", onMessage);
      currentChild.on("disconnect", onDisconnect);
      addCleanup(() => currentChild.off("message", onMessage));
      addCleanup(() => currentChild.off("disconnect", onDisconnect));
    }

    if (
      Number.isFinite(normalized.timeoutMs) &&
      normalized.timeoutMs >= 0
    ) {
      timeoutTimer = setTimeout(() => {
        fail(
          "timeout",
          `Child process exceeded the ${normalized.timeoutMs} ms timeout`,
        );
      }, normalized.timeoutMs);
      timeoutTimer.unref();
      addCleanup(() => {
        if (timeoutTimer) {
          clearTimeout(timeoutTimer);
          timeoutTimer = undefined;
        }
      });
    }

    if (normalized.signal) {
      const signal = normalized.signal;
      const onAbort = (): void => {
        if (!processClosed) {
          fail("abort", "Child process execution was aborted", signal.reason);
        }
      };
      signal.addEventListener("abort", onAbort, { once: true });
      addCleanup(() => signal.removeEventListener("abort", onAbort));
    }

    const inputCleanup = wireInput({
      input: normalized.input,
      child: currentChild,
      maxQueueBytes: normalized.maxStdinQueueBytes,
      autoEnd: request.autoEndStdin,
      fail,
      isFinished: () => finished || subscriber.closed,
    });
    if (inputCleanup) {
      addCleanup(inputCleanup);
    }

    return () => {
      if (!finished) {
        finished = true;
      }
      runCleanup();
      if (!processClosed) {
        requestTermination();
      }
    };
  });
}

function normalizeOptions(
  options: RxBaseOptions & Record<string, unknown>,
): NormalizedOptions {
  const timeoutMs = normalizeLimit(
    options.timeoutMs,
    DEFAULT_TIMEOUT_MS,
    "timeoutMs",
  );
  const killGraceMs = normalizeNonNegative(
    options.killGraceMs,
    DEFAULT_KILL_GRACE_MS,
    "killGraceMs",
  );
  const maxStdoutBytes = normalizeLimit(
    options.maxStdoutBytes,
    DEFAULT_MAX_OUTPUT_BYTES,
    "maxStdoutBytes",
  );
  const maxStderrBytes = normalizeLimit(
    options.maxStderrBytes,
    DEFAULT_MAX_OUTPUT_BYTES,
    "maxStderrBytes",
  );
  const maxStdinQueueBytes = normalizeLimit(
    options.maxStdinQueueBytes,
    DEFAULT_MAX_STDIN_QUEUE_BYTES,
    "maxStdinQueueBytes",
  );
  const encoding = options.encoding ?? null;

  if (encoding !== null && !Buffer.isEncoding(encoding)) {
    throw new TypeError(`Unsupported output encoding: ${String(encoding)}`);
  }
  if (encoding !== null) {
    try {
      new StringDecoder(encoding);
    } catch (error) {
      throw new TypeError(
        `Output encoding is not supported by the current Node.js: ${encoding}`,
        {
          cause: error,
        },
      );
    }
  }

  const signal = options.signal;
  if (signal !== undefined && !isAbortSignal(signal)) {
    throw new TypeError("signal must be an AbortSignal");
  }

  const input = options.input;
  if (input !== undefined && !isSupportedInput(input)) {
    throw new TypeError(
      "input must be a string, Buffer, Readable, or Observable",
    );
  }

  return {
    signal,
    timeoutMs,
    killGraceMs,
    killTree: options.killTree ?? true,
    maxStdoutBytes,
    maxStderrBytes,
    input,
    maxStdinQueueBytes,
    encoding,
    batching: normalizeBatching(options.batching),
    failOnNonZeroExit: options.failOnNonZeroExit ?? false,
    failOnSignal: options.failOnSignal ?? false,
  };
}

function normalizeBatching(
  value: false | RxBatchOptions | undefined,
): Required<RxBatchOptions> | undefined {
  if (value === false || value === undefined) {
    return undefined;
  }
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new TypeError(
      "batching must be false or a batching options object",
    );
  }

  const maxBytes = normalizeLimit(
    value.maxBytes,
    DEFAULT_BATCH_MAX_BYTES,
    "batching.maxBytes",
  );
  const flushIntervalMs = normalizeNonNegative(
    value.flushIntervalMs,
    DEFAULT_BATCH_INTERVAL_MS,
    "batching.flushIntervalMs",
  );

  return { maxBytes, flushIntervalMs };
}

function normalizeLimit(
  value: number | undefined,
  fallback: number,
  name: string,
): number {
  if (value === undefined) {
    return fallback;
  }
  if (value === 0 || value === Number.POSITIVE_INFINITY) {
    return Number.POSITIVE_INFINITY;
  }
  if (!Number.isFinite(value) || value < 0) {
    throw new TypeError(`${name} must be non-negative, 0, or Infinity`);
  }
  return Math.floor(value);
}

function normalizeNonNegative(
  value: number | undefined,
  fallback: number,
  name: string,
): number {
  if (value === undefined) {
    return fallback;
  }
  if (value === Number.POSITIVE_INFINITY) {
    return value;
  }
  if (!Number.isFinite(value) || value < 0) {
    throw new TypeError(`${name} must be non-negative or Infinity`);
  }
  return Math.floor(value);
}

function validateStdio(
  request: ExecuteRequest,
  normalized: NormalizedOptions,
): void {
  if (
    normalized.input !== undefined &&
    !isPipe(request.options.stdio, 0, request.stdinPipeByDefault)
  ) {
    throw new TypeError("stdin must be a pipe when input is configured");
  }
}

function isPipe(
  stdio: unknown,
  index: number,
  defaultValue: boolean,
): boolean {
  if (stdio === undefined) {
    return defaultValue;
  }
  if (Array.isArray(stdio)) {
    const entry = stdio[index];
    return entry === undefined || entry === null || entry === "pipe";
  }
  return stdio === "pipe";
}

function isSupportedInput(input: unknown): input is RxProcessInput {
  return (
    typeof input === "string" ||
    Buffer.isBuffer(input) ||
    isReadable(input) ||
    isObservable(input)
  );
}

function isReadable(value: unknown): value is Readable {
  return value instanceof Readable;
}

function isAbortSignal(value: unknown): value is AbortSignal {
  return (
    typeof value === "object" &&
    value !== null &&
    "aborted" in value &&
    typeof (value as { addEventListener?: unknown }).addEventListener ===
      "function"
  );
}

function extractNativeOptions(
  options: RxBaseOptions & Record<string, unknown>,
): Record<string, unknown> {
  const nativeOptions = { ...options } as Record<string, unknown>;
  for (const key of REGULATED_OPTION_KEYS) {
    delete nativeOptions[key];
  }
  return nativeOptions;
}

function configurationError(
  command: RxCommandDescriptor,
  cause: unknown,
  message: string,
): RxChildProcessError {
  return new RxChildProcessError(message, {
    kind: "configuration",
    command,
    cause,
  });
}

interface WireInputOptions {
  readonly input?: RxProcessInput;
  readonly child: ChildProcess;
  readonly maxQueueBytes: number;
  readonly autoEnd: boolean;
  readonly fail: (kind: "stdin", message: string, cause?: unknown) => void;
  readonly isFinished: () => boolean;
}

function wireInput(options: WireInputOptions): (() => void) | undefined {
  const stdin = options.child.stdin;
  if (!options.input) {
    if (options.autoEnd && stdin) {
      const cleanup = attachStdinErrorGuard(stdin, options);
      stdin.end();
      return cleanup;
    }
    return undefined;
  }

  if (!stdin) {
    options.fail(
      "stdin",
      "The current stdio configuration has no writable stdin pipe",
    );
    return undefined;
  }

  const input = options.input;
  if (typeof input === "string" || Buffer.isBuffer(input)) {
    const cleanup = attachStdinErrorGuard(stdin, options);
    try {
      stdin.end(input);
    } catch (error) {
      if (!isIgnorableStdinError(error)) {
        options.fail("stdin", "Failed to write to child process stdin", error);
      }
    }
    return cleanup;
  }

  if (isReadable(input)) {
    const onReadableError = (error: Error): void => {
      if (!options.isFinished()) {
        options.fail("stdin", "Failed to read stdin input", error);
      }
    };
    const cleanupGuard = attachStdinErrorGuard(stdin, options);
    input.once("error", onReadableError);
    input.pipe(stdin);

    return () => {
      input.off("error", onReadableError);
      input.unpipe(stdin);
      cleanupGuard();
    };
  }

  if (!isObservable(input)) {
    options.fail("stdin", "Unsupported stdin input type");
    return undefined;
  }

  return wireObservableInput(input, stdin, options);
}

function wireObservableInput(
  input: Observable<string | Buffer>,
  stdin: Writable,
  options: WireInputOptions,
): () => void {
  const queue: QueuedInput[] = [];
  let queuedBytes = 0;
  let waitingForDrain = false;
  let sourceCompleted = false;
  let stdinEnded = false;
  let disposed = false;

  const guardCleanup = attachStdinErrorGuard(stdin, options);

  const endIfReady = (): void => {
    if (
      !stdinEnded &&
      sourceCompleted &&
      queue.length === 0 &&
      !waitingForDrain
    ) {
      stdinEnded = true;
      stdin.end();
    }
  };

  const pump = (): void => {
    if (disposed || options.isFinished() || waitingForDrain || stdinEnded) {
      endIfReady();
      return;
    }

    while (queue.length > 0) {
      const item = queue.shift() as QueuedInput;
      queuedBytes -= item.bytes;
      try {
        if (!stdin.write(item.chunk)) {
          waitingForDrain = true;
          return;
        }
      } catch (error) {
        if (!isIgnorableStdinError(error)) {
          options.fail(
            "stdin",
            "Failed to write to child process stdin",
            error,
          );
        }
        return;
      }
    }

    endIfReady();
  };

  const onDrain = (): void => {
    waitingForDrain = false;
    pump();
  };
  stdin.on("drain", onDrain);

  const subscription: Subscription = input.subscribe({
    next(chunk) {
      if (disposed || options.isFinished()) {
        return;
      }
      const bytes = Buffer.byteLength(chunk);
      if (
        Number.isFinite(options.maxQueueBytes) &&
        queuedBytes + bytes > options.maxQueueBytes
      ) {
        options.fail(
          "stdin",
          `stdin Observable backlog exceeded the ${options.maxQueueBytes}-byte limit`,
        );
        return;
      }
      queue.push({ chunk, bytes });
      queuedBytes += bytes;
      pump();
    },
    error(error) {
      if (!disposed && !options.isFinished()) {
        options.fail("stdin", "stdin Observable input failed", error);
      }
    },
    complete() {
      sourceCompleted = true;
      pump();
    },
  });

  if (options.isFinished()) {
    subscription.unsubscribe();
    disposed = true;
    queue.length = 0;
    queuedBytes = 0;
    stdin.off("drain", onDrain);
    guardCleanup();
  }

  return () => {
    disposed = true;
    subscription.unsubscribe();
    queue.length = 0;
    queuedBytes = 0;
    stdin.off("drain", onDrain);
    guardCleanup();
  };
}

function attachStdinErrorGuard(
  stdin: Writable,
  options: WireInputOptions,
): () => void {
  const onStdinError = (error: NodeJS.ErrnoException): void => {
    if (options.isFinished() || isIgnorableStdinError(error)) {
      return;
    }
    options.fail("stdin", "Child process stdin encountered an error", error);
  };

  stdin.on("error", onStdinError);
  return () => stdin.off("error", onStdinError);
}

function isIgnorableStdinError(error: unknown): boolean {
  if (typeof error !== "object" || error === null || !("code" in error)) {
    return false;
  }
  const code = (error as { code?: unknown }).code;
  return (
    code === "EPIPE" ||
    code === "ERR_STREAM_WRITE_AFTER_END" ||
    code === "ERR_STREAM_DESTROYED"
  );
}
