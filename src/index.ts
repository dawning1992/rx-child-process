import { fork, spawn } from "node:child_process";
import { Observable } from "rxjs";
import { execute, type ExecuteRequest } from "./runner.js";
import type {
  RxBaseOptions,
  RxCommandDescriptor,
  RxExecFileOptions,
  RxExecOptions,
  RxForkOptions,
  RxNodeCodeOptions,
  RxProcessEvent,
  RxSpawnOptions,
} from "./types.js";

export { rxCollect } from "./collect.js";
export { RxEventType, RxLimits } from "./constants.js";
export {
  filterEvent,
  isCloseEvent,
  isDisconnectEvent,
  isExitEvent,
  isMessageEvent,
  isOutputEvent,
  isRxEventType,
  isSpawnEvent,
  isStderrEvent,
  isStdoutEvent,
  type RxEventOfType,
} from "./events.js";
export {
  RxChildProcessError,
  RxErrorKind,
  isChildProcessErrorKind,
  isRxChildProcessError,
  type RxChildProcessErrorKind,
  type RxChildProcessErrorOptions,
  type RxErrorResult,
} from "./errors.js";
export type {
  RxBaseOptions,
  RxBatchOptions,
  RxCloseEvent,
  RxCollectOptions,
  RxCommandDescriptor,
  RxCommandResult,
  RxDisconnectEvent,
  RxExecFileOptions,
  RxExecOptions,
  RxExitEvent,
  RxForkOptions,
  RxMessageEvent,
  RxNodeCodeOptions,
  RxOutputEvent,
  RxProcessEvent,
  RxProcessInput,
  RxSendHandle,
  RxSpawnEvent,
  RxSpawnOptions,
  RxStderrEvent,
  RxStdoutEvent,
} from "./types.js";

/** 使用可执行文件和 argv 启动子进程，默认不经过 shell。 */
export function rxSpawn(
  command: string,
  options: RxSpawnOptions & { encoding: BufferEncoding },
): Observable<RxProcessEvent<unknown, string>>;
export function rxSpawn(
  command: string,
  options?: RxSpawnOptions & { encoding?: null },
): Observable<RxProcessEvent<unknown, Buffer>>;
export function rxSpawn(
  command: string,
  options?: RxSpawnOptions,
): Observable<RxProcessEvent<unknown, Buffer | string>>;
export function rxSpawn(
  command: string,
  args: readonly string[] | undefined,
  options: RxSpawnOptions & { encoding: BufferEncoding },
): Observable<RxProcessEvent<unknown, string>>;
export function rxSpawn(
  command: string,
  args?: readonly string[],
  options?: RxSpawnOptions & { encoding?: null },
): Observable<RxProcessEvent<unknown, Buffer>>;
export function rxSpawn(
  command: string,
  args?: readonly string[],
  options?: RxSpawnOptions,
): Observable<RxProcessEvent<unknown, Buffer | string>>;
export function rxSpawn(
  command: string,
  argsOrOptions?: readonly string[] | RxSpawnOptions,
  options?: RxSpawnOptions,
): Observable<RxProcessEvent<unknown, Buffer | string>> {
  const resolved = splitArgsAndOptions(argsOrOptions, options);
  const resolvedArgs = resolved.args;
  const resolvedOptions = resolved.options;
  return executeRequest<unknown, Buffer | string>({
    command: descriptor(
      command,
      resolvedArgs,
      resolvedOptions.shell ?? false,
      resolvedOptions.cwd,
    ),
    options: toRecord(resolvedOptions),
    launch(nativeOptions) {
      return spawn(command, resolvedArgs, {
        ...nativeOptions,
        shell: resolvedOptions.shell ?? false,
      });
    },
    autoEndStdin: false,
    stdinPipeByDefault: true,
  });
}

/** 通过 shell 执行命令；不得拼接未经验证的输入。 */
export function rxExec(
  command: string,
  options: RxExecOptions & { encoding: BufferEncoding },
): Observable<RxProcessEvent<unknown, string>>;
export function rxExec(
  command: string,
  options?: RxExecOptions & { encoding?: null },
): Observable<RxProcessEvent<unknown, Buffer>>;
export function rxExec(
  command: string,
  options?: RxExecOptions,
): Observable<RxProcessEvent<unknown, Buffer | string>>;
export function rxExec(
  command: string,
  options?: RxExecOptions,
): Observable<RxProcessEvent<unknown, Buffer | string>> {
  const resolvedOptions = options ?? {};
  const shell = resolvedOptions.shell ?? true;
  return executeRequest<unknown, Buffer | string>({
    command: descriptor(command, [], shell, resolvedOptions.cwd),
    options: toRecord(resolvedOptions),
    launch(nativeOptions) {
      return spawn(command, [], {
        ...nativeOptions,
        shell,
        stdio: "pipe",
      });
    },
    autoEndStdin: true,
    stdinPipeByDefault: true,
  });
}

/** 直接执行可执行文件，默认不经过 shell。 */
export function rxExecFile(
  file: string,
  options: RxExecFileOptions & { encoding: BufferEncoding },
): Observable<RxProcessEvent<unknown, string>>;
export function rxExecFile(
  file: string,
  options?: RxExecFileOptions & { encoding?: null },
): Observable<RxProcessEvent<unknown, Buffer>>;
export function rxExecFile(
  file: string,
  options?: RxExecFileOptions,
): Observable<RxProcessEvent<unknown, Buffer | string>>;
export function rxExecFile(
  file: string,
  args: readonly string[] | undefined,
  options: RxExecFileOptions & { encoding: BufferEncoding },
): Observable<RxProcessEvent<unknown, string>>;
export function rxExecFile(
  file: string,
  args?: readonly string[],
  options?: RxExecFileOptions & { encoding?: null },
): Observable<RxProcessEvent<unknown, Buffer>>;
export function rxExecFile(
  file: string,
  args?: readonly string[],
  options?: RxExecFileOptions,
): Observable<RxProcessEvent<unknown, Buffer | string>>;
export function rxExecFile(
  file: string,
  argsOrOptions?: readonly string[] | RxExecFileOptions,
  options?: RxExecFileOptions,
): Observable<RxProcessEvent<unknown, Buffer | string>> {
  const resolved = splitArgsAndOptions(argsOrOptions, options);
  const resolvedArgs = resolved.args;
  const resolvedOptions = resolved.options;
  const shell = resolvedOptions.shell ?? false;
  return executeRequest<unknown, Buffer | string>({
    command: descriptor(file, resolvedArgs, shell, resolvedOptions.cwd),
    options: toRecord(resolvedOptions),
    launch(nativeOptions) {
      return spawn(file, resolvedArgs, {
        ...nativeOptions,
        shell,
        stdio: "pipe",
      });
    },
    autoEndStdin: true,
    stdinPipeByDefault: true,
  });
}

/** 创建带 IPC 的 Node.js 子进程。 */
export function rxFork<TMessage = unknown>(
  modulePath: string | URL,
  options: RxForkOptions & { encoding: BufferEncoding },
): Observable<RxProcessEvent<TMessage, string>>;
export function rxFork<TMessage = unknown>(
  modulePath: string | URL,
  options?: RxForkOptions & { encoding?: null },
): Observable<RxProcessEvent<TMessage, Buffer>>;
export function rxFork<TMessage = unknown>(
  modulePath: string | URL,
  options?: RxForkOptions,
): Observable<RxProcessEvent<TMessage, Buffer | string>>;
export function rxFork<TMessage = unknown>(
  modulePath: string | URL,
  args: readonly string[] | undefined,
  options: RxForkOptions & { encoding: BufferEncoding },
): Observable<RxProcessEvent<TMessage, string>>;
export function rxFork<TMessage = unknown>(
  modulePath: string | URL,
  args?: readonly string[],
  options?: RxForkOptions & { encoding?: null },
): Observable<RxProcessEvent<TMessage, Buffer>>;
export function rxFork<TMessage = unknown>(
  modulePath: string | URL,
  args?: readonly string[],
  options?: RxForkOptions,
): Observable<RxProcessEvent<TMessage, Buffer | string>>;
export function rxFork<TMessage = unknown>(
  modulePath: string | URL,
  argsOrOptions?: readonly string[] | RxForkOptions,
  options?: RxForkOptions,
): Observable<RxProcessEvent<TMessage, Buffer | string>> {
  const resolved = splitArgsAndOptions(argsOrOptions, options);
  const resolvedArgs = resolved.args;
  const resolvedOptions = resolved.options;
  const silent = resolvedOptions.silent ?? true;
  const moduleName = modulePath.toString();

  return executeRequest<TMessage, Buffer | string>({
    command: descriptor(
      moduleName,
      resolvedArgs,
      false,
      resolvedOptions.cwd,
    ),
    options: toRecord(resolvedOptions),
    launch(nativeOptions) {
      const forkOptions = { ...nativeOptions };
      if (forkOptions.stdio === undefined) {
        forkOptions.silent = silent;
      }
      return fork(modulePath, resolvedArgs, forkOptions);
    },
    autoEndStdin: false,
    stdinPipeByDefault:
      resolvedOptions.stdio === undefined ? silent : true,
  });
}

/** 不经过 shell，以 `node -e` 执行内联代码。 */
export function rxNodeCode(
  code: string,
  options: RxNodeCodeOptions & { encoding: BufferEncoding },
): Observable<RxProcessEvent<unknown, string>>;
export function rxNodeCode(
  code: string,
  options?: RxNodeCodeOptions & { encoding?: null },
): Observable<RxProcessEvent<unknown, Buffer>>;
export function rxNodeCode(
  code: string,
  options?: RxNodeCodeOptions,
): Observable<RxProcessEvent<unknown, Buffer | string>>;
export function rxNodeCode(
  code: string,
  args: readonly string[] | undefined,
  options: RxNodeCodeOptions & { encoding: BufferEncoding },
): Observable<RxProcessEvent<unknown, string>>;
export function rxNodeCode(
  code: string,
  args?: readonly string[],
  options?: RxNodeCodeOptions & { encoding?: null },
): Observable<RxProcessEvent<unknown, Buffer>>;
export function rxNodeCode(
  code: string,
  args?: readonly string[],
  options?: RxNodeCodeOptions,
): Observable<RxProcessEvent<unknown, Buffer | string>>;
export function rxNodeCode(
  code: string,
  argsOrOptions?: readonly string[] | RxNodeCodeOptions,
  options?: RxNodeCodeOptions,
): Observable<RxProcessEvent<unknown, Buffer | string>> {
  const resolved = splitArgsAndOptions(argsOrOptions, options);
  const resolvedArgs = resolved.args;
  const resolvedOptions = resolved.options;
  const nodeArgs = [...(resolvedOptions.nodeArgs ?? [])];
  const allArgs = [...nodeArgs, "-e", code, ...resolvedArgs];

  return executeRequest<unknown, Buffer | string>({
    command: descriptor(
      process.execPath,
      allArgs,
      false,
      resolvedOptions.cwd,
    ),
    options: toRecord(resolvedOptions),
    launch(nativeOptions) {
      return spawn(process.execPath, allArgs, {
        ...nativeOptions,
        shell: false,
      });
    },
    autoEndStdin: false,
    stdinPipeByDefault: true,
  });
}

/** 以 fork + IPC 方式执行 Node.js 模块文件。 */
export function rxNodeModule<TMessage = unknown>(
  modulePath: string | URL,
  options: RxForkOptions & { encoding: BufferEncoding },
): Observable<RxProcessEvent<TMessage, string>>;
export function rxNodeModule<TMessage = unknown>(
  modulePath: string | URL,
  options?: RxForkOptions & { encoding?: null },
): Observable<RxProcessEvent<TMessage, Buffer>>;
export function rxNodeModule<TMessage = unknown>(
  modulePath: string | URL,
  options?: RxForkOptions,
): Observable<RxProcessEvent<TMessage, Buffer | string>>;
export function rxNodeModule<TMessage = unknown>(
  modulePath: string | URL,
  args: readonly string[] | undefined,
  options: RxForkOptions & { encoding: BufferEncoding },
): Observable<RxProcessEvent<TMessage, string>>;
export function rxNodeModule<TMessage = unknown>(
  modulePath: string | URL,
  args?: readonly string[],
  options?: RxForkOptions & { encoding?: null },
): Observable<RxProcessEvent<TMessage, Buffer>>;
export function rxNodeModule<TMessage = unknown>(
  modulePath: string | URL,
  args?: readonly string[],
  options?: RxForkOptions,
): Observable<RxProcessEvent<TMessage, Buffer | string>>;
export function rxNodeModule<TMessage = unknown>(
  modulePath: string | URL,
  argsOrOptions?: readonly string[] | RxForkOptions,
  options?: RxForkOptions,
): Observable<RxProcessEvent<TMessage, Buffer | string>> {
  const resolved = splitArgsAndOptions(argsOrOptions, options);
  return rxFork(modulePath, resolved.args, resolved.options);
}

function executeRequest<
  TMessage = unknown,
  TOutput extends Buffer | string = Buffer,
>(
  request: ExecuteRequest,
): Observable<RxProcessEvent<TMessage, TOutput>> {
  return execute<TMessage, TOutput>(request);
}

function descriptor(
  file: string,
  args: readonly string[],
  shell: boolean | string,
  cwd: string | URL | undefined,
): RxCommandDescriptor {
  return {
    file,
    args,
    shell,
    ...(cwd === undefined ? {} : { cwd: cwd.toString() }),
  };
}

function splitArgsAndOptions<TOptions extends object>(
  argsOrOptions: readonly string[] | TOptions | undefined,
  options: TOptions | undefined,
): { readonly args: string[]; readonly options: TOptions } {
  if (Array.isArray(argsOrOptions)) {
    return {
      args: [...argsOrOptions],
      options: options ?? ({} as TOptions),
    };
  }

  return {
    args: [],
    options:
      (argsOrOptions as TOptions | undefined) ??
      options ??
      ({} as TOptions),
  };
}

function toRecord<T extends RxBaseOptions>(
  options: T,
): RxBaseOptions & Record<string, unknown> {
  return options as unknown as RxBaseOptions & Record<string, unknown>;
}
