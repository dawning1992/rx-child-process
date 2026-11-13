import type {
  ChildProcess,
  ExecFileOptions as NodeExecFileOptions,
  ExecOptions as NodeExecOptions,
  ForkOptions as NodeForkOptions,
  SpawnOptions as NodeSpawnOptions,
} from "node:child_process";
import type { Readable } from "node:stream";
import type { Observable } from "rxjs";

/** 子进程可写入的标准输入形式。 */
export type RxProcessInput =
  | string
  | Buffer
  | Readable
  | Observable<string | Buffer>;

/** 输出批处理约束；达到任一阈值时刷新当前通道的数据。 */
export interface RxBatchOptions {
  /** 单个批次允许累计的最大原始字节数。 */
  maxBytes?: number;
  /** 批次最长等待时间，单位毫秒。 */
  flushIntervalMs?: number;
}

/**
 * 所有执行入口共享的受监管选项。
 *
 * 资源上限使用 `0` 或 `Infinity` 表示关闭；超时和终止宽限必须是非负值。
 */
export interface RxBaseOptions {
  /** 中止执行；中止后触发进程树回收并通过 error 通道报告。 */
  signal?: AbortSignal;
  /** 执行超时，单位毫秒。 */
  timeoutMs?: number;
  /** SIGTERM 与 SIGKILL 之间的等待时间，单位毫秒。 */
  killGraceMs?: number;
  /** 是否终止整个进程树。 */
  killTree?: boolean;
  /** stdout 总量上限，单位字节。 */
  maxStdoutBytes?: number;
  /** stderr 总量上限，单位字节。 */
  maxStderrBytes?: number;
  /** 自动写入子进程 stdin 的数据。 */
  input?: RxProcessInput;
  /** Observable 输入在等待 drain 时允许积压的最大字节数。 */
  maxStdinQueueBytes?: number;
  /** 对外数据块的编码；为 null 时输出 Buffer。 */
  encoding?: BufferEncoding | null;
  /** stdout/stderr 批处理；false 表示逐块发射。 */
  batching?: false | RxBatchOptions;
  /** 是否在非零退出后通过 error 通道结束。 */
  failOnNonZeroExit?: boolean;
  /** 是否在异常信号退出后通过 error 通道结束。 */
  failOnSignal?: boolean;
}

type RemovedNativeOption =
  | "signal"
  | "timeout"
  | "killSignal"
  | "detached"
  | "maxBuffer"
  | "encoding";

/** spawn 执行选项。 */
export type RxSpawnOptions = Omit<NodeSpawnOptions, RemovedNativeOption> &
  RxBaseOptions;

/** shell 命令执行选项；默认启用 shell。 */
export type RxExecOptions = Omit<NodeExecOptions, RemovedNativeOption> &
  RxBaseOptions;

/** 可执行文件执行选项；默认不启用 shell。 */
export type RxExecFileOptions = Omit<
  NodeExecFileOptions,
  RemovedNativeOption
> &
  RxBaseOptions;

/** fork 执行选项。 */
export type RxForkOptions = Omit<NodeForkOptions, RemovedNativeOption> &
  RxBaseOptions;

/** Node.js 内联代码执行选项。 */
export type RxNodeCodeOptions = Omit<RxSpawnOptions, "shell"> & {
  /** 传给 Node.js 可执行文件、位于 `-e` 之前的参数。 */
  nodeArgs?: readonly string[];
};

/** 由库补充到事件中的命令元数据。 */
export interface RxCommandDescriptor {
  /** 可执行文件、shell 命令或 Node.js 模块路径。 */
  readonly file: string;
  /** 参数列表。 */
  readonly args: readonly string[];
  /** shell 使用情况或具体 shell 可执行文件。 */
  readonly shell: boolean | string;
  /** 工作目录。 */
  readonly cwd?: string;
}

/** 子进程成功创建后发射的生命周期事件。 */
export interface RxSpawnEvent {
  readonly type: "spawn";
  /** 子进程 PID；极端平台限制下可能不可用。 */
  readonly pid?: number;
  /** 原始 Node.js 子进程句柄，用于高级控制和 IPC。 */
  readonly child: ChildProcess;
  /** 当前执行的命令描述。 */
  readonly command: RxCommandDescriptor;
  /** 当前输出编码；null 表示 Buffer。 */
  readonly encoding: BufferEncoding | null;
}

/** stdout 或 stderr 数据事件。 */
export interface RxOutputEvent<TOutput extends Buffer | string = Buffer> {
  readonly type: "stdout" | "stderr";
  readonly chunk: TOutput;
}

/** fork IPC 消息事件。 */
export interface RxMessageEvent<TMessage = unknown> {
  readonly type: "message";
  readonly message: TMessage;
  readonly sendHandle?: unknown;
}

/** fork IPC 通道断开事件。 */
export interface RxDisconnectEvent {
  readonly type: "disconnect";
}

/** 子进程退出事件。 */
export interface RxExitEvent {
  readonly type: "exit";
  readonly code: number | null;
  readonly signal: NodeJS.Signals | null;
}

/** 子进程及其标准流关闭事件。 */
export interface RxCloseEvent {
  readonly type: "close";
  readonly code: number | null;
  readonly signal: NodeJS.Signals | null;
  readonly durationMs: number;
}

/** 所有执行 API 发射的判别事件。 */
export type RxProcessEvent<
  TMessage = unknown,
  TOutput extends Buffer | string = Buffer,
> =
  | RxSpawnEvent
  | RxOutputEvent<TOutput>
  | RxMessageEvent<TMessage>
  | RxDisconnectEvent
  | RxExitEvent
  | RxCloseEvent;

/** 聚合执行结果。 */
export interface RxCommandResult<
  TMessage = unknown,
  TOutput extends Buffer | string = Buffer,
> {
  readonly command: RxCommandDescriptor;
  readonly pid?: number;
  readonly stdout: TOutput;
  readonly stderr: TOutput;
  readonly code: number | null;
  readonly signal: NodeJS.Signals | null;
  readonly messages: readonly TMessage[];
  readonly durationMs: number;
}

/** `rxCollect` 的完成语义和消息保留限制。 */
export interface RxCollectOptions {
  /** 非零退出时是否进入 error 通道，默认 true。 */
  failOnNonZeroExit?: boolean;
  /** 异常信号退出时是否进入 error 通道，默认 true。 */
  failOnSignal?: boolean;
  /** 最多保留的 IPC 消息数量，默认 1000。 */
  maxMessages?: number;
}
