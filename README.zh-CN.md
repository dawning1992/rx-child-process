# rx-child-process

[English](./README.md) | 简体中文

一个面向 Node.js `child_process` 的类型化 RxJS 封装。你可以将可执行文件、shell 命令、Node.js 模块和 Node.js 内联代码作为可取消、可观测且带资源约束的子进程运行，并使用统一的失败模型处理结果。

- 运行环境：Node.js 20+
- RxJS：对等依赖 `rxjs@^7.5.5`
- 模块格式：ESM 和 CommonJS
- 包内容：包含 TypeScript 类型声明

## 特性

- 冷 Observable：每次订阅都启动一个独立子进程。
- 统一事件流：覆盖 `spawn`、stdout、stderr、IPC 消息、断开、`exit` 和 `close`。
- 两层 API：低层流式事件 API，以及用于一次性聚合结果的 `rxCollect`。
- 类型化事件、类型守卫，以及随 `encoding` 变化的输出类型。
- 支持通过 `AbortSignal` 或 RxJS 退订取消执行。
- 在 POSIX 和 Windows 上终止进程树。
- 默认安全处理参数：除非显式指定，否则不经过 shell。
- 可控资源：超时、输出字节上限、IPC 消息上限和 stdin 背压。
- stdin 输入支持 string、Buffer、Node.js Readable 和 RxJS Observable。
- 支持按字节数或时间批处理输出。

## 安装

```bash
pnpm add rx-child-process rxjs
```

使用 npm：

```bash
npm install rx-child-process rxjs
```

包同时支持 ESM 和 CommonJS：

```js
const { rxExecFile } = require("rx-child-process");
```

## 快速开始

```ts
import { firstValueFrom } from "rxjs";
import { rxCollect, rxExecFile } from "rx-child-process";

const result = await firstValueFrom(
  rxCollect(rxExecFile("git", ["status", "--short"])),
);

process.stdout.write(result.stdout);
console.error(result.stderr);
console.log(`退出码：${result.code}，耗时：${result.durationMs} ms`);
```

如果不需要等待执行结束，可以直接消费事件流：

```ts
import {
  filterEvent,
  RxEventType,
  rxSpawn,
} from "rx-child-process";

const source = rxSpawn("node", ["worker.mjs"]);

source.pipe(filterEvent(RxEventType.Stdout)).subscribe({
  next(event) {
    process.stdout.write(event.chunk);
  },
  error(error) {
    console.error(error);
  },
  complete() {
    console.log("进程已关闭");
  },
});
```

## 核心概念

### 冷 Observable

所有执行 API 都返回冷 Observable。订阅两次会启动两个进程：

```ts
const source = rxSpawn("node", ["worker.mjs"], { timeoutMs: 5_000 });

source.subscribe(console.log);
source.subscribe(console.log); // 启动第二个进程
```

如果多个消费者需要共享同一个进程，应在分支前共享源：

```ts
import { merge, share } from "rxjs";
import {
  filterEvent,
  RxEventType,
  rxSpawn,
} from "rx-child-process";

const shared = rxSpawn("node", ["worker.mjs"]).pipe(share());

merge(
  shared.pipe(filterEvent(RxEventType.Stdout)),
  shared.pipe(filterEvent(RxEventType.Stderr)),
).subscribe(console.log);
```

`rxCollect` 与其他冷源一样，在自身被订阅时订阅上游。如果多个消费者必须观察同一个真实进程，应先共享低层事件源，再在其上分别聚合或过滤，并确保这些消费者在所需事件发生前完成订阅。

### 参数与 options 重载

所有进程入口都支持显式参数数组，也支持将 options 作为第二个参数：

```ts
rxSpawn("git", ["status", "--short"], options);
rxSpawn("git", options);
```

`rxExecFile`、`rxFork`、`rxNodeCode` 和 `rxNodeModule` 遵循相同规则。传入的参数数组会先被复制，再交给 Node.js。

### 输出类型

默认输出为 `Buffer`：

```ts
const buffered = rxSpawn("node", ["worker.mjs"]);
// Observable<RxProcessEvent<unknown, Buffer>>

const decoded = rxSpawn("node", ["worker.mjs"], { encoding: "utf8" });
// Observable<RxProcessEvent<unknown, string>>
```

`rxFork` 的 IPC 消息可以指定类型：

```ts
type WorkerMessage = { type: "ready" } | { type: "result"; value: number };

const worker = rxFork<WorkerMessage>("worker.mjs", { encoding: "utf8" });
// Observable<RxProcessEvent<WorkerMessage, string>>
```

## 执行 API

所有进程入口都返回 `Observable<RxProcessEvent<TMessage, TOutput>>`。

| API | 用途 | 默认经过 shell | stdin pipe | 未配置 `input` 时自动关闭 stdin |
| --- | --- | --- | --- | --- |
| `rxSpawn(command, args?, options?)` | 使用 argv 数组启动可执行文件。 | 否 | 遵循 Node.js 默认值 | 否 |
| `rxExec(command, options?)` | 执行 shell 命令字符串。 | 是 | 强制为 pipe | 是 |
| `rxExecFile(file, args?, options?)` | 使用 argv 数组启动文件。 | 否 | 强制为 pipe | 是 |
| `rxFork(modulePath, args?, options?)` | 启动带 IPC 通道的 Node.js 模块。 | 否 | 启用 `silent` 时为 pipe | 否 |
| `rxNodeCode(code, args?, options?)` | 通过 `node -e` 执行内联代码。 | 否 | 遵循 Node.js 默认值 | 否 |
| `rxNodeModule(modulePath, args?, options?)` | 语义化的 `rxFork` 别名。 | 否 | 启用 `silent` 时为 pipe | 否 |

所有 API 也支持将 options 作为第二个参数。例如，`rxExecFile(file, options)` 等价于 `rxExecFile(file, [], options)`。

### `rxSpawn`

```ts
import { rxSpawn } from "rx-child-process";

const source = rxSpawn(
  "ffprobe",
  ["-v", "error", "-show_format", "input.mp4"],
  {
    cwd: "/data/media",
    timeoutMs: 10_000,
  },
);
```

`rxSpawn` 将 `command` 和 `args` 直接传给 Node.js `spawn`。除非显式设置 `shell`，否则不经过 shell。使用 Node.js 默认 `stdio` 时，stdin 是可写 pipe；如果既没有配置 `input`，也没有配置 `stdio: "ignore"`，stdin 会保持打开。若程序等待 EOF，可在 `spawn` 事件中调用 `child.stdin?.end()`。

### `rxExec`

```ts
import { rxExec } from "rx-child-process";

const source = rxExec("printf '%s\n' \"$HOME\"", {
  shell: "/bin/bash",
  timeoutMs: 5_000,
});
```

`rxExec` 接收完整 shell 命令字符串，默认启用 shell，并强制使用 `stdio: "pipe"`。只有完整命令字符串可信时才应使用它。

### `rxExecFile`

```ts
import { rxExecFile } from "rx-child-process";

const source = rxExecFile(
  "convert",
  [untrustedInputPath, "output.png"],
  { timeoutMs: 30_000 },
);
```

`rxExecFile` 使用 argv 数组，默认不调用 shell，并强制使用 `stdio: "pipe"`。任一参数来自不可信输入时，应优先使用此 API。

### `rxFork`

```ts
import { filterEvent, rxFork, RxEventType } from "rx-child-process";

const worker = rxFork<{ type: "ready" }>("worker.mjs");

worker.pipe(filterEvent(RxEventType.Message)).subscribe(({ message }) => {
  console.log(message);
});
```

`rxFork` 创建 Node.js IPC 通道。`silent` 默认为 `true`，因此会捕获 stdout 和 stderr。如果显式配置了 `stdio`，该配置会透传给 Node.js。未提供 `input` 时，低层事件源不会自动关闭 stdin。

发送消息或句柄时，可从 `spawn` 事件取得原始子进程：

```ts
worker.subscribe((event) => {
  if (event.type === "spawn") {
    event.child.send({ command: "start" });
  }
});
```

### `rxNodeCode`

```ts
import { rxNodeCode } from "rx-child-process";

const source = rxNodeCode(
  "console.log(JSON.stringify({ argv: process.argv.slice(1) }))",
  ["first", "second"],
  {
    nodeArgs: ["--no-warnings"],
    encoding: "utf8",
  },
);
```

`rxNodeCode` 使用 `process.execPath`，参数顺序为：

```text
[...nodeArgs, "-e", code, ...args]
```

它不会启用 shell。`nodeArgs` 只用于向 Node.js 可执行文件传递参数，不是绕过 shell 限制的手段。

### `rxNodeModule`

```ts
import { firstValueFrom } from "rxjs";
import { rxCollect, rxNodeModule } from "rx-child-process";

const result = await firstValueFrom(
  rxCollect(rxNodeModule("./worker.mjs", ["--mode", "batch"])),
);
```

`rxNodeModule` 是语义更明确的 `rxFork` 别名，运行时行为完全一致，也支持 IPC。

### `rxCollect`

```ts
import { firstValueFrom } from "rxjs";
import { rxCollect, rxExecFile } from "rx-child-process";

const result = await firstValueFrom(
  rxCollect(rxExecFile("node", ["--version"]), {
    failOnNonZeroExit: true,
    failOnSignal: true,
    maxMessages: 1_000,
  }),
);
```

`rxCollect(source, options?)` 订阅低层事件流，只发射一个 `RxCommandResult`，随后完成。

| 选项 | 默认值 | 说明 |
| --- | --- | --- |
| `failOnNonZeroExit` | `true` | `code` 为非零数字时进入 error 通道。 |
| `failOnSignal` | `true` | 进程因信号退出时进入 error 通道。 |
| `maxMessages` | `1000` | 最多保留的 IPC 消息数量。`0` 或 `RxLimits.Disabled` 关闭限制。 |

`RxCommandResult.messages` 只保存消息负载。可选的 `sendHandle` 仍会出现在低层 `message` 事件中，但 `rxCollect` 不会保留它。超过 `maxMessages` 时会停止订阅上游，同时请求终止底层进程。

返回结果会继承上游输出类型：

```ts
import { firstValueFrom } from "rxjs";

const text = await firstValueFrom(
  rxCollect(rxSpawn("node", ["worker.mjs"], { encoding: "utf8" })),
);
// text.stdout 和 text.stderr 都是 string
```

## 选项

### 受管选项

以下选项由 `rx-child-process` 实现，所有进程入口共享。

| 选项 | 默认值 | 说明 |
| --- | --- | --- |
| `signal` | 无 | 用于取消执行的 `AbortSignal`。 |
| `timeoutMs` | `30000` | 执行超时，单位毫秒。`0` 或 `RxLimits.Disabled` 关闭。 |
| `killGraceMs` | `2000` | 优雅终止与强制终止之间的等待时间。`Infinity` 关闭强制升级。 |
| `killTree` | `true` | 终止整个进程树，而不是只终止直接子进程。 |
| `maxStdoutBytes` | `10 MiB` | 解码前允许接收的 stdout 最大字节数。`0` 或 `RxLimits.Disabled` 关闭。 |
| `maxStderrBytes` | `10 MiB` | 解码前允许接收的 stderr 最大字节数。`0` 或 `RxLimits.Disabled` 关闭。 |
| `input` | 无 | 写入 stdin 的 string、Buffer、Readable 或 Observable。 |
| `maxStdinQueueBytes` | `1 MiB` | stdin 背压期间最多积压的 Observable 输入字节数。`0` 或 `RxLimits.Disabled` 关闭。 |
| `encoding` | `null` | 输出编码。`null` 发射 Buffer，合法的 `BufferEncoding` 发射 string。 |
| `batching` | `false` | 按通道批处理输出。`false` 保持 Node.js stream 原始数据块。 |
| `failOnNonZeroExit` | `false` | 低层模式：非零退出时先发射 `close`，再进入 error 通道。 |
| `failOnSignal` | `false` | 低层模式：信号退出时先发射 `close`，再进入 error 通道。 |

`rxNodeCode` 额外支持：

| 选项 | 默认值 | 说明 |
| --- | --- | --- |
| `nodeArgs` | `[]` | 位于 `-e` 和内联代码之前的 Node.js 参数。 |

### 批处理

```ts
const source = rxSpawn("server", ["--verbose"], {
  batching: {
    maxBytes: 64 * 1024,
    flushIntervalMs: 10,
  },
});
```

| 字段 | 默认值 | 说明 |
| --- | --- | --- |
| `maxBytes` | `64 KiB` | 通道累计原始字节达到该值时刷新。`0` 或 `Infinity` 关闭字节阈值。 |
| `flushIntervalMs` | `10` | 经过该时间后刷新。`0` 或 `Infinity` 关闭时间刷新。 |

批处理按 stdout 和 stderr 独立进行。`close` 前会刷新缓冲区，包括解码器尾部。未启用批处理时，每个非空数据块都会立即发射。

### `RxLimits`

```ts
import { RxLimits, rxSpawn } from "rx-child-process";

rxSpawn("long-running-task", {
  timeoutMs: RxLimits.Disabled,
  maxStdoutBytes: RxLimits.Disabled,
});
```

`RxLimits.Disabled` 的值是 `Number.POSITIVE_INFINITY`。对于超时和输出字节上限，`0` 也具有相同的关闭效果。

### Node.js 原生选项

选项类型继承对应的 Node.js 类型：`SpawnOptions`、`ExecOptions`、`ExecFileOptions` 和 `ForkOptions`。所选入口支持的原生字段，例如 `cwd`、`env`、`stdio`、`windowsHide`、`uid`、`gid`、`serialization` 和 `argv0`，会继续透传。

以下字段由本库接管：

| 受管字段 | 替代方式 |
| --- | --- |
| `signal` | 使用本库的 `signal`。 |
| `timeout` | 使用 `timeoutMs`。 |
| `killSignal` | 使用本库的进程树终止策略。 |
| `detached` | 根据 `killTree` 和平台推导。 |
| `maxBuffer` | 使用 `maxStdoutBytes` 和 `maxStderrBytes`。 |
| `encoding` | 使用本库的 `encoding`。 |

这些字段会在启动 Node.js 进程前被移除，不能覆盖本库行为。

其他透传说明：

- `windowsHide` 默认是 `true`。
- `rxExec` 和 `rxExecFile` 强制 `stdio: "pipe"`。
- 未配置 `stdio` 时，`rxFork` 默认使用 `silent: true`。
- POSIX 上会强制将 `detached` 与 `killTree` 对齐；Windows 上强制 `detached` 为 `false`。

### stdin 行为

`input` 支持：

```ts
type RxProcessInput =
  | string
  | Buffer
  | Readable
  | Observable<string | Buffer>;
```

各类输入的行为：

| 输入 | 行为 |
| --- | --- |
| string 或 Buffer | 写入 stdin，随后关闭 stdin。 |
| Node.js `Readable` | 管道写入 stdin；Readable 结束时关闭 stdin。 |
| RxJS `Observable` | 带背压写入，等待 `drain`，在输入完成且队列清空后关闭 stdin。 |

如果 Observable 永不完成，stdin 会保持打开。子进程不读取输入时，应配置 `stdio: "ignore"` 作为 stdin。配置 `input` 时 stdin 必须是 pipe；不兼容的 `stdio` 会在启动前以 `configuration` 错误失败。

示例：

```ts
import { firstValueFrom, of } from "rxjs";
import { rxCollect, rxSpawn } from "rx-child-process";

const result = await firstValueFrom(
  rxCollect(
    rxSpawn("cat", [], {
      input: of("first line\n", "second line\n"),
      maxStdinQueueBytes: 1024 * 1024,
      encoding: "utf8",
    }),
  ),
);

console.log(result.stdout);
```

## 事件

```ts
type RxProcessEvent<TMessage = unknown, TOutput extends Buffer | string = Buffer> =
  | {
      type: "spawn";
      pid: number;
      child: ChildProcess;
      command: RxCommandDescriptor;
      encoding: BufferEncoding | null;
    }
  | { type: "stdout"; chunk: TOutput }
  | { type: "stderr"; chunk: TOutput }
  | {
      type: "message";
      message: TMessage;
      sendHandle?: RxSendHandle;
    }
  | { type: "disconnect" }
  | {
      type: "exit";
      code: number | null;
      signal: NodeJS.Signals | null;
    }
  | {
      type: "close";
      code: number | null;
      signal: NodeJS.Signals | null;
      durationMs: number;
    };
```

### 事件顺序

正常启动后的顺序为：

```text
spawn -> stdout/stderr/message/disconnect 事件 -> exit -> close
```

- stdout 和 stderr 各自保持通道内的数据顺序。
- 不同通道之间的顺序只保证本库实际观察到事件的顺序，不额外承诺跨流同步。
- 仅在存在 IPC 通道时发射 `message` 和 `disconnect`。
- `exit` 表示进程已经退出；`close` 表示进程及其标准流已经关闭。
- `close` 是最后一个 `next` 事件，并且只发射一次。
- `durationMs` 从订阅中的执行设置阶段开始，到 `close` 结束。

### 事件常量与类型守卫

```ts
import {
  isCloseEvent,
  isDisconnectEvent,
  isExitEvent,
  isMessageEvent,
  isOutputEvent,
  isRxEventType,
  isSpawnEvent,
  isStderrEvent,
  isStdoutEvent,
  RxEventType,
} from "rx-child-process";

event.type === RxEventType.Stdout;
isStdoutEvent(event);
isOutputEvent(event);
```

`RxEventType` 提供 `Spawn`、`Stdout`、`Stderr`、`Message`、`Disconnect`、`Exit` 和 `Close`。

`filterEvent(type)` 筛选一种事件并收窄类型：

```ts
import { filterEvent, RxEventType, rxSpawn } from "rx-child-process";

rxSpawn("node", ["worker.mjs"])
  .pipe(filterEvent(RxEventType.Stderr))
  .subscribe(({ chunk }) => process.stderr.write(chunk));
```

用于提取事件类型的辅助类型是 `RxEventOfType<TMessage, TOutput, TType>`。

## 结果与错误

### 成功结果

```ts
interface RxCommandDescriptor {
  readonly file: string;
  readonly args: readonly string[];
  readonly shell: boolean | string;
  readonly cwd?: string;
}

interface RxCommandResult<
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
```

`command` 包含 `file`、`args`、`shell` 和可选的 `cwd`。进程未启动，或上游没有提供 PID 时，`pid` 不存在。因信号退出时，`code` 为 `null`，`signal` 为非空值。

### `RxChildProcessError`

所有可预期失败都使用 `RxChildProcessError`：

```ts
class RxChildProcessError extends Error {
  readonly name: "RxChildProcessError";
  readonly kind: RxChildProcessErrorKind;
  readonly command?: RxCommandDescriptor;
  readonly result?: RxErrorResult;
  readonly cause?: unknown;
}

interface RxChildProcessErrorOptions {
  readonly kind: RxChildProcessErrorKind;
  readonly command?: RxCommandDescriptor;
  readonly result?: RxErrorResult;
  readonly cause?: unknown;
}

interface RxErrorResult {
  readonly command: RxCommandDescriptor;
  readonly pid?: number;
  readonly stdout: Buffer;
  readonly stderr: Buffer;
  readonly code: number | null;
  readonly signal: NodeJS.Signals | null;
  readonly durationMs: number;
}
```

即使成功输出使用 string 编码，错误诊断中的 stdout 和 stderr 也始终是 Buffer。低层诊断快照对每个输出通道最多保留前 1 MiB。

```ts
import { firstValueFrom } from "rxjs";
import {
  isRxChildProcessError,
  RxErrorKind,
  rxCollect,
  rxExecFile,
} from "rx-child-process";

try {
  await firstValueFrom(rxCollect(rxExecFile("missing-command")));
} catch (error) {
  if (isRxChildProcessError(error, RxErrorKind.Spawn)) {
    console.error(error.command, error.result?.stderr.toString());
  }
}
```

### 错误分类

| 分类 | 含义 |
| --- | --- |
| `configuration` | 选项无效、`stdio` 不兼容、`maxMessages` 无效或编码不受支持。 |
| `spawn` | 可执行文件或模块无法启动。 |
| `runtime` | 启动后发生子进程错误，或 `rxCollect` 收到未类型化的上游错误。 |
| `abort` | `AbortSignal` 取消了执行。 |
| `timeout` | 执行超过 `timeoutMs`。 |
| `output-limit` | stdout 或 stderr 超过配置的字节上限。 |
| `message-limit` | `rxCollect` 保留的 IPC 消息超过 `maxMessages`。 |
| `stdin` | 读取、写入或配置 stdin 失败。 |
| `exit` | 进程以非零退出码结束，且启用了退出失败策略。 |
| `signal` | 进程因信号结束，且启用了信号失败策略。 |

使用 `isChildProcessErrorKind(value)` 校验错误分类字符串，使用 `isRxChildProcessError(error, kind?)` 校验库错误及其可选分类。后者的判断不依赖原型链，因此也适用于依赖重复安装以及 ESM/CJS 混合加载。

### 失败顺序与聚合

- 未启用 `failOnNonZeroExit` 或 `failOnSignal` 时，低层 API 将非零退出和信号退出视为正常完成。
- 任一低层失败开关命中时，源会先发射 `close`，随后调用 `error`。
- `rxCollect` 默认将非零退出码和信号退出转为错误。
- 低层源已经发射的错误会由 `rxCollect` 原样传播。
- 错误携带部分执行结果时，通过 `error.result` 获取诊断信息；`rxCollect` 不会在错误前额外发射结果。
- abort、timeout、output-limit、spawn、runtime 和 stdin 等运行期失败会请求终止子进程；启动前识别出的 configuration 错误不会启动进程。

## 取消与进程树

使用 `AbortSignal`：

```ts
import { firstValueFrom } from "rxjs";
import {
  isRxChildProcessError,
  RxErrorKind,
  rxCollect,
  rxSpawn,
} from "rx-child-process";

const controller = new AbortController();
setTimeout(() => controller.abort(new Error("由调用方取消")), 1_000);

try {
  await firstValueFrom(
    rxCollect(
      rxSpawn("long-running-command", [], {
        signal: controller.signal,
      }),
    ),
  );
} catch (error) {
  if (isRxChildProcessError(error, RxErrorKind.Abort)) {
    console.log("已取消");
  }
}
```

也可以直接退订：

```ts
const subscription = rxSpawn("long-running-command").subscribe(console.log);

subscription.unsubscribe();
// 已请求终止进程树；不会发送 error 或 complete 通知。
```

取消语义：

- 传入已经中止的 `AbortSignal` 时，不会启动进程，而是以 `abort` 失败。
- 执行中触发中止会请求终止进程，并报告 `abort` 错误。
- 错误可能在操作系统完全回收所有已终止进程之前报告。
- RxJS 退订不会进入 error 或 complete 通道，但会请求终止进程树。

终止策略：

| 平台 | `killTree: true` | `killTree: false` |
| --- | --- | --- |
| POSIX | 创建独立进程组，向进程组发送 SIGTERM，等待 `killGraceMs` 后发送 SIGKILL。 | 向直接子进程发送 SIGTERM，仍在运行时于 `killGraceMs` 后发送 SIGKILL。 |
| Windows | 使用 `taskkill /PID <pid> /T /F`。 | 向直接子进程发送 SIGTERM。 |

设置 `killGraceMs: Infinity` 会关闭强制升级，可能导致无响应进程永久残留。Windows 的进程树路径会直接执行 `taskkill /F`，不存在与 POSIX 对应的优雅终止阶段。

POSIX 进程树模式会让子进程运行在独立进程组中，从而改变 session 和 TTY 语义。依赖终端控制的交互式程序可能需要设置 `killTree: false`。

## 安全边界

`rxSpawn`、`rxExecFile`、`rxFork`、`rxNodeCode` 和 `rxNodeModule` 默认不经过 shell。参数通过 argv 传递，因此 `;`、`|`、`$()`、反引号等元字符不会被 shell 解释。

```ts
// 推荐：不可信参数使用 argv。
rxExecFile("convert", [untrustedInput, "output.png"]);

// 危险：完整命令字符串会被 shell 解释。
rxExec(`convert ${untrustedInput} output.png`);
```

安全边界：

- `rxExec` 只应执行可信的完整命令字符串。
- `rxNodeCode` 和 `rxNodeModule` 会执行任意 Node.js 代码。
- 本库提供进程监管，不是文件系统、网络、操作系统用户或权限沙箱。
- 不经过 shell 只避免 shell 插值，不代表被调用的可执行文件本身安全。

## 输出、内存与性能

- 未启用批处理时，数据按 Node.js 提供的节奏发射，不增加固定延迟。
- 高频小块写入可通过 batching 减少 Observable 通知次数。
- 字节上限在字符串解码前按原始字节计算。
- `StringDecoder` 能正确处理跨数据块的多字节字符，并在关闭时刷新不完整序列。
- `rxCollect` 在内存中累计 stdout 和 stderr，结束时只构建一次最终 Buffer 或 string。
- 低层执行始终为每个通道保留最多 1 MiB 的错误诊断快照。
- `rxCollect` 会保留全部 IPC 消息，直到达到上限、构造结果或失败。

长时间日志流需要提高或关闭输出上限：

```ts
import { RxLimits, rxSpawn } from "rx-child-process";

const source = rxSpawn("server", ["--verbose"], {
  maxStdoutBytes: 100 * 1024 * 1024,
  maxStderrBytes: RxLimits.Disabled,
});
```

输出非常大时应优先流式消费，并在订阅者中处理背压，而不是使用 `rxCollect`。

## 类型参考

### 公开类型

| 类型 | 说明 |
| --- | --- |
| `RxBaseOptions` | 所有入口共享的受管选项。 |
| `RxBatchOptions` | `maxBytes` 和 `flushIntervalMs` 批处理控制。 |
| `RxProcessInput` | 可接受的 stdin 输入联合类型。 |
| `RxSpawnOptions` | Node.js spawn 选项加受管选项。 |
| `RxExecOptions` | Node.js exec 选项加受管选项。 |
| `RxExecFileOptions` | Node.js execFile 选项加受管选项。 |
| `RxForkOptions` | Node.js fork 选项加受管选项。 |
| `RxNodeCodeOptions` | 不含 `shell` 的 spawn 选项，并增加 `nodeArgs`。 |
| `RxCollectOptions` | 聚合失败策略和消息上限。 |
| `RxCommandDescriptor` | 观察到的命令元数据。 |
| `RxSpawnEvent` | 启动成功事件。 |
| `RxStdoutEvent<TOutput>` | stdout 数据块事件。 |
| `RxStderrEvent<TOutput>` | stderr 数据块事件。 |
| `RxOutputEvent<TOutput>` | stdout 或 stderr 事件联合。 |
| `RxMessageEvent<TMessage>` | fork IPC 消息事件。 |
| `RxSendHandle` | `net.Socket` 或 `net.Server` IPC 句柄。 |
| `RxDisconnectEvent` | IPC 通道断开事件。 |
| `RxExitEvent` | 进程退出事件。 |
| `RxCloseEvent` | 进程及标准流关闭事件。 |
| `RxProcessEvent<TMessage, TOutput>` | 完整事件联合类型。 |
| `RxCommandResult<TMessage, TOutput>` | 聚合后的成功结果。 |
| `RxEventOfType<TMessage, TOutput, TType>` | 按事件名收窄后的事件类型。 |
| `RxChildProcessErrorKind` | 错误分类联合类型。 |
| `RxChildProcessErrorOptions` | 错误构造参数。 |
| `RxErrorResult` | 错误附带的部分执行快照。 |

### 运行时导出

| 导出 | 说明 |
| --- | --- |
| `rxSpawn` | 使用 argv 启动可执行文件。 |
| `rxExec` | 执行 shell 命令。 |
| `rxExecFile` | 使用 argv 启动文件。 |
| `rxFork` | 启动带 IPC 的 Node.js 模块。 |
| `rxNodeCode` | 执行 Node.js 内联代码。 |
| `rxNodeModule` | `rxFork` 的别名。 |
| `rxCollect` | 聚合事件流。 |
| `filterEvent` | 筛选并收窄进程事件。 |
| `isSpawnEvent` | 判断是否为 spawn 事件。 |
| `isStdoutEvent` | 判断是否为 stdout 事件。 |
| `isStderrEvent` | 判断是否为 stderr 事件。 |
| `isOutputEvent` | 判断是否为 stdout 或 stderr 事件。 |
| `isMessageEvent` | 判断是否为 IPC 消息。 |
| `isDisconnectEvent` | 判断是否为 IPC 断开。 |
| `isExitEvent` | 判断是否为进程退出。 |
| `isCloseEvent` | 判断是否为进程关闭。 |
| `isRxEventType` | 校验事件名称。 |
| `isChildProcessErrorKind` | 校验错误分类。 |
| `isRxChildProcessError` | 校验类型化错误及可选的错误分类。 |
| `RxEventType` | 事件名称常量及类型。 |
| `RxErrorKind` | 错误分类常量。 |
| `RxLimits` | `Disabled` 资源上限值。 |
| `RxChildProcessError` | 类型化执行错误类。 |

## 示例

```bash
pnpm demo:basic
pnpm demo:stream
pnpm demo:cancel
pnpm demo:node
pnpm demo:ipc
```

## 开发

```bash
pnpm install
pnpm typecheck
pnpm test
pnpm test:coverage
pnpm build
pnpm bench
```

## 许可证

[MIT](./LICENSE)
