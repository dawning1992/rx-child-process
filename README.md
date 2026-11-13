# rx-child-process

基于 RxJS 的 Node.js `child_process` 响应式工具库。用于安全、可取消、可观测地执行外部命令、Node.js 内联代码和 Node.js 模块。

## 特性

- 冷 Observable：每次订阅独立启动子进程。
- 统一事件流：实时观察 stdout、stderr、IPC 消息和完整生命周期。
- 两层 API：低层 `rxSpawn` 等用于流式消费，`rxCollect` 用于一次性聚合。
- 安全默认值：默认不经过 shell、30 秒超时、stdout/stderr 各 10 MiB、进程树终止。
- 可靠取消：支持 RxJS 退订和 `AbortSignal`，POSIX 使用 SIGTERM 升级 SIGKILL。
- stdin 与 IPC：支持 string、Buffer、Readable、Observable 输入，并暴露原始 `ChildProcess`。
- 性能控制：默认逐块输出，可按字节或时间批处理；聚合时不执行重复字符串拼接。

## 安装

```bash
pnpm add rx-child-process rxjs
```

需要 Node.js 20 或更高版本。

## 快速开始

```ts
import { firstValueFrom } from "rxjs";
import { rxCollect, rxExecFile } from "rx-child-process";

const result = await firstValueFrom(
  rxCollect(rxExecFile("git", ["status", "--short"])),
);

console.log(result.stdout.toString());
```

低层 API 返回 `Observable<RxProcessEvent>`：

```ts
import { rxSpawn } from "rx-child-process";

rxSpawn("node", ["worker.mjs"]).subscribe({
  next(event) {
    switch (event.type) {
      case "spawn":
        console.log("PID", event.pid);
        event.child.send?.({ type: "start" });
        break;
      case "stdout":
      case "stderr":
        process[event.type].write(event.chunk);
        break;
      case "close":
        console.log("退出码", event.code, "信号", event.signal);
        break;
    }
  },
  error(error) {
    console.error(error);
  },
});
```

## 安全边界

`rxSpawn`、`rxExecFile`、`rxFork`、`rxNodeCode` 和 `rxNodeModule` 默认不经过 shell。位置参数通过 argv 传递，即使包含 `;`、`$()` 等字符也不会被 shell 解释。

```ts
// 推荐：不可信参数使用 argv。
rxExecFile("convert", [untrustedInput, "output.png"]);

// 避免：不要将不可信输入拼进 shell 字符串。
rxExec(`convert ${untrustedInput} output.png`);
```

`rxExec` 的设计目的是执行 shell 命令，调用方必须自行保证命令字符串可信。`rxNodeCode` 和 `rxNodeModule` 执行的是任意 Node.js 代码，库不提供文件系统、网络或权限沙箱。

## API

```ts
rxSpawn(command, args?, options?)
rxExec(command, options?)
rxExecFile(file, args?, options?)
rxFork(modulePath, args?, options?)
rxNodeCode(code, args?, options?)
rxNodeModule(modulePath, args?, options?)
rxCollect(source, options?)
```

### 事件

```ts
type RxProcessEvent<TMessage = unknown, TOutput extends Buffer | string = Buffer> =
  | { type: "spawn"; pid?: number; child: ChildProcess; command; encoding }
  | { type: "stdout"; chunk: TOutput }
  | { type: "stderr"; chunk: TOutput }
  | { type: "message"; message: TMessage; sendHandle?: unknown }
  | { type: "disconnect" }
  | { type: "exit"; code: number | null; signal: NodeJS.Signals | null }
  | {
      type: "close";
      code: number | null;
      signal: NodeJS.Signals | null;
      durationMs: number;
    };
```

stdout、stderr 和消息按实际到达顺序交错发射。正常启动后依次产生数据、`exit` 和 `close` 事件。

### 选项

| 选项 | 默认值 | 说明 |
| --- | --- | --- |
| `signal` | 无 | 通过 AbortSignal 取消执行 |
| `timeoutMs` | `30000` | 执行超时；`0` 或 `Infinity` 关闭 |
| `killGraceMs` | `2000` | SIGTERM 到 SIGKILL 的等待时间 |
| `killTree` | `true` | 是否终止整个进程树 |
| `maxStdoutBytes` | `10 MiB` | stdout 总量上限；`0` 或 `Infinity` 关闭 |
| `maxStderrBytes` | `10 MiB` | stderr 总量上限；`0` 或 `Infinity` 关闭 |
| `input` | 无 | string、Buffer、Readable 或 Observable |
| `maxStdinQueueBytes` | `1 MiB` | Observable 输入等待 drain 时的积压上限 |
| `encoding` | `null` | 输出编码；null 发射 Buffer |
| `batching` | `false` | `{ maxBytes?, flushIntervalMs? }` 批处理 |
| `failOnNonZeroExit` | `false` | 低层是否将非零退出转入 error 通道 |
| `failOnSignal` | `false` | 低层是否将信号退出转入 error 通道 |
| `maxMessages` | `1000` | `rxCollect` 最多保留的 IPC 消息数 |

`cwd`、`env`、`stdio`、`windowsHide` 等 Node 原生选项会继续透传。`timeout`、`signal`、`killSignal`、`detached`、`maxBuffer` 和 `encoding` 由本库统一管理。

`rxFork` 默认启用 `silent: true` 以捕获 stdout 和 stderr；显式配置 `stdio` 时遵循 Node.js 的流类型。

## 错误

所有可预期失败使用 `RxChildProcessError`：

```ts
import { firstValueFrom } from "rxjs";
import { RxChildProcessError } from "rx-child-process";

try {
  await firstValueFrom(rxCollect(rxExecFile("missing-command")));
} catch (error) {
  if (error instanceof RxChildProcessError) {
    console.log(error.kind, error.command, error.result?.stderr);
  }
}
```

`kind` 可为 `configuration`、`spawn`、`runtime`、`abort`、`timeout`、`output-limit`、`message-limit`、`stdin`、`exit` 或 `signal`。

低层失败开关默认关闭，因此非零退出仍会完整发射 `exit` 和 `close`。开启对应开关后，`close` 会先发射，随后进入 error 通道。`rxCollect` 默认把非零退出和异常信号视为失败。

## 取消和进程树

```ts
import { rxCollect, rxSpawn } from "rx-child-process";

const controller = new AbortController();
setTimeout(() => controller.abort(), 1_000);

rxCollect(
  rxSpawn("long-running-command", [], {
    signal: controller.signal,
  }),
).subscribe();
```

POSIX 默认创建独立进程组，先向整个进程组发送 SIGTERM，宽限后发送 SIGKILL，因此可以清理孙进程。这也会改变 TTY/session 语义；依赖终端控制的交互程序应设置 `killTree: false`。Windows 使用 `taskkill /T /F`，不存在与 SIGTERM 等价的优雅终止阶段。

## 性能

- 默认按 Node stream 数据块直接发射，不增加固定延迟。
- 高频小块输出可配置 batching，减少 Observable 通知次数。
- `rxCollect` 在结束时只执行一次 Buffer 拼接或字符串 join。
- 原始输出上限在解码前按字节计算。
- 低层错误快照最多保留每个通道前 1 MiB 输出，避免流式任务持续占用内存。
- 运行 `pnpm bench` 查看逐块、批处理和聚合吞吐。

长日志流式转发时，需要显式提高或关闭 `maxStdoutBytes`/`maxStderrBytes`。

## Demo

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
```
