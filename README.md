# rx-child-process

English | [简体中文](./README.zh-CN.md)

A typed RxJS wrapper for Node.js `child_process`. It lets you run executables, shell commands, forked Node.js modules, and inline Node.js code as cancellable, observable processes with explicit resource limits and a consistent failure model.

- Runtime: Node.js 20+
- RxJS: peer dependency `rxjs@^7.8.2`
- Module formats: ESM and CommonJS
- Package includes: TypeScript declarations

## Features

- Cold Observables: every subscription starts an independent child process.
- Unified event stream for `spawn`, stdout, stderr, IPC messages, disconnects, `exit`, and `close`.
- Low-level streaming APIs plus `rxCollect` for one-shot aggregated results.
- Typed events, type guards, and output types that follow the configured encoding.
- Abort through `AbortSignal` or RxJS unsubscription.
- Process-tree termination on POSIX and Windows.
- Safe argument handling by default: commands run without a shell unless explicitly requested.
- Controlled resources: timeout, output byte limits, IPC message limits, and stdin backpressure.
- stdin input from strings, buffers, Node.js Readables, or RxJS Observables.
- Optional output batching by byte count or elapsed time.

## Installation

```bash
pnpm add rx-child-process rxjs
```

Using npm:

```bash
npm install rx-child-process rxjs
```

The package supports both ESM and CommonJS:

```js
const { rxExecFile } = require("rx-child-process");
```

## Quick start

```ts
import { firstValueFrom } from "rxjs";
import { rxCollect, rxExecFile } from "rx-child-process";

const result = await firstValueFrom(
  rxCollect(rxExecFile("git", ["status", "--short"])),
);

process.stdout.write(result.stdout);
console.error(result.stderr);
console.log(`exit code: ${result.code}, duration: ${result.durationMs} ms`);
```

Stream process events instead of waiting for completion:

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
    console.log("process closed");
  },
});
```

## Core concepts

### Cold Observables

Every execution API returns a cold Observable. Subscribing twice starts two processes:

```ts
const source = rxSpawn("node", ["worker.mjs"], { timeoutMs: 5_000 });

source.subscribe(console.log);
source.subscribe(console.log); // Starts a second process.
```

If several consumers need the same process, share the source before branching:

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

`rxCollect` subscribes to its upstream when it is subscribed, just like any other cold source. If multiple consumers need the same live process, share the low-level event source before branching, and make sure those consumers subscribe before the events they need occur.

### Arguments and options overloads

Every process entry point accepts either an explicit argument array or options in the second position:

```ts
rxSpawn("git", ["status", "--short"], options);
rxSpawn("git", options);
```

The same rule applies to `rxExecFile`, `rxFork`, `rxNodeCode`, and `rxNodeModule`. Argument arrays are copied before being passed to Node.js.

### Output typing

Output is a `Buffer` by default:

```ts
const buffered = rxSpawn("node", ["worker.mjs"]);
// Observable<RxProcessEvent<unknown, Buffer>>

const decoded = rxSpawn("node", ["worker.mjs"], { encoding: "utf8" });
// Observable<RxProcessEvent<unknown, string>>
```

For fork IPC messages, provide the message type:

```ts
type WorkerMessage = { type: "ready" } | { type: "result"; value: number };

const worker = rxFork<WorkerMessage>("worker.mjs", { encoding: "utf8" });
// Observable<RxProcessEvent<WorkerMessage, string>>
```

## Execution APIs

All process entry points return `Observable<RxProcessEvent<TMessage, TOutput>>`.

| API | Purpose | Shell by default | stdin pipe | Auto-close stdin when no `input` is supplied |
| --- | --- | --- | --- | --- |
| `rxSpawn(command, args?, options?)` | Spawn an executable with an argv array. | No | Node.js default | No |
| `rxExec(command, options?)` | Run a shell command string. | Yes | Forced pipe | Yes |
| `rxExecFile(file, args?, options?)` | Spawn a file using an argv array. | No | Forced pipe | Yes |
| `rxFork(modulePath, args?, options?)` | Fork a Node.js module with an IPC channel. | No | Pipe when `silent` is enabled | No |
| `rxNodeCode(code, args?, options?)` | Run inline code through `node -e`. | No | Node.js default | No |
| `rxNodeModule(modulePath, args?, options?)` | Intent-revealing alias of `rxFork`. | No | Pipe when `silent` is enabled | No |

All APIs also accept options as the second argument. For example, `rxExecFile(file, options)` is equivalent to calling `rxExecFile(file, [], options)`.

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

`rxSpawn` passes `command` and `args` directly to Node.js `spawn`. It does not use a shell unless `shell` is explicitly set. With the default Node.js `stdio`, stdin is a writable pipe but remains open when neither `input` nor `stdio: "ignore"` is configured. Call `child.stdin?.end()` from the `spawn` event if the program waits for EOF.

### `rxExec`

```ts
import { rxExec } from "rx-child-process";

const source = rxExec("printf '%s\n' \"$HOME\"", {
  shell: "/bin/bash",
  timeoutMs: 5_000,
});
```

`rxExec` accepts a complete shell command string. It enables the shell by default and forces `stdio: "pipe"`. Only use it when the complete command string is trusted.

### `rxExecFile`

```ts
import { rxExecFile } from "rx-child-process";

const source = rxExecFile(
  "convert",
  [untrustedInputPath, "output.png"],
  { timeoutMs: 30_000 },
);
```

`rxExecFile` uses an argv array and does not invoke a shell by default. It forces `stdio: "pipe"`. This is the preferred API when any argument comes from untrusted input.

### `rxFork`

```ts
import { filterEvent, rxFork, RxEventType } from "rx-child-process";

const worker = rxFork<{ type: "ready" }>("worker.mjs");

worker.pipe(filterEvent(RxEventType.Message)).subscribe(({ message }) => {
  console.log(message);
});
```

`rxFork` creates a Node.js IPC channel. `silent` defaults to `true`, so stdout and stderr are captured. If `stdio` is explicitly configured, that configuration is passed through. The low-level source does not close stdin automatically when no `input` is supplied.

To send a message or handle, use the original child process from the `spawn` event:

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

`rxNodeCode` runs `process.execPath` with arguments ordered as:

```text
[...nodeArgs, "-e", code, ...args]
```

It never enables a shell. `nodeArgs` is public only for Node.js execution options; it is not a shell escape.

### `rxNodeModule`

```ts
import { firstValueFrom } from "rxjs";
import { rxCollect, rxNodeModule } from "rx-child-process";

const result = await firstValueFrom(
  rxCollect(rxNodeModule("./worker.mjs", ["--mode", "batch"])),
);
```

`rxNodeModule` is an intent-revealing alias of `rxFork` and has identical runtime behavior, including IPC support.

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

`rxCollect(source, options?)` subscribes to a low-level event stream and emits one `RxCommandResult`, then completes.

| Option | Default | Description |
| --- | --- | --- |
| `failOnNonZeroExit` | `true` | Reject when `code` is a non-zero number. |
| `failOnSignal` | `true` | Reject when the process exits because of a signal. |
| `maxMessages` | `1000` | Maximum retained IPC messages. `0` or `RxLimits.Disabled` disables the limit. |

`RxCommandResult.messages` contains message payloads only. The optional `sendHandle` field remains available on low-level `message` events but is not retained by `rxCollect`. Exceeding `maxMessages` stops the upstream subscription, which also requests termination of the underlying process.

The returned result is typed from the upstream output:

```ts
import { firstValueFrom } from "rxjs";

const text = await firstValueFrom(
  rxCollect(rxSpawn("node", ["worker.mjs"], { encoding: "utf8" })),
);
// text.stdout and text.stderr are strings.
```

## Options

### Managed options

These options are implemented by `rx-child-process` and are shared by all process entry points.

| Option | Default | Description |
| --- | --- | --- |
| `signal` | none | `AbortSignal` used to cancel the execution. |
| `timeoutMs` | `30000` | Execution timeout in milliseconds. `0` or `RxLimits.Disabled` disables it. |
| `killGraceMs` | `2000` | Delay between graceful termination and forced termination. `Infinity` disables escalation. |
| `killTree` | `true` | Terminate the process tree instead of only the direct child. |
| `maxStdoutBytes` | `10 MiB` | Maximum stdout bytes received before decoding. `0` or `RxLimits.Disabled` disables it. |
| `maxStderrBytes` | `10 MiB` | Maximum stderr bytes received before decoding. `0` or `RxLimits.Disabled` disables it. |
| `input` | none | string, Buffer, Readable, or Observable written to stdin. |
| `maxStdinQueueBytes` | `1 MiB` | Maximum queued Observable-input bytes while stdin is backpressured. `0` or `RxLimits.Disabled` disables it. |
| `encoding` | `null` | Output encoding. `null` emits Buffers; a valid `BufferEncoding` emits strings. |
| `batching` | `false` | Per-channel output batching. `false` preserves Node.js stream chunks. |
| `failOnNonZeroExit` | `false` | Low-level mode: emit `close`, then enter the error channel for a non-zero exit code. |
| `failOnSignal` | `false` | Low-level mode: emit `close`, then enter the error channel for a signal exit. |

`rxNodeCode` additionally supports:

| Option | Default | Description |
| --- | --- | --- |
| `nodeArgs` | `[]` | Node.js arguments placed before `-e` and the inline code. |

### Batching

```ts
const source = rxSpawn("server", ["--verbose"], {
  batching: {
    maxBytes: 64 * 1024,
    flushIntervalMs: 10,
  },
});
```

| Field | Default | Description |
| --- | --- | --- |
| `maxBytes` | `64 KiB` | Flush a channel when its accumulated raw bytes reach this value. `0` or `Infinity` disables the byte threshold. |
| `flushIntervalMs` | `10` | Flush after this delay. `0` or `Infinity` disables time-based flushing. |

Batching is isolated per channel. Buffers are flushed before `close`, including decoder tails. Without batching, each non-empty data chunk is emitted immediately.

### `RxLimits`

```ts
import { RxLimits, rxSpawn } from "rx-child-process";

rxSpawn("long-running-task", {
  timeoutMs: RxLimits.Disabled,
  maxStdoutBytes: RxLimits.Disabled,
});
```

`RxLimits.Disabled` is `Number.POSITIVE_INFINITY`. For timeout and byte limits, `0` has the same disabling effect.

### Native Node.js options

The option types extend the corresponding Node.js types (`SpawnOptions`, `ExecOptions`, `ExecFileOptions`, and `ForkOptions`). Options such as `cwd`, `env`, `stdio`, `windowsHide`, `uid`, `gid`, `serialization`, and `argv0` are passed through when supported by the selected entry point.

The library controls these fields:

| Controlled field | Replacement |
| --- | --- |
| `signal` | Use the library-level `signal`. |
| `timeout` | Use `timeoutMs`. |
| `killSignal` | Termination uses the library's process-tree strategy. |
| `detached` | Derived from `killTree` and platform. |
| `maxBuffer` | Use `maxStdoutBytes` and `maxStderrBytes`. |
| `encoding` | Use the library-level `encoding`. |

These fields are removed before Node.js launch. They do not override library behavior.

Additional pass-through notes:

- `windowsHide` defaults to `true`.
- `rxExec` and `rxExecFile` force `stdio: "pipe"`.
- `rxFork` defaults to `silent: true` when `stdio` is not provided.
- On POSIX, `detached` is forced to match `killTree`. On Windows, `detached` is forced to `false`.

### stdin behavior

`input` accepts:

```ts
type RxProcessInput =
  | string
  | Buffer
  | Readable
  | Observable<string | Buffer>;
```

Behavior by input type:

| Input | Behavior |
| --- | --- |
| string or Buffer | Written to stdin and then ends stdin. |
| Node.js `Readable` | Piped to stdin; stdin ends when the Readable ends. |
| RxJS `Observable` | Writes with backpressure, waits for `drain`, and ends stdin after completion and queue draining. |

An Observable that never completes keeps stdin open. Configure `stdio: "ignore"` for stdin when the child does not read input. When `input` is configured, stdin must be a pipe; an incompatible `stdio` configuration fails with a `configuration` error before launch.

Example:

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

## Events

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

### Event order

After a successful launch, the normal sequence is:

```text
spawn -> stdout/stderr/message/disconnect events -> exit -> close
```

- stdout and stderr preserve order within their own channels.
- Cross-channel ordering is not guaranteed beyond the order in which the events are observed by this library.
- `message` and `disconnect` are emitted only when an IPC channel is available.
- `exit` reports that the process ended; `close` reports that the process and its standard streams have closed.
- `close` is the final `next` event and is emitted exactly once.
- `durationMs` measures from subscription execution setup to `close`.

### Event constants and guards

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

`RxEventType` provides `Spawn`, `Stdout`, `Stderr`, `Message`, `Disconnect`, `Exit`, and `Close`.

`filterEvent(type)` filters one event kind and narrows its type:

```ts
import { filterEvent, RxEventType, rxSpawn } from "rx-child-process";

rxSpawn("node", ["worker.mjs"])
  .pipe(filterEvent(RxEventType.Stderr))
  .subscribe(({ chunk }) => process.stderr.write(chunk));
```

The extracted type helper is `RxEventOfType<TMessage, TOutput, TType>`.

## Results and errors

### Success result

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

`command` contains `file`, `args`, `shell`, and an optional `cwd`. `pid` is absent when the process never started or the upstream source did not provide it. `code` is `null` for a signal exit; `signal` is non-null in that case.

### `RxChildProcessError`

All expected library failures use `RxChildProcessError`:

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

Error diagnostics are always Buffers, even when successful output uses a string encoding. The low-level diagnostic snapshot retains at most the first 1 MiB of each output channel.

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

### Error kinds

| Kind | Meaning |
| --- | --- |
| `configuration` | Invalid option, incompatible `stdio`, invalid `maxMessages`, or unsupported encoding. |
| `spawn` | The executable or module could not start. |
| `runtime` | A child-process error occurred after startup, or an untyped upstream error reached `rxCollect`. |
| `abort` | An `AbortSignal` aborted the execution. |
| `timeout` | The execution exceeded `timeoutMs`. |
| `output-limit` | stdout or stderr exceeded its configured byte limit. |
| `message-limit` | `rxCollect` retained more IPC messages than `maxMessages`. |
| `stdin` | Reading, writing, or configuring stdin failed. |
| `exit` | The process exited with a non-zero code and exit failures are enabled. |
| `signal` | The process exited because of a signal and signal failures are enabled. |

Use `isChildProcessErrorKind(value)` to validate a category and `isRxChildProcessError(error, kind?)` to validate a library error. The latter does not depend on prototype identity, so it also works across duplicate package installations and mixed ESM/CommonJS loading.

### Failure ordering and aggregation

- Low-level APIs treat non-zero exits and signal exits as successful completion unless `failOnNonZeroExit` or `failOnSignal` is enabled.
- When either low-level failure flag matches, the source emits `close` first, then calls `error`.
- `rxCollect` rejects by default for non-zero exit codes and signal exits.
- Errors already emitted by the low-level source pass through `rxCollect` unchanged.
- If an error contains a result, use `error.result` for diagnostics because `rxCollect` does not emit a separate result before the error.
- Runtime failures such as abort, timeout, output-limit, spawn, runtime, and stdin failures request termination of the child process. Configuration failures detected before launch do not start a process.

## Cancellation and process trees

Use `AbortSignal`:

```ts
import { firstValueFrom } from "rxjs";
import {
  isRxChildProcessError,
  RxErrorKind,
  rxCollect,
  rxSpawn,
} from "rx-child-process";

const controller = new AbortController();
setTimeout(() => controller.abort(new Error("cancelled by caller")), 1_000);

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
    console.log("aborted");
  }
}
```

Or unsubscribe:

```ts
const subscription = rxSpawn("long-running-command").subscribe(console.log);

subscription.unsubscribe();
// The process tree is terminated. No error or completion notification is sent.
```

Cancellation semantics:

- An already-aborted `AbortSignal` fails with `abort` before launching a process.
- Aborting during execution requests termination and reports an `abort` error.
- The error may be reported before the operating system has fully reaped every terminated process.
- RxJS unsubscription is silent but still requests process-tree termination.

Termination strategy:

| Platform | `killTree: true` | `killTree: false` |
| --- | --- | --- |
| POSIX | Starts a new process group, sends SIGTERM to the group, then SIGKILL after `killGraceMs`. | Sends SIGTERM to the direct child, then SIGKILL after `killGraceMs` if still running. |
| Windows | Uses `taskkill /PID <pid> /T /F`. | Sends SIGTERM to the direct child. |

Set `killGraceMs: Infinity` to disable escalation. This can leave an unresponsive process running indefinitely. On Windows, the process-tree path is forced immediately by `taskkill /F`; there is no POSIX-equivalent graceful phase.

POSIX process-tree mode changes session and TTY behavior because the child runs in its own process group. Interactive programs that depend on terminal control may need `killTree: false`.

## Security

`rxSpawn`, `rxExecFile`, `rxFork`, `rxNodeCode`, and `rxNodeModule` do not invoke a shell by default. Arguments are passed through argv, so metacharacters such as `;`, `|`, `$()` and backticks are not interpreted by a shell.

```ts
// Preferred for untrusted arguments.
rxExecFile("convert", [untrustedInput, "output.png"]);

// Dangerous: the complete command string is interpreted by a shell.
rxExec(`convert ${untrustedInput} output.png`);
```

Security boundaries:

- Use `rxExec` only with trusted command strings.
- `rxNodeCode` and `rxNodeModule` execute arbitrary Node.js code.
- The library provides process supervision, not a filesystem, network, OS-user, or permission sandbox.
- Shell-free execution prevents shell interpolation; it does not make the invoked executable safe.

## Output, memory, and performance

- Without batching, data is emitted as Node.js provides it, with no fixed delay.
- Batching reduces Observable notifications for high-frequency small writes.
- Byte limits are evaluated before string decoding.
- `StringDecoder` preserves multibyte characters across chunk boundaries and flushes incomplete sequences on close.
- `rxCollect` accumulates stdout and stderr in memory and builds the final Buffer or string only once.
- Low-level execution always retains up to 1 MiB per channel for error diagnostics.
- `rxCollect` retains all messages until limit, result construction, or failure.

Increase or disable output limits for long-running log streams:

```ts
import { RxLimits, rxSpawn } from "rx-child-process";

const source = rxSpawn("server", ["--verbose"], {
  maxStdoutBytes: 100 * 1024 * 1024,
  maxStderrBytes: RxLimits.Disabled,
});
```

For very large outputs, prefer streaming and handle backpressure in the subscriber instead of using `rxCollect`.

## Type reference

### Public types

| Type | Description |
| --- | --- |
| `RxBaseOptions` | Shared managed options. |
| `RxBatchOptions` | `maxBytes` and `flushIntervalMs` batching controls. |
| `RxProcessInput` | Accepted stdin input union. |
| `RxSpawnOptions` | Node.js spawn options plus managed options. |
| `RxExecOptions` | Node.js exec options plus managed options. |
| `RxExecFileOptions` | Node.js execFile options plus managed options. |
| `RxForkOptions` | Node.js fork options plus managed options. |
| `RxNodeCodeOptions` | Spawn options without `shell`, plus `nodeArgs`. |
| `RxCollectOptions` | Aggregation failure policy and message limit. |
| `RxCommandDescriptor` | Observed command metadata. |
| `RxSpawnEvent` | Successful startup event. |
| `RxStdoutEvent<TOutput>` | stdout chunk event. |
| `RxStderrEvent<TOutput>` | stderr chunk event. |
| `RxOutputEvent<TOutput>` | stdout or stderr event union. |
| `RxMessageEvent<TMessage>` | Fork IPC message event. |
| `RxSendHandle` | `net.Socket` or `net.Server` IPC handle. |
| `RxDisconnectEvent` | IPC disconnection event. |
| `RxExitEvent` | Process exit event. |
| `RxCloseEvent` | Process and stream close event. |
| `RxProcessEvent<TMessage, TOutput>` | Complete event union. |
| `RxCommandResult<TMessage, TOutput>` | Aggregated successful result. |
| `RxEventOfType<TMessage, TOutput, TType>` | Narrowed event from a type name. |
| `RxChildProcessErrorKind` | Error category union. |
| `RxChildProcessErrorOptions` | Error constructor metadata. |
| `RxErrorResult` | Partial execution snapshot attached to errors. |

### Runtime exports

| Export | Description |
| --- | --- |
| `rxSpawn` | Spawn an executable with argv. |
| `rxExec` | Run a shell command. |
| `rxExecFile` | Spawn a file with argv. |
| `rxFork` | Fork a Node.js module with IPC. |
| `rxNodeCode` | Run inline Node.js code. |
| `rxNodeModule` | Alias of `rxFork`. |
| `rxCollect` | Aggregate an event stream. |
| `filterEvent` | Filter and type-narrow process events. |
| `isSpawnEvent` | Detect a spawn event. |
| `isStdoutEvent` | Detect a stdout event. |
| `isStderrEvent` | Detect a stderr event. |
| `isOutputEvent` | Detect stdout or stderr. |
| `isMessageEvent` | Detect an IPC message. |
| `isDisconnectEvent` | Detect IPC disconnection. |
| `isExitEvent` | Detect a process exit. |
| `isCloseEvent` | Detect a process close. |
| `isRxEventType` | Validate an event type name. |
| `isChildProcessErrorKind` | Validate an error category. |
| `isRxChildProcessError` | Validate a typed library error and optional category. |
| `RxEventType` | Event-name constants and type. |
| `RxErrorKind` | Error-category constants. |
| `RxLimits` | `Disabled` resource-limit value. |
| `RxChildProcessError` | Typed execution error class. |

## Demo

```bash
pnpm demo:basic
pnpm demo:stream
pnpm demo:cancel
pnpm demo:node
pnpm demo:ipc
```

## Development

```bash
pnpm install
pnpm typecheck
pnpm test
pnpm test:coverage
pnpm build
pnpm bench
```

## License

[MIT](./LICENSE)
