import { EventEmitter } from "node:events";
import type { ChildProcess } from "node:child_process";
import { Readable } from "node:stream";
import { Observable, lastValueFrom, of, toArray } from "rxjs";
import { describe, expect, it, vi } from "vitest";
import { RxChildProcessError } from "../src/errors.js";
import { execute, type ExecuteRequest } from "../src/runner.js";
import type { RxProcessEvent } from "../src/types.js";

interface FakeChild extends EventEmitter {
  pid: number | undefined;
  stdout: EventEmitter;
  stderr: EventEmitter;
  stdin?: EventEmitter;
  kill: ReturnType<typeof vi.fn>;
  exitCode: number | null;
  signalCode: NodeJS.Signals | null;
}

interface FakeStdin extends EventEmitter {
  readonly end: ReturnType<typeof vi.fn>;
  readonly write: ReturnType<typeof vi.fn>;
}

const command = {
  file: "fake-process",
  args: [],
  shell: false,
} as const;

function createChild(pid: number | undefined = 4321): FakeChild {
  const child = new EventEmitter() as FakeChild;
  child.pid = pid;
  child.stdout = new EventEmitter();
  child.stderr = new EventEmitter();
  child.exitCode = null;
  child.signalCode = null;
  child.kill = vi.fn(() => true);
  return child;
}

function createStdin(): FakeStdin {
  const stdin = new EventEmitter() as FakeStdin;
  Object.assign(stdin, {
    end: vi.fn(),
    write: vi.fn(() => true),
  });
  return stdin;
}

function request(
  child: ChildProcess,
  options: ExecuteRequest["options"] = {},
): ExecuteRequest {
  return {
    command,
    options,
    launch: () => child,
    autoEndStdin: false,
    stdinPipeByDefault: true,
  };
}

function collectEvents(
  source: Observable<RxProcessEvent<unknown, Buffer | string>>,
): Promise<Array<RxProcessEvent<unknown, Buffer | string>>> {
  return lastValueFrom(source.pipe(toArray()));
}

type CloseListener = (
  code: number | null,
  signal: NodeJS.Signals | null,
) => void;

function getOnceCloseListener(child: FakeChild): CloseListener {
  const wrapper = child.rawListeners("close")[0] as
    | (CloseListener & { readonly listener?: CloseListener })
    | undefined;
  if (!wrapper) {
    throw new Error("缺少 close 监听器");
  }
  return wrapper.listener ?? wrapper;
}

describe("执行运行时异常路径", () => {
  it("同步启动失败时报告 spawn 错误并保留原因", async () => {
    const cause = new Error("launch failed");
    const source = execute({
      ...request(createChild() as unknown as ChildProcess),
      launch() {
        throw cause;
      },
    });

    const failure = await lastValueFrom(source).catch(
      (error: unknown) => error,
    );
    expect(failure).toBeInstanceOf(RxChildProcessError);
    expect(failure).toMatchObject({
      kind: "spawn",
      cause,
      command,
    });
  });

  it("spawn 事件缺少 PID 时报告 spawn 错误", async () => {
    const child = createChild();
    child.pid = undefined;
    const promise = collectEvents(
      execute(request(child as unknown as ChildProcess)),
    );

    child.emit("spawn");

    await expect(promise).rejects.toMatchObject({
      kind: "spawn",
      message: "子进程启动后未分配 PID",
    });
  });

  it("spawn 后的运行时错误与启动错误使用不同分类", async () => {
    const child = createChild();
    const cause = new Error("runtime failed");
    const promise = collectEvents(
      execute(request(child as unknown as ChildProcess)),
    );

    child.emit("spawn");
    child.emit("error", cause);

    await expect(promise).rejects.toMatchObject({
      kind: "runtime",
      cause,
    });
  });

  it("批处理达到时间阈值后主动刷新缓冲数据", async () => {
    vi.useFakeTimers();
    try {
      const child = createChild();
      const events: Array<RxProcessEvent<unknown, Buffer | string>> = [];
      const subscription = execute(
        request(child as unknown as ChildProcess, {
          batching: { maxBytes: 1_000, flushIntervalMs: 10 },
        }),
      ).subscribe((event) => events.push(event));

      child.emit("spawn");
      child.stdout.emit("data", Buffer.from("batched"));
      expect(
        events.filter((event) => event.type === "stdout"),
      ).toHaveLength(0);

      await vi.advanceTimersByTimeAsync(10);
      expect(events).toContainEqual({
        type: "stdout",
        chunk: Buffer.from("batched"),
      });
      subscription.unsubscribe();
    } finally {
      vi.useRealTimers();
    }
  });

  it("无限刷新间隔在关闭时仍会冲刷尾部数据", async () => {
    const child = createChild();
    const promise = collectEvents(
      execute(
        request(child as unknown as ChildProcess, {
          batching: {
            maxBytes: 1_000,
            flushIntervalMs: Number.POSITIVE_INFINITY,
          },
        }),
      ),
    );

    child.emit("spawn");
    child.stdout.emit("data", Buffer.from("tail"));
    child.emit("exit", 0, null);
    child.emit("close", 0, null);

    const events = await promise;
    expect(events).toContainEqual({
      type: "stdout",
      chunk: Buffer.from("tail"),
    });
    expect(events.at(-1)).toMatchObject({ type: "close", code: 0 });
  });

  it("重复 close 只发射一次关闭事件并完成一次", () => {
    const child = createChild();
    const events: Array<RxProcessEvent<unknown, Buffer | string>> = [];
    let completions = 0;
    execute(request(child as unknown as ChildProcess)).subscribe({
      next: (event) => events.push(event),
      complete: () => {
        completions += 1;
      },
    });

    child.emit("spawn");
    const closeListener = getOnceCloseListener(child);
    child.emit("exit", 0, null);
    child.emit("close", 0, null);
    closeListener(0, null);

    expect(events.filter((event) => event.type === "close")).toHaveLength(1);
    expect(completions).toBe(1);
  });

  it("订阅关闭后到达的 close 不会继续发射事件", () => {
    const child = createChild();
    const next = vi.fn();
    const complete = vi.fn();
    const subscription = execute(
      request(child as unknown as ChildProcess),
    ).subscribe({ next, complete });

    child.emit("spawn");
    const closeListener = getOnceCloseListener(child);
    subscription.unsubscribe();
    closeListener(0, null);

    expect(next).toHaveBeenCalledTimes(1);
    expect(complete).not.toHaveBeenCalled();
  });

  it("Windows 强制关闭 detached 并使用非进程组启动语义", () => {
    const child = createChild();
    const platform = vi
      .spyOn(process, "platform", "get")
      .mockReturnValue("win32");
    let detached: unknown;
    try {
      const subscription = execute({
        ...request(child as unknown as ChildProcess),
        launch(nativeOptions) {
          detached = nativeOptions.detached;
          return child as unknown as ChildProcess;
        },
      }).subscribe();

      expect(detached).toBe(false);
      subscription.unsubscribe();
    } finally {
      platform.mockRestore();
    }
  });

  it("字符串形式的标准输出会转换为 Buffer 后发射", async () => {
    const child = createChild();
    const promise = collectEvents(
      execute(request(child as unknown as ChildProcess)),
    );

    child.emit("spawn");
    child.stdout.emit("data", "text");
    child.emit("exit", 0, null);
    child.emit("close", 0, null);

    const events = await promise;
    expect(events).toContainEqual({
      type: "stdout",
      chunk: Buffer.from("text"),
    });
  });

  it("失败清理期间的迟到回调不会重复终结执行", async () => {
    const child = createChild();
    const stdin = createStdin();
    child.stdin = stdin;
    const input = new Observable<string>(() => () => {
      child.emit("error", new Error("late runtime error"));
      child.stdout.emit("data", Buffer.from("late output"));
      stdin.emit("error", new Error("late stdin error"));
    });
    const promise = collectEvents(
      execute(
        request(child as unknown as ChildProcess, {
          input,
          maxStdoutBytes: 3,
        }),
      ),
    );

    child.emit("spawn");
    child.stdout.emit("data", Buffer.from("too long"));

    await expect(promise).rejects.toMatchObject({ kind: "output-limit" });
  });
});

describe("stdin 异常与背压路径", () => {
  it("Readable 输入报错时通过 stdin 错误终止执行", async () => {
    const child = createChild();
    const stdin = createStdin();
    child.stdin = stdin;
    const cause = new Error("readable failed");
    const input = new Readable({
      read() {
        // 由测试显式触发错误，避免依赖底层文件或网络 I/O。
      },
    });
    const promise = collectEvents(
      execute(
        request(child as unknown as ChildProcess, {
          input,
        }),
      ),
    );

    input.destroy(cause);

    await expect(promise).rejects.toMatchObject({
      kind: "stdin",
      cause,
    });
  });

  it("Observable 输入报错时保留原始错误原因", async () => {
    const child = createChild();
    child.stdin = createStdin();
    const cause = new Error("observable failed");
    const promise = collectEvents(
      execute(
        request(child as unknown as ChildProcess, {
          input: new Observable<Buffer>((subscriber) => {
            subscriber.error(cause);
          }),
        }),
      ),
    );

    await expect(promise).rejects.toMatchObject({
      kind: "stdin",
      cause,
    });
  });

  it("运行中 stdin 发生非忽略错误时终止执行", async () => {
    const child = createChild();
    const stdin = createStdin();
    child.stdin = stdin;
    const cause = Object.assign(new Error("stdin failed"), { code: "EIO" });
    const promise = collectEvents(
      execute(
        request(child as unknown as ChildProcess, {
          input: new Observable<string>(() => undefined),
        }),
      ),
    );

    stdin.emit("error", cause);

    await expect(promise).rejects.toMatchObject({
      kind: "stdin",
      cause,
    });
  });

  it("无 stdin pipe 时报告配置导致无法写入输入", async () => {
    const child = createChild();
    const promise = collectEvents(
      execute(
        request(child as unknown as ChildProcess, {
          input: "hello",
        }),
      ),
    );

    await expect(promise).rejects.toMatchObject({
      kind: "stdin",
      message: "当前 stdio 配置没有可写的 stdin pipe",
    });
  });

  it("同步写入 stdin 抛错时报告 stdin 错误", async () => {
    const child = createChild();
    const stdin = createStdin();
    const cause = new Error("write failed");
    stdin.end.mockImplementation(() => {
      throw cause;
    });
    vi.spyOn(stdin, "off").mockImplementation(() => {
      throw new Error("late cleanup failed");
    });
    child.stdin = stdin;

    await expect(
      lastValueFrom(
        execute(
          request(child as unknown as ChildProcess, {
            input: "hello",
          }),
        ),
      ),
    ).rejects.toMatchObject({
      kind: "stdin",
      cause,
    });
  });

  it("正常清理时单个清理器抛错不会阻止其他资源释放", async () => {
    const child = createChild();
    const stdin = createStdin();
    vi.spyOn(stdin, "off").mockImplementation(() => {
      throw new Error("cleanup failed");
    });
    child.stdin = stdin;
    const promise = collectEvents(
      execute(
        request(child as unknown as ChildProcess, {
          input: new Observable<string>(() => undefined),
        }),
      ),
    );

    child.emit("spawn");
    child.emit("exit", 0, null);
    child.emit("close", 0, null);

    await expect(promise).resolves.toEqual(
      expect.arrayContaining([expect.objectContaining({ type: "close" })]),
    );
    expect(child.listenerCount("spawn")).toBe(0);
    expect(child.listenerCount("close")).toBe(0);
  });

  it.each([
    "EPIPE",
    "ERR_STREAM_WRITE_AFTER_END",
    "ERR_STREAM_DESTROYED",
  ])("忽略 stdin 已关闭产生的 %s", async (code) => {
    const child = createChild();
    const stdin = createStdin();
    stdin.end.mockImplementation(() => {
      throw Object.assign(new Error(code), { code });
    });
    child.stdin = stdin;
    const promise = collectEvents(
      execute(
        request(child as unknown as ChildProcess, {
          input: "hello",
        }),
      ),
    );

    child.emit("spawn");
    child.emit("exit", 0, null);
    child.emit("close", 0, null);

    await expect(promise).resolves.toEqual(
      expect.arrayContaining([expect.objectContaining({ type: "close" })]),
    );
  });

  it("Observable 输入等待 drain 后继续写入并关闭 stdin", async () => {
    const child = createChild();
    const stdin = createStdin();
    stdin.write
      .mockReturnValueOnce(false)
      .mockReturnValueOnce(true);
    child.stdin = stdin;
    const promise = collectEvents(
      execute(
        request(child as unknown as ChildProcess, {
          input: of("first", "second"),
          maxStdinQueueBytes: 0,
        }),
      ),
    );

    expect(stdin.write).toHaveBeenCalledTimes(1);
    expect(stdin.write).toHaveBeenLastCalledWith("first");
    stdin.emit("drain");
    expect(stdin.write).toHaveBeenCalledTimes(2);
    expect(stdin.write).toHaveBeenLastCalledWith("second");
    expect(stdin.end).toHaveBeenCalledTimes(1);

    child.emit("spawn");
    child.emit("exit", 0, null);
    child.emit("close", 0, null);
    await expect(promise).resolves.toEqual(
      expect.arrayContaining([expect.objectContaining({ type: "close" })]),
    );
  });

  it("Observable 写入 stdin 抛错时报告 stdin 错误", async () => {
    const child = createChild();
    const stdin = createStdin();
    const cause = new Error("stream write failed");
    stdin.write.mockImplementation(() => {
      throw cause;
    });
    child.stdin = stdin;

    await expect(
      lastValueFrom(
        execute(
          request(child as unknown as ChildProcess, {
            input: of("value"),
          }),
        ),
      ),
    ).rejects.toMatchObject({
      kind: "stdin",
      cause,
    });
  });

  it("stdio 以 pipe 字符串配置时也允许自动输入", async () => {
    const child = createChild();
    const stdin = createStdin();
    child.stdin = stdin;
    const promise = collectEvents(
      execute(
        request(child as unknown as ChildProcess, {
          input: "hello",
          stdio: "pipe",
        }),
      ),
    );

    expect(stdin.end).toHaveBeenCalledWith("hello");
    child.emit("spawn");
    child.emit("exit", 0, null);
    child.emit("close", 0, null);
    await expect(promise).resolves.toEqual(
      expect.arrayContaining([expect.objectContaining({ type: "close" })]),
    );
  });

  it("启动期间输入被改成不支持的类型时报告 stdin 错误", async () => {
    const child = createChild();
    const stdin = createStdin();
    child.stdin = stdin;
    const input = {
      lift() {
        return input;
      },
      subscribe() {
        return { unsubscribe: vi.fn() };
      },
    };
    const options: ExecuteRequest["options"] = {
      input: input as unknown as Observable<string>,
    };
    const executeRequest = request(child as unknown as ChildProcess, options);
    const source = execute({
      ...executeRequest,
      launch(nativeOptions) {
        input.subscribe = undefined as never;
        return executeRequest.launch(nativeOptions);
      },
    });

    await expect(lastValueFrom(source)).rejects.toMatchObject({
      kind: "stdin",
      message: "不支持的 stdin 输入类型",
    });
  });
});
