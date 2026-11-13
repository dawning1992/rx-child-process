import { readFile, realpath, rm, writeFile } from "node:fs/promises";
import type { ChildProcess } from "node:child_process";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Readable } from "node:stream";
import {
  Observable,
  firstValueFrom,
  lastValueFrom,
  of,
  type Subscriber,
  type Subscription,
  toArray,
} from "rxjs";
import { describe, expect, expectTypeOf, it, vi } from "vitest";
import {
  RxChildProcessError,
  isOutputEvent,
  rxCollect,
  rxExec,
  rxExecFile,
  rxFork,
  rxNodeCode,
  rxNodeModule,
  rxSpawn,
  type RxCommandResult,
  type RxProcessEvent,
} from "../src/index.js";
import {
  fixture,
  isProcessAlive,
  sleep,
  waitForProcessExit,
} from "./helpers.js";

async function collectEvents<TMessage = unknown>(
  source: Observable<RxProcessEvent<TMessage, Buffer | string>>,
): Promise<Array<RxProcessEvent<TMessage, Buffer | string>>> {
  return lastValueFrom(source.pipe(toArray()));
}

function text(value: Buffer | string): string {
  return typeof value === "string" ? value : value.toString();
}

describe("执行与聚合", () => {
  it("encoding 重载提供准确的输出类型", () => {
    const source = rxSpawn(process.execPath, ["-e", "process.exit(0)"], {
      encoding: "utf8",
    });
    expectTypeOf(source).toEqualTypeOf<
      Observable<RxProcessEvent<unknown, string>>
    >();
    expectTypeOf(firstValueFrom(rxCollect(source))).toEqualTypeOf<
      Promise<RxCommandResult<unknown, string>>
    >();
  });

  it("收集 stdout、stderr、PID 和退出元数据", async () => {
    const result = await firstValueFrom(
      rxCollect(
        rxExecFile(process.execPath, [fixture("streams.mjs"), "0"], {
          timeoutMs: 2_000,
        }),
      ),
    );

    expect(text(result.stdout)).toBe("stdout-line\n");
    expect(text(result.stderr)).toBe("stderr-line\n");
    expect(result.code).toBe(0);
    expect(result.signal).toBeNull();
    expect(result.pid).toBeTypeOf("number");
    expect(result.command.file).toBe(process.execPath);
    expect(result.durationMs).toBeGreaterThanOrEqual(0);
  });

  it("低层默认把非零退出作为可观察的普通完成", async () => {
    const events = await collectEvents(
      rxSpawn(process.execPath, [fixture("streams.mjs"), "7"], {
        timeoutMs: 2_000,
      }),
    );

    expect(events.at(-1)).toMatchObject({ type: "close", code: 7 });
    expect(events.some((event) => event.type === "close")).toBe(true);
  });

  it("低层失败模式先发射 close 再进入 error 通道", async () => {
    const events: Array<RxProcessEvent<unknown, Buffer | string>> = [];
    let failure: unknown;

    await new Promise<void>((resolve) => {
      rxSpawn(process.execPath, [fixture("streams.mjs"), "9"], {
        failOnNonZeroExit: true,
        timeoutMs: 2_000,
      }).subscribe({
        next: (event) => events.push(event),
        error: (error) => {
          failure = error;
          resolve();
        },
        complete: resolve,
      });
    });

    expect(events.at(-1)).toMatchObject({ type: "close", code: 9 });
    expect(failure).toBeInstanceOf(RxChildProcessError);
    expect((failure as RxChildProcessError).kind).toBe("exit");
    expect(text((failure as RxChildProcessError).result?.stdout ?? "")).toContain(
      "stdout-line",
    );
  });

  it("rxCollect 默认把非零退出转成类型化错误", async () => {
    await expect(
      firstValueFrom(
        rxCollect(
          rxSpawn(process.execPath, [fixture("streams.mjs"), "3"], {
            timeoutMs: 2_000,
          }),
        ),
      ),
    ).rejects.toMatchObject({
      name: "RxChildProcessError",
      kind: "exit",
    });
  });

  it("支持关闭退出码失败但保留信号失败", async () => {
    const result = await firstValueFrom(
      rxCollect(
        rxSpawn(process.execPath, [fixture("streams.mjs"), "4"], {
          timeoutMs: 2_000,
        }),
        { failOnNonZeroExit: false },
      ),
    );
    expect(result.code).toBe(4);
  });

  it("按配置输出字符串并正确跨块解码 UTF-8", async () => {
    const result = await firstValueFrom(
      rxCollect(
        rxSpawn(process.execPath, [fixture("utf8-split.mjs")], {
          encoding: "utf8",
          timeoutMs: 2_000,
        }),
      ),
    );
    expect(result.stdout).toBe("你");
  });

  it("批处理保持通道隔离且不丢失尾部数据", async () => {
    const events = await collectEvents(
      rxSpawn(process.execPath, [fixture("streams.mjs")], {
        batching: { maxBytes: 4, flushIntervalMs: 2 },
        timeoutMs: 2_000,
      }),
    );
    const stdout = events
      .flatMap((event) =>
        event.type === "stdout" ? [text(event.chunk)] : [],
      )
      .join("");
    const stderr = events
      .flatMap((event) =>
        event.type === "stderr" ? [text(event.chunk)] : [],
      )
      .join("");

    expect(stdout).toBe("stdout-line\n");
    expect(stderr).toBe("stderr-line\n");
  });

  it("编码与批处理组合会在关闭前刷新解码尾部", async () => {
    const result = await firstValueFrom(
      rxCollect(
        rxSpawn(process.execPath, [fixture("utf8-split.mjs")], {
          encoding: "utf8",
          batching: { maxBytes: 1_000, flushIntervalMs: 0 },
          timeoutMs: 2_000,
        }),
      ),
    );
    expect(result.stdout).toBe("你");
  });

  it.each([false, true])(
    "关闭时输出不完整 UTF-8 序列（batching=%s）",
    async (batching) => {
      const result = await firstValueFrom(
        rxCollect(
          rxSpawn(process.execPath, [fixture("utf8-incomplete.mjs")], {
            encoding: "utf8",
            ...(batching ? { batching: { maxBytes: 1_000 } } : {}),
            timeoutMs: 2_000,
          }),
        ),
      );

      expect(result.stdout).toBe("�");
    },
  );

  it("多个订阅各自启动独立子进程", async () => {
    const source = rxCollect(
      rxSpawn(process.execPath, [fixture("streams.mjs")], {
        timeoutMs: 2_000,
      }),
    );
    const [first, second] = await Promise.all([
      firstValueFrom(source),
      firstValueFrom(source),
    ]);

    expect(first.pid).not.toBe(second.pid);
    expect(text(first.stdout)).toBe(text(second.stdout));
  });
});

describe("错误、取消与资源限制", () => {
  it("启动不存在可执行文件时报告 spawn 错误", async () => {
    await expect(
      firstValueFrom(rxCollect(rxSpawn("__rx_missing_executable__"))),
    ).rejects.toMatchObject({
      name: "RxChildProcessError",
      kind: "spawn",
    });
  });

  it("拒绝无效配置且不启动进程", async () => {
    await expect(
      firstValueFrom(
        rxCollect(
          rxSpawn(process.execPath, [], {
            encoding: "not-an-encoding" as BufferEncoding,
          }),
        ),
      ),
    ).rejects.toMatchObject({ kind: "configuration" });

    await expect(
      firstValueFrom(
        rxCollect(
          rxSpawn(process.execPath, [fixture("echo-stdin.mjs")], {
            stdio: ["ignore", "pipe", "pipe"],
            input: "hello",
          }),
        ),
      ),
    ).rejects.toMatchObject({ kind: "configuration" });
  });

  it.each([
    ["timeoutMs", { timeoutMs: -1 }],
    ["timeoutMs NaN", { timeoutMs: Number.NaN }],
    ["killGraceMs", { killGraceMs: -1 }],
    ["maxStdoutBytes", { maxStdoutBytes: -1 }],
    ["maxStderrBytes", { maxStderrBytes: -1 }],
    ["maxStdinQueueBytes", { maxStdinQueueBytes: -1 }],
    ["signal", { signal: {} as AbortSignal }],
    ["input", { input: 123 as unknown as string }],
    ["batching", { batching: true as never }],
    [
      "batching.maxBytes",
      { batching: { maxBytes: -1 } },
    ],
    [
      "batching.flushIntervalMs",
      { batching: { flushIntervalMs: -1 } },
    ],
  ])("拒绝无效 %s 选项", async (_name, options) => {
    await expect(
      firstValueFrom(
        rxCollect(
          rxSpawn(process.execPath, [fixture("streams.mjs")], options),
        ),
      ),
    ).rejects.toMatchObject({ kind: "configuration" });
  });

  it("超时后报告 timeout 并终止进程", async () => {
    await expect(
      firstValueFrom(
        rxCollect(
          rxSpawn(process.execPath, [fixture("ignore-term.mjs")], {
            timeoutMs: 30,
            killGraceMs: 30,
          }),
        ),
      ),
    ).rejects.toMatchObject({ kind: "timeout" });
  });

  it("预取消不会启动进程", async () => {
    const controller = new AbortController();
    controller.abort("stop");

    await expect(
      firstValueFrom(
        rxCollect(
          rxSpawn(process.execPath, [fixture("streams.mjs")], {
            signal: controller.signal,
          }),
        ),
      ),
    ).rejects.toMatchObject({ kind: "abort" });
  });

  it("运行中取消通过 AbortSignal 报告错误并终止进程", async () => {
    const controller = new AbortController();
    setTimeout(() => controller.abort(), 20);

    await expect(
      firstValueFrom(
        rxCollect(
          rxSpawn(process.execPath, [fixture("ignore-term.mjs")], {
            signal: controller.signal,
            killGraceMs: 20,
          }),
        ),
      ),
    ).rejects.toMatchObject({ kind: "abort" });
  });

  it("RxJS 退订静默终止进程", async () => {
    let errorCalled = false;
    let completeCalled = false;
    let childPid: number | undefined;

    const subscription = rxSpawn(
      process.execPath,
      [fixture("ignore-term.mjs")],
      { timeoutMs: 0, killGraceMs: 20 },
    ).subscribe({
      next(event) {
        if (event.type === "spawn") {
          childPid = event.pid;
        }
      },
      error() {
        errorCalled = true;
      },
      complete() {
        completeCalled = true;
      },
    });

    await sleep(60);
    subscription.unsubscribe();
    await sleep(100);

    expect(errorCalled).toBe(false);
    expect(completeCalled).toBe(false);
    expect(childPid).toBeTypeOf("number");
    expect(isProcessAlive(childPid as number)).toBe(false);
  });

  it("stdout 超限时报告 output-limit", async () => {
    await expect(
      firstValueFrom(
        rxCollect(
          rxSpawn(process.execPath, [fixture("streams.mjs")], {
            maxStdoutBytes: 3,
            timeoutMs: 2_000,
          }),
        ),
      ),
    ).rejects.toMatchObject({ kind: "output-limit" });
  });

  it("stderr 超限时独立报告 output-limit", async () => {
    await expect(
      firstValueFrom(
        rxCollect(
          rxSpawn(process.execPath, [fixture("streams.mjs")], {
            maxStderrBytes: 3,
            timeoutMs: 2_000,
          }),
        ),
      ),
    ).rejects.toMatchObject({ kind: "output-limit" });
  });

  it("错误快照独立限制为每通道 1 MiB", async () => {
    const controller = new AbortController();
    const snapshotBytes = 1024 * 1024;
    let observedBytes = 0;
    let failure: unknown;

    await new Promise<void>((resolve, reject) => {
      rxSpawn(
        process.execPath,
        [
          "-e",
          "process.stdout.write('x'.repeat(2 * 1024 * 1024)); setInterval(() => {}, 1000)",
        ],
        {
          signal: controller.signal,
          maxStdoutBytes: 0,
          timeoutMs: 5_000,
          killGraceMs: 20,
        },
      ).subscribe({
        next(event) {
          if (event.type !== "stdout") {
            return;
          }

          observedBytes += Buffer.isBuffer(event.chunk)
            ? event.chunk.length
            : Buffer.byteLength(event.chunk);
          if (observedBytes >= snapshotBytes && !controller.signal.aborted) {
            controller.abort("snapshot-ready");
          }
        },
        error(error) {
          failure = error;
          resolve();
        },
        complete() {
          reject(new Error("子进程在快照验证前意外完成"));
        },
      });
    });

    expect(failure).toBeInstanceOf(RxChildProcessError);
    expect((failure as RxChildProcessError).result?.stdout).toHaveLength(
      snapshotBytes,
    );
  });

  it("清理真实进程树中的孙进程", async () => {
    const marker = join(
      tmpdir(),
      `rx-child-process-tree-${process.pid}-${Date.now()}`,
    );
    let childPid: number | undefined;

    const subscription = rxSpawn(
      process.execPath,
      [fixture("tree-parent.mjs")],
      {
        env: { ...process.env, MARKER: marker },
        timeoutMs: 0,
        killGraceMs: 30,
      },
    ).subscribe({
      next(event) {
        if (event.type === "spawn") {
          childPid = event.pid;
        }
      },
    });

    let grandchildPid = 0;
    for (let attempt = 0; attempt < 100; attempt += 1) {
      try {
        grandchildPid = Number(await readFile(marker, "utf8"));
        break;
      } catch {
        await sleep(10);
      }
    }

    expect(childPid).toBeTypeOf("number");
    expect(grandchildPid).toBeGreaterThan(0);
    expect(isProcessAlive(grandchildPid)).toBe(true);

    subscription.unsubscribe();
    expect(await waitForProcessExit(childPid as number)).toBe(true);
    expect(await waitForProcessExit(grandchildPid)).toBe(true);
    await rm(marker, { force: true });
  });

  it("关闭 killTree 时仍然升级并终止主进程", async () => {
    let childPid: number | undefined;
    const subscription = rxSpawn(
      process.execPath,
      [fixture("ignore-term.mjs")],
      { timeoutMs: 0, killTree: false, killGraceMs: 20 },
    ).subscribe({
      next(event) {
        if (event.type === "spawn") {
          childPid = event.pid;
        }
      },
    });

    await sleep(60);
    subscription.unsubscribe();
    await sleep(100);

    expect(childPid).toBeTypeOf("number");
    expect(isProcessAlive(childPid as number)).toBe(false);
  });

  it("IPC 消息数量超过上限时报告 message-limit", async () => {
    await expect(
      firstValueFrom(
        rxCollect(
          rxFork(fixture("ipc-burst.mjs"), [], { timeoutMs: 2_000 }),
          { maxMessages: 2 },
        ),
      ),
    ).rejects.toMatchObject({ kind: "message-limit" });
  });

  it("拒绝无效的消息上限", async () => {
    await expect(
      firstValueFrom(
        rxCollect(
          rxSpawn(process.execPath, [fixture("streams.mjs")], {
            timeoutMs: 2_000,
          }),
          { maxMessages: -1 },
        ),
      ),
    ).rejects.toMatchObject({ kind: "configuration" });
  });

  it("聚合器包装非类型化的上游错误", async () => {
    const source = new Observable<RxProcessEvent>((subscriber) => {
      subscriber.error(new Error("upstream"));
    });
    await expect(firstValueFrom(rxCollect(source))).rejects.toMatchObject({
      kind: "runtime",
    });
  });

  it("上游只发射 close 时也会在下个微任务完成任务聚合", async () => {
    const source = new Observable<RxProcessEvent>((subscriber) => {
      subscriber.next({
        type: "close",
        code: 0,
        signal: null,
        durationMs: 12,
      });
    });

    const result = await firstValueFrom(rxCollect(source));
    expect(result).toMatchObject({
      command: { file: "<unknown>" },
      code: 0,
      signal: null,
      durationMs: 12,
    });
  });

  it("容忍违反 Observable 协议的重复终结回调", async () => {
    let observer: Subscriber<RxProcessEvent> | undefined;
    const unsubscribe = vi.fn();
    const source = {
      subscribe(received: Subscriber<RxProcessEvent>): Subscription {
        observer = received;
        return { unsubscribe } as unknown as Subscription;
      },
    } as unknown as Observable<RxProcessEvent>;
    const resultPromise = firstValueFrom(rxCollect(source));

    observer?.next({ type: "close", code: 0, signal: null, durationMs: 1 });
    observer?.next({ type: "close", code: 0, signal: null, durationMs: 1 });
    await Promise.resolve();
    observer?.next({ type: "stdout", chunk: Buffer.from("late") });
    observer?.error(new Error("late error"));

    await expect(resultPromise).resolves.toMatchObject({
      code: 0,
      durationMs: 1,
    });
    expect(unsubscribe).toHaveBeenCalled();
  });

  it("上游在未发射 close 时直接完成也会返回已聚合结果", async () => {
    const source = new Observable<RxProcessEvent>((subscriber) => {
      subscriber.next({ type: "stdout", chunk: Buffer.from("partial") });
      subscriber.next({
        type: "spawn",
        pid: 99,
        child: {} as ChildProcess,
        command: {
          file: "partial-command",
          args: [],
          shell: false,
        },
        encoding: null,
      });
      subscriber.complete();
    });

    const result = await firstValueFrom(rxCollect(source));
    expect(result.command.file).toBe("partial-command");
    expect(result.pid).toBe(99);
    expect(text(result.stdout)).toBe("partial");
    expect(result.code).toBeNull();
    expect(result.signal).toBeNull();
    expect(result.durationMs).toBeGreaterThanOrEqual(0);
  });

  it("字符串输出发生失败时错误快照仍为 Buffer", async () => {
    const source = new Observable<RxProcessEvent<unknown, string>>(
      (subscriber) => {
        subscriber.next({
          type: "spawn",
          pid: 7,
          child: {} as ChildProcess,
          command: {
            file: "encoded-command",
            args: [],
            shell: false,
          },
          encoding: "utf8",
        });
        subscriber.next({ type: "stdout", chunk: "你好" });
        subscriber.next({ type: "close", code: 2, signal: null, durationMs: 5 });
        subscriber.complete();
      },
    );

    const failure = await firstValueFrom(rxCollect(source)).catch(
      (error: unknown) => error,
    );
    expect(failure).toBeInstanceOf(RxChildProcessError);
    expect((failure as RxChildProcessError).result?.stdout.toString()).toBe(
      "你好",
    );
    expect(Buffer.isBuffer((failure as RxChildProcessError).result?.stdout)).toBe(
      true,
    );
  });

  it("stderr 字符串内容也会进入失败快照", async () => {
    const source = new Observable<RxProcessEvent<unknown, string>>(
      (subscriber) => {
        subscriber.next({
          type: "stderr",
          chunk: "失败详情",
        });
        subscriber.next({ type: "close", code: 1, signal: null, durationMs: 1 });
        subscriber.complete();
      },
    );

    const failure = await firstValueFrom(rxCollect(source)).catch(
      (error: unknown) => error,
    );
    expect((failure as RxChildProcessError).result?.stderr.toString()).toBe(
      "失败详情",
    );
  });

  it.each([0, Number.POSITIVE_INFINITY])(
    "maxMessages 为 %s 时关闭消息数量限制",
    async (maxMessages) => {
      const source = new Observable<RxProcessEvent>((subscriber) => {
        for (let index = 0; index < 5; index += 1) {
          subscriber.next({ type: "message", message: index });
        }
        subscriber.next({ type: "close", code: 0, signal: null, durationMs: 1 });
        subscriber.complete();
      });

      const result = await firstValueFrom(rxCollect(source, { maxMessages }));
      expect(result.messages).toEqual([0, 1, 2, 3, 4]);
    },
  );

  it("拒绝混用 Buffer 和 string 的上游输出", async () => {
    const source = of(
      { type: "stdout", chunk: Buffer.from("a") },
      { type: "stdout", chunk: "b" },
    ) as Observable<RxProcessEvent>;

    await expect(firstValueFrom(rxCollect(source))).rejects.toMatchObject({
      kind: "configuration",
    });
  });

  it("提供输出事件判别辅助函数", () => {
    expect(
      isOutputEvent({ type: "stdout", chunk: Buffer.from("a") }),
    ).toBe(true);
    expect(
      isOutputEvent({ type: "exit", code: 0, signal: null }),
    ).toBe(false);
  });

  it.runIf(process.platform !== "win32")(
    "异常信号退出可配置为信号错误",
    async () => {
      await expect(
        firstValueFrom(
          rxCollect(
            rxSpawn(process.execPath, [fixture("self-signal.mjs")], {
              timeoutMs: 2_000,
            }),
          ),
        ),
      ).rejects.toMatchObject({ kind: "signal" });

      const result = await firstValueFrom(
        rxCollect(
          rxSpawn(process.execPath, [fixture("self-signal.mjs")], {
            timeoutMs: 2_000,
          }),
          { failOnSignal: false },
        ),
      );
      expect(result.signal).toBe("SIGTERM");
    },
  );

  it.runIf(process.platform !== "win32")(
    "低层信号失败模式先发射 close",
    async () => {
      const events: Array<RxProcessEvent<unknown, Buffer | string>> = [];
      let failure: unknown;
      await new Promise<void>((resolve) => {
        rxSpawn(process.execPath, [fixture("self-signal.mjs")], {
          failOnSignal: true,
          timeoutMs: 2_000,
        }).subscribe({
          next: (event) => events.push(event),
          error: (error) => {
            failure = error;
            resolve();
          },
          complete: resolve,
        });
      });

      expect(events.at(-1)).toMatchObject({
        type: "close",
        signal: "SIGTERM",
      });
      expect(failure).toMatchObject({ kind: "signal" });
    },
  );
});

describe("stdin 输入", () => {
  it.each([
    ["string", "你好"],
    ["Buffer", Buffer.from("你好")],
  ])("支持 %s 输入", async (_name, input) => {
    const result = await firstValueFrom(
      rxCollect(
        rxSpawn(process.execPath, [fixture("echo-stdin.mjs")], {
          input,
          timeoutMs: 2_000,
        }),
      ),
    );
    expect(text(result.stdout)).toBe("你好");
  });

  it("支持 Readable 背压输入", async () => {
    const result = await firstValueFrom(
      rxCollect(
        rxSpawn(process.execPath, [fixture("echo-stdin.mjs")], {
          input: Readable.from(["a", "b", "c"]),
          timeoutMs: 2_000,
        }),
      ),
    );
    expect(text(result.stdout)).toBe("abc");
  });

  it("支持 Observable 输入", async () => {
    const result = await firstValueFrom(
      rxCollect(
        rxSpawn(process.execPath, [fixture("echo-stdin.mjs")], {
          input: of("x", "y", "z"),
          timeoutMs: 2_000,
        }),
      ),
    );
    expect(text(result.stdout)).toBe("xyz");
  });

  it("Observable 积压超过限制时终止执行", async () => {
    const input = new Observable<string>((subscriber) => {
      for (let index = 0; index < 64; index += 1) {
        subscriber.next("x".repeat(16 * 1024));
      }
      subscriber.complete();
    });

    await expect(
      firstValueFrom(
        rxCollect(
          rxSpawn(process.execPath, [fixture("echo-stdin.mjs")], {
            input,
            maxStdinQueueBytes: 1,
            timeoutMs: 2_000,
          }),
        ),
      ),
    ).rejects.toMatchObject({ kind: "stdin" });
  });

  it("忽略子进程提前关闭 stdin 产生的 EPIPE/已关闭流错误", async () => {
    const input = new Observable<Buffer>((subscriber) => {
      for (let index = 0; index < 16; index += 1) {
        subscriber.next(Buffer.alloc(64 * 1024));
      }
      subscriber.complete();
    });

    const result = await firstValueFrom(
      rxCollect(
        rxSpawn(process.execPath, [fixture("close-stdin.mjs")], {
          input,
          maxStdinQueueBytes: 0,
          timeoutMs: 2_000,
        }),
      ),
    );
    expect(result.code).toBe(0);
  });
});

describe("定时器", () => {
  it("默认 30 秒超时由 fake timer 触发", async () => {
    vi.useFakeTimers();
    try {
      const failurePromise = firstValueFrom(
        rxCollect(
          rxSpawn(process.execPath, [fixture("ignore-term.mjs")], {
            killGraceMs: 20,
          }),
        ),
      ).catch((error: unknown) => error);

      await vi.advanceTimersByTimeAsync(30_000);
      const failure = await failurePromise;

      expect(failure).toBeInstanceOf(RxChildProcessError);
      expect((failure as RxChildProcessError).kind).toBe("timeout");
    } finally {
      vi.useRealTimers();
    }
  });
});

describe("命令、Node 代码与模块", () => {
  it("rxExec 使用默认选项", async () => {
    const result = await firstValueFrom(rxCollect(rxExec("echo default-exec")));
    expect(text(result.stdout)).toContain("default-exec");
  });

  it("rxExec 通过 shell 执行命令", async () => {
    const result = await firstValueFrom(
      rxCollect(rxExec("echo rx-shell-ok", { timeoutMs: 2_000 })),
    );
    expect(text(result.stdout)).toContain("rx-shell-ok");
  });

  it("rxExecFile 将 shell 元字符作为普通参数", async () => {
    const value = "a; echo injected";
    const result = await firstValueFrom(
      rxCollect(
        rxExecFile(
          process.execPath,
          ["-e", "process.stdout.write(process.argv[1])", value],
          { timeoutMs: 2_000 },
        ),
      ),
    );
    expect(text(result.stdout)).toBe(value);
  });

  it("rxNodeCode 不经过 shell 并传递脚本参数", async () => {
    const value = "b; echo injected";
    const result = await firstValueFrom(
      rxCollect(
        rxNodeCode("process.stdout.write(process.argv[1])", [value], {
          timeoutMs: 2_000,
        }),
      ),
    );
    expect(text(result.stdout)).toBe(value);
  });

  it("rxNodeCode 支持省略参数和选项", async () => {
    const result = await firstValueFrom(
      rxCollect(rxNodeCode("process.stdout.write('minimal')")),
    );
    expect(text(result.stdout)).toBe("minimal");
  });

  it("rxFork 收集 IPC 消息并允许手动发送", async () => {
    const messages: unknown[] = [];
    const result = await new Promise<RxProcessEvent<unknown, Buffer | string>[]>(
      (resolve, reject) => {
        const events: Array<RxProcessEvent<unknown, Buffer | string>> = [];
        let child: ChildProcess | undefined;
        const subscription = rxFork(fixture("ipc-child.mjs"), [], {
          timeoutMs: 2_000,
        }).subscribe({
          next(event) {
            events.push(event);
            if (event.type === "spawn") {
              child = event.child;
              event.child.send("ping");
            } else if (event.type === "message") {
              messages.push(event.message);
              if (messages.length >= 2) {
                child?.send?.("exit");
              }
            }
          },
          error: reject,
          complete: () => resolve(events as never),
        });
        return () => subscription.unsubscribe();
      },
    );

    expect(result.some((event) => event.type === "close")).toBe(true);
    expect(messages).toEqual(
      expect.arrayContaining([
        { type: "ready" },
        { type: "reply", value: "ping" },
      ]),
    );
  });

  it("rxCollect 聚合 fork IPC 消息", async () => {
    const result = await firstValueFrom(
      rxCollect(
        rxFork(fixture("ipc-child.mjs"), [], {
          env: { ...process.env, AUTO_EXIT: "1" },
          timeoutMs: 2_000,
        }),
      ),
    );
    expect(result.messages).toContainEqual({ type: "ready" });
  });

  it("rxFork 支持省略参数、选项及显式 stdio", async () => {
    const defaultResult = await firstValueFrom(
      rxCollect(rxFork(fixture("ipc-burst.mjs"))),
    );
    expect(defaultResult.messages).toHaveLength(5);

    const explicitResult = await firstValueFrom(
      rxCollect(
        rxFork(fixture("ipc-burst.mjs"), undefined, {
          stdio: ["ignore", "pipe", "pipe", "ipc"],
          timeoutMs: 2_000,
        }),
      ),
    );
    expect(explicitResult.messages).toHaveLength(5);
  });

  it("命令元数据保留 cwd", async () => {
    const cwd = tmpdir();
    const result = await firstValueFrom(
      rxCollect(
        rxSpawn(
          process.execPath,
          ["-e", "process.stdout.write(process.cwd())"],
          { cwd, timeoutMs: 2_000 },
        ),
      ),
    );
    expect(result.command.cwd).toBe(cwd);
    expect(await realpath(text(result.stdout))).toBe(await realpath(cwd));
  });

  it("rxNodeModule 使用 fork 语义并传递参数", async () => {
    const module = join(tmpdir(), `rx-node-module-${Date.now()}.mjs`);
    await writeFile(
      module,
      "process.stdout.write(process.argv.slice(2).join('|'));\n",
    );
    try {
      const result = await firstValueFrom(
        rxCollect(
          rxNodeModule(module, ["one", "two"], { timeoutMs: 2_000 }),
        ),
      );
      expect(text(result.stdout)).toBe("one|two");
    } finally {
      await rm(module, { force: true });
    }
  });
});
