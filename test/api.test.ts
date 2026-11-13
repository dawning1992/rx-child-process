import type { ChildProcess } from "node:child_process";
import { firstValueFrom, lastValueFrom, Observable, of, toArray } from "rxjs";
import { describe, expect, expectTypeOf, it } from "vitest";
import {
  filterEvent,
  isChildProcessErrorKind,
  isCloseEvent,
  isDisconnectEvent,
  isExitEvent,
  isMessageEvent,
  isOutputEvent,
  isRxChildProcessError,
  isRxEventType,
  isSpawnEvent,
  isStderrEvent,
  isStdoutEvent,
  RxChildProcessError,
  RxErrorKind,
  RxEventType,
  type RxEventType as RxEventTypeName,
  RxLimits,
  rxCollect,
  rxExecFile,
  rxFork,
  rxNodeCode,
  rxNodeModule,
  rxSpawn,
  type RxProcessEvent,
  type RxSpawnEvent,
  type RxStdoutEvent,
} from "../src/index.js";
import { fixture } from "./helpers.js";

const command = {
  file: "node",
  args: [],
  shell: false,
} as const;

const child = {} as ChildProcess;

describe("公共辅助 API", () => {
  it("导出语义化常量和资源限制值", () => {
    const eventType: RxEventTypeName = RxEventType.Stdout;
    expect(RxEventType.Spawn).toBe("spawn");
    expect(eventType).toBe("stdout");
    expect(RxEventType.Stdout).toBe("stdout");
    expect(RxErrorKind.Abort).toBe("abort");
    expect(RxErrorKind.Timeout).toBe("timeout");
    expect(RxLimits.Disabled).toBe(Number.POSITIVE_INFINITY);
    expect(isRxEventType("stdout")).toBe(true);
    expect(isRxEventType("error")).toBe(false);
    expect(isChildProcessErrorKind("abort")).toBe(true);
    expect(isChildProcessErrorKind("unknown")).toBe(false);
  });

  it("事件守卫覆盖全部生命周期", () => {
    const spawn: RxSpawnEvent = {
      type: RxEventType.Spawn,
      pid: 123,
      child,
      command,
      encoding: null,
    };

    expect(isSpawnEvent(spawn)).toBe(true);
    expect(
      isStdoutEvent({ type: RxEventType.Stdout, chunk: Buffer.from("a") }),
    ).toBe(true);
    expect(
      isStderrEvent({ type: RxEventType.Stderr, chunk: Buffer.from("b") }),
    ).toBe(true);
    expect(
      isOutputEvent({ type: RxEventType.Stderr, chunk: Buffer.from("b") }),
    ).toBe(true);
    expect(
      isMessageEvent({ type: RxEventType.Message, message: { ok: true } }),
    ).toBe(true);
    expect(
      isDisconnectEvent({ type: RxEventType.Disconnect }),
    ).toBe(true);
    expect(
      isExitEvent({ type: RxEventType.Exit, code: 0, signal: null }),
    ).toBe(true);
    expect(
      isCloseEvent({
        type: RxEventType.Close,
        code: 0,
        signal: null,
        durationMs: 1,
      }),
    ).toBe(true);
  });

  it("filterEvent 保留通道、消息和输出类型", async () => {
    type Message = { readonly kind: "ping" };
    const source = of(
      { type: "stdout", chunk: "out" },
      { type: "stderr", chunk: "err" },
      { type: "message", message: { kind: "ping" } as Message },
      { type: "exit", code: 0, signal: null },
    ) as Observable<RxProcessEvent<Message, string>>;

    expectTypeOf(
      source.pipe(filterEvent(RxEventType.Stdout)),
    ).toEqualTypeOf<Observable<RxStdoutEvent<string>>>();

    const events = await lastValueFrom(
      source.pipe(filterEvent(RxEventType.Stdout), toArray()),
    );
    expect(events).toEqual([{ type: "stdout", chunk: "out" }]);
  });

  it("错误守卫不依赖原型链并支持分类校验", () => {
    const error = new RxChildProcessError("cancelled", {
      kind: RxErrorKind.Abort,
    });
    const duplicatedPackageError = {
      name: "RxChildProcessError",
      message: "timeout",
      kind: "timeout",
    };

    expect(isRxChildProcessError(error)).toBe(true);
    expect(isRxChildProcessError(error, RxErrorKind.Abort)).toBe(true);
    expect(isRxChildProcessError(error, RxErrorKind.Timeout)).toBe(false);
    expect(isRxChildProcessError(duplicatedPackageError)).toBe(true);
    expect(
      isRxChildProcessError(duplicatedPackageError, RxErrorKind.Timeout),
    ).toBe(true);
    expect(isRxChildProcessError(new Error("plain"))).toBe(false);
    expect(isRxChildProcessError(null)).toBe(false);
    expect(isRxChildProcessError({ name: "RxChildProcessError" })).toBe(false);
  });

  it("成功 spawn 事件的 pid 类型为必填 number", () => {
    expectTypeOf<RxSpawnEvent["pid"]>().toEqualTypeOf<number>();
  });
});

describe("省略参数时直接传入 options", () => {
  it("全部 entries 的 options 重载保留字符串输出类型", () => {
    expectTypeOf(
      rxSpawn("command", { encoding: "utf8" }),
    ).toEqualTypeOf<Observable<RxProcessEvent<unknown, string>>>();
    expectTypeOf(
      rxExecFile("command", { encoding: "utf8" }),
    ).toEqualTypeOf<Observable<RxProcessEvent<unknown, string>>>();
    expectTypeOf(
      rxFork("module.mjs", { encoding: "utf8" }),
    ).toEqualTypeOf<Observable<RxProcessEvent<unknown, string>>>();
    expectTypeOf(
      rxNodeModule("module.mjs", { encoding: "utf8" }),
    ).toEqualTypeOf<Observable<RxProcessEvent<unknown, string>>>();
  });

  it("rxSpawn 支持第二参数 options", async () => {
    const result = await firstValueFrom(
      rxCollect(
        rxSpawn(process.execPath, {
          input: "process.exit(0)\n",
          timeoutMs: 2_000,
        }),
      ),
    );
    expect(result.code).toBe(0);
  });

  it("rxExecFile 支持第二参数 options", async () => {
    const result = await firstValueFrom(
      rxCollect(
        rxExecFile(process.execPath, {
          timeoutMs: 2_000,
        }),
      ),
    );
    expect(result.code).toBe(0);
  });

  it("rxNodeCode 支持 options 并保留字符串输出类型", async () => {
    const source = rxNodeCode("process.stdout.write('ok')", {
      encoding: "utf8",
      timeoutMs: 2_000,
    });
    expectTypeOf(source).toEqualTypeOf<
      Observable<RxProcessEvent<unknown, string>>
    >();
    const result = await firstValueFrom(rxCollect(source));
    expect(result.stdout).toBe("ok");
  });

  it("rxFork 和 rxNodeModule 支持第二参数 options", async () => {
    const options = {
      env: { ...process.env, AUTO_EXIT: "1" },
      timeoutMs: 2_000,
    };
    const result = await firstValueFrom(
      rxCollect(rxFork(fixture("ipc-child.mjs"), options)),
    );
    expect(result.messages).toContainEqual({ type: "ready" });

    const moduleResult = await firstValueFrom(
      rxCollect(
        rxNodeModule(fixture("ipc-burst.mjs"), { timeoutMs: 2_000 }),
      ),
    );
    expect(moduleResult.messages).toHaveLength(5);
  });

  it("entries 均允许只传参数数组并省略 options", () => {
    const sources = [
      rxSpawn("command", ["arg"]),
      rxExecFile("command", ["arg"]),
      rxFork("module.mjs", ["arg"]),
      rxNodeCode("process.exit(0)", ["arg"]),
      rxNodeModule("module.mjs", ["arg"]),
    ];

    for (const source of sources) {
      expect(source).toBeInstanceOf(Observable);
    }
  });
});
