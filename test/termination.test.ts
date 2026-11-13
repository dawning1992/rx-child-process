import type { ChildProcess } from "node:child_process";
import { EventEmitter } from "node:events";
import { describe, expect, it, vi } from "vitest";
import {
  terminateProcess,
  type TerminationRuntime,
} from "../src/termination.js";
import { sleep } from "./helpers.js";

interface FakeChild extends ChildProcess {
  readonly kill: ReturnType<typeof vi.fn>;
  readonly pid: number;
}

function createChild(pid = 4321): FakeChild {
  return {
    pid,
    exitCode: null,
    signalCode: null,
    kill: vi.fn(() => true),
  } as unknown as FakeChild;
}

function createKiller() {
  const killer = new EventEmitter() as EventEmitter & {
    unref: ReturnType<typeof vi.fn>;
  };
  killer.unref = vi.fn();
  return killer;
}

function runtime(
  platform: NodeJS.Platform,
  spawnProcess: TerminationRuntime["spawn"],
  kill: TerminationRuntime["kill"] = vi.fn(() => true),
): TerminationRuntime {
  return {
    platform,
    spawn: spawnProcess,
    kill,
  };
}

describe("进程终止", () => {
  it("Windows 进程树通过 taskkill 清理", () => {
    const child = createChild();
    const killer = createKiller();
    const spawnProcess = vi.fn(() => killer) as unknown as TerminationRuntime["spawn"];

    terminateProcess(
      { child, killTree: true, graceMs: 10 },
      runtime("win32", spawnProcess),
    );

    expect(spawnProcess).toHaveBeenCalledWith(
      "taskkill",
      ["/PID", "4321", "/T", "/F"],
      expect.objectContaining({ stdio: "ignore" }),
    );
    killer.emit("close", 0);
    expect(child.kill).not.toHaveBeenCalled();
  });

  it("taskkill 失败时回退到主进程 SIGKILL", () => {
    const child = createChild();
    const killer = createKiller();
    const spawnProcess = vi.fn(() => killer) as unknown as TerminationRuntime["spawn"];

    terminateProcess(
      { child, killTree: true, graceMs: 10 },
      runtime("win32", spawnProcess),
    );
    killer.emit("error", new Error("missing taskkill"));
    expect(child.kill).toHaveBeenCalledWith("SIGKILL");
  });

  it("taskkill 非零退出时回退到主进程 SIGKILL", () => {
    const child = createChild();
    const killer = createKiller();
    const spawnProcess = vi.fn(() => killer) as unknown as TerminationRuntime["spawn"];

    terminateProcess(
      { child, killTree: true, graceMs: 10 },
      runtime("win32", spawnProcess),
    );
    killer.emit("close", 1);
    expect(child.kill).toHaveBeenCalledWith("SIGKILL");
  });

  it("taskkill 无法创建时直接终止主进程", () => {
    const child = createChild();
    const spawnProcess = vi.fn(() => {
      throw new Error("cannot spawn");
    }) as unknown as TerminationRuntime["spawn"];

    terminateProcess(
      { child, killTree: true, graceMs: 10 },
      runtime("win32", spawnProcess),
    );
    expect(child.kill).toHaveBeenCalledWith("SIGKILL");
  });

  it("Windows 关闭 killTree 时仅终止主进程", () => {
    const child = createChild();
    terminateProcess(
      { child, killTree: false, graceMs: 10 },
      runtime("win32", vi.fn() as unknown as TerminationRuntime["spawn"]),
    );
    expect(child.kill).toHaveBeenCalledWith("SIGTERM");
  });

  it("POSIX 进程树先 TERM 后升级 KILL", async () => {
    const child = createChild();
    const kill = vi.fn(() => true);

    terminateProcess(
      { child, killTree: true, graceMs: 1 },
      runtime("darwin", vi.fn() as unknown as TerminationRuntime["spawn"], kill),
    );
    expect(kill).toHaveBeenCalledWith(-4321, "SIGTERM");
    await sleep(10);
    expect(kill).toHaveBeenCalledWith(-4321, "SIGKILL");
  });

  it("进程组不存在时保持幂等", async () => {
    const child = createChild();
    const missing = Object.assign(new Error("gone"), { code: "ESRCH" });
    const kill = vi.fn(() => {
      throw missing;
    });

    terminateProcess(
      { child, killTree: true, graceMs: 0 },
      runtime("linux", vi.fn() as unknown as TerminationRuntime["spawn"], kill),
    );
    await sleep(5);
    expect(child.kill).not.toHaveBeenCalled();
  });

  it("进程组信号失败时回退到主进程", () => {
    const child = createChild();
    const denied = Object.assign(new Error("denied"), { code: "EPERM" });
    const kill = vi.fn(() => {
      throw denied;
    });

    terminateProcess(
      { child, killTree: true, graceMs: Number.POSITIVE_INFINITY },
      runtime("linux", vi.fn() as unknown as TerminationRuntime["spawn"], kill),
    );
    expect(child.kill).toHaveBeenCalledWith("SIGTERM");
  });

  it("关闭 killTree 后在宽限期升级主进程信号", async () => {
    const child = createChild();
    terminateProcess(
      { child, killTree: false, graceMs: 1 },
      runtime("linux", vi.fn() as unknown as TerminationRuntime["spawn"]),
    );
    expect(child.kill).toHaveBeenCalledWith("SIGTERM");
    await sleep(10);
    expect(child.kill).toHaveBeenCalledWith("SIGKILL");
  });

  it("底层 kill 抛错时不会泄漏异常", () => {
    const child = createChild();
    child.kill.mockImplementation(() => {
      throw new Error("cannot kill");
    });

    expect(() =>
      terminateProcess(
        { child, killTree: false, graceMs: Number.POSITIVE_INFINITY },
        runtime("linux", vi.fn() as unknown as TerminationRuntime["spawn"]),
      ),
    ).not.toThrow();
  });
});
