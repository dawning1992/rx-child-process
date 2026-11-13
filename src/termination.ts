import { spawn } from "node:child_process";
import type { ChildProcess } from "node:child_process";

/** 终止请求所需的参数。 */
export interface TerminationOptions {
  readonly child: ChildProcess;
  readonly killTree: boolean;
  readonly graceMs: number;
}

/** 平台相关终止依赖，测试可覆盖平台与信号实现。 */
export interface TerminationRuntime {
  readonly platform: NodeJS.Platform;
  readonly kill: (pid: number, signal: NodeJS.Signals | 0) => boolean;
  readonly spawn: typeof spawn;
}

const DEFAULT_RUNTIME: TerminationRuntime = {
  platform: process.platform,
  kill: (pid, signal) => process.kill(pid, signal),
  spawn,
};

/**
 * 幂等地请求终止子进程或进程树。
 *
 * POSIX 进程树模式依赖调用方使用独立进程组创建子进程。Windows 没有与
 * SIGTERM 等价的跨进程信号，因此直接使用 taskkill 强制结束进程树。
 */
export function terminateProcess(
  options: TerminationOptions,
  runtime: TerminationRuntime = DEFAULT_RUNTIME,
): void {
  const { child, killTree, graceMs } = options;
  const pid = child.pid;

  if (runtime.platform === "win32") {
    if (killTree && pid) {
      terminateWindowsTree(child, pid, runtime);
      return;
    }

    killChild(child, "SIGTERM");
    return;
  }

  if (killTree && pid) {
    sendPosixGroupSignal(child, pid, "SIGTERM", runtime);

    if (Number.isFinite(graceMs)) {
      const timer = setTimeout(() => {
        sendPosixGroupSignal(child, pid, "SIGKILL", runtime);
      }, Math.max(0, graceMs));
      timer.unref();
    }
    return;
  }

  killChild(child, "SIGTERM");

  if (Number.isFinite(graceMs)) {
    const timer = setTimeout(() => {
      if (child.exitCode === null && child.signalCode === null) {
        killChild(child, "SIGKILL");
      }
    }, Math.max(0, graceMs));
    timer.unref();
  }
}

function terminateWindowsTree(
  child: ChildProcess,
  pid: number,
  runtime: TerminationRuntime,
): void {
  try {
    const killer = runtime.spawn(
      "taskkill",
      ["/PID", String(pid), "/T", "/F"],
      {
        stdio: "ignore",
        windowsHide: true,
      },
    );

    killer.once("error", () => {
      killChild(child, "SIGKILL");
    });
    killer.once("close", (code) => {
      if (code !== 0) {
        killChild(child, "SIGKILL");
      }
    });
    killer.unref();
  } catch {
    killChild(child, "SIGKILL");
  }
}

function sendPosixGroupSignal(
  child: ChildProcess,
  pid: number,
  signal: NodeJS.Signals,
  runtime: TerminationRuntime,
): boolean {
  try {
    runtime.kill(-pid, signal);
    return true;
  } catch (error) {
    if (isMissingProcess(error)) {
      return false;
    }

    return killChild(child, signal);
  }
}

function killChild(child: ChildProcess, signal: NodeJS.Signals): boolean {
  try {
    return child.kill(signal);
  } catch {
    return false;
  }
}

function isMissingProcess(error: unknown): boolean {
  return (
    typeof error === "object" &&
    error !== null &&
    "code" in error &&
    (error as { code?: unknown }).code === "ESRCH"
  );
}
