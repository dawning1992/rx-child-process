import { fileURLToPath } from "node:url";

export const fixturesDir = fileURLToPath(
  new URL("./fixtures/", import.meta.url),
);

export function fixture(name: string): string {
  return `${fixturesDir}${name}`;
}

export function sleep(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

export function isProcessAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return !(
      typeof error === "object" &&
      error !== null &&
      "code" in error &&
      (error as { code?: unknown }).code === "ESRCH"
    );
  }
}

/**
 * 在限定时间内等待进程退出。
 *
 * Windows 的 `taskkill` 是异步启动的独立进程，固定等待时长容易受 CI 负载影响；
 * 调用方应使用该函数轮询进程状态，而不是依赖某个具体时钟间隔。
 */
export async function waitForProcessExit(
  pid: number,
  timeoutMs = 5_000,
): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (!isProcessAlive(pid)) {
      return true;
    }
    await sleep(20);
  }
  return !isProcessAlive(pid);
}
