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
