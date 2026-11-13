import { performance } from "node:perf_hooks";
import { lastValueFrom, toArray } from "rxjs";
import { rxCollect, rxSpawn } from "../src/index.js";

const iterations = 20_000;
const script = `
  const line = "x".repeat(31) + "\\n";
  for (let index = 0; index < ${iterations}; index += 1) {
    process.stdout.write(line);
  }
`;

async function measure(
  name: string,
  action: () => Promise<unknown>,
): Promise<void> {
  const startedAt = performance.now();
  const result = await action();
  const elapsed = performance.now() - startedAt;
  const summary =
    result instanceof Array ? `${result.length} 个事件` : "1 个结果";
  process.stdout.write(
    `${name.padEnd(18)} ${elapsed.toFixed(1).padStart(8)} ms  ${summary}\n`,
  );
}

await measure("逐块事件", async () =>
  lastValueFrom(
    rxSpawn(process.execPath, ["-e", script], {
      maxStdoutBytes: 0,
      timeoutMs: 0,
    }).pipe(toArray()),
  ),
);

await measure("批处理事件", async () =>
  lastValueFrom(
    rxSpawn(process.execPath, ["-e", script], {
      maxStdoutBytes: 0,
      timeoutMs: 0,
      batching: { maxBytes: 64 * 1024, flushIntervalMs: 10 },
    }).pipe(toArray()),
  ),
);

await measure("rxCollect", async () =>
  lastValueFrom(
    rxCollect(
      rxSpawn(process.execPath, ["-e", script], {
        maxStdoutBytes: 0,
        timeoutMs: 0,
      }),
    ),
  ),
);
