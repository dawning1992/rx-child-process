import { firstValueFrom } from "rxjs";
import { RxChildProcessError, rxCollect, rxSpawn } from "../src/index.js";

const controller = new AbortController();
setTimeout(() => controller.abort(), 100);

try {
  await firstValueFrom(
    rxCollect(
      rxSpawn(
        process.execPath,
        ["-e", "setInterval(() => {}, 1000)"],
        {
          signal: controller.signal,
          timeoutMs: 0,
          killGraceMs: 50,
        },
      ),
    ),
  );
} catch (error) {
  if (error instanceof RxChildProcessError && error.kind === "abort") {
    process.stdout.write("子进程已取消并完成回收\n");
  } else {
    throw error;
  }
}
