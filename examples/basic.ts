import { firstValueFrom } from "rxjs";
import { rxCollect, rxSpawn } from "../src/index.js";

const result = await firstValueFrom(
  rxCollect(
    rxSpawn(process.execPath, [
      "-e",
      "console.log('hello from child'); console.error('diagnostic')",
    ]),
  ),
);

process.stdout.write(result.stdout);
process.stderr.write(result.stderr);
process.stdout.write(
  `退出码 ${result.code}，耗时 ${result.durationMs.toFixed(1)}ms\n`,
);
