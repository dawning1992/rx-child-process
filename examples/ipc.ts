import type { ChildProcess } from "node:child_process";
import { fileURLToPath } from "node:url";
import {
  isCloseEvent,
  isMessageEvent,
  isSpawnEvent,
  rxFork,
  type RxProcessEvent,
} from "../src/index.js";

const worker = fileURLToPath(
  new URL("./fixtures/ipc-worker.mjs", import.meta.url),
);

await new Promise<void>((resolve, reject) => {
  let child: ChildProcess | undefined;

  rxFork(worker, { timeoutMs: 0 }).subscribe({
    next(event: RxProcessEvent<unknown, Buffer | string>) {
      if (isSpawnEvent(event)) {
        child = event.child;
        event.child.send("ping");
      } else if (isMessageEvent(event)) {
        process.stdout.write(`${JSON.stringify(event.message)}\n`);
        if (
          typeof event.message === "object" &&
          event.message !== null &&
          "type" in event.message &&
          event.message.type === "reply"
        ) {
          child?.send("shutdown");
        }
      } else if (isCloseEvent(event)) {
        if (event.code === 0) {
          resolve();
        } else {
          reject(new Error(`worker exited with ${event.code}`));
        }
      }
    },
    error: reject,
  });
});
