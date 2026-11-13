import { rxSpawn, type RxProcessEvent } from "../src/index.js";

const source = rxSpawn(
  process.execPath,
  [
    "-e",
    `
      let count = 0;
      const timer = setInterval(() => {
        process.stdout.write(\`实时输出 \${++count}\\n\`);
        if (count === 8) {
          clearInterval(timer);
        }
      }, 20);
    `,
  ],
  {
    batching: { maxBytes: 32, flushIntervalMs: 20 },
  },
);

source.subscribe({
  next(event: RxProcessEvent<unknown, Buffer | string>) {
    if (event.type === "stdout") {
      process.stdout.write(event.chunk);
    }
  },
  error(error) {
    console.error(error);
    process.exitCode = 1;
  },
});
