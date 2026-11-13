import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";

process.on("SIGTERM", () => {});

const childPath = fileURLToPath(new URL("./tree-child.mjs", import.meta.url));
spawn(process.execPath, [childPath], {
  stdio: "ignore",
  env: process.env,
});

process.stdout.write("ready\n");
setInterval(() => {}, 1_000);
