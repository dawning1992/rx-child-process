import { writeFileSync } from "node:fs";

process.on("SIGTERM", () => {});
writeFileSync(process.env.MARKER, String(process.pid));
setInterval(() => {}, 1_000);
