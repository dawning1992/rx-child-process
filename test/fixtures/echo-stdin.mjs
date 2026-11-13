const chunks = [];

process.stdin.on("data", (chunk) => chunks.push(chunk));
process.stdin.on("end", () => {
  process.stdout.write(Buffer.concat(chunks));
});
