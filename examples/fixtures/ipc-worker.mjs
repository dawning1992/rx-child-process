process.send?.({ type: "ready" });

process.on("message", (message) => {
  if (message === "shutdown") {
    process.exit(0);
    return;
  }

  process.send?.({
    type: "reply",
    received: message,
    pid: process.pid,
  });
});
