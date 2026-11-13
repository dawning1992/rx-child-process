process.send?.({ type: "ready" });

if (process.env.AUTO_EXIT === "1") {
  setTimeout(() => process.exit(0), 20);
}

process.on("message", (message) => {
  if (message === "exit") {
    process.exit(0);
    return;
  }

  process.send?.({ type: "reply", value: message });
});
