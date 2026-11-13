for (let index = 0; index < 5; index += 1) {
  process.send?.({ index });
}
setTimeout(() => process.exit(0), 10);
