const value = Buffer.from("你");
process.stdout.write(value.subarray(0, 1));
setTimeout(() => process.stdout.write(value.subarray(1)), 5);
