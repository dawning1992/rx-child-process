const exitCode = Number(process.argv[2] ?? 0);

process.stdout.write("stdout-line\n");
process.stderr.write("stderr-line\n");
setTimeout(() => process.exit(exitCode), 15);
