process.stdout.write("before-signal\n");
process.kill(process.pid, "SIGTERM");
