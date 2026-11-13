import { spawnSync } from "node:child_process";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, dirname, isAbsolute, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const rootDir = resolve(dirname(fileURLToPath(import.meta.url)), "..");

/**
 * 执行外部命令并在失败时保留可诊断输出。
 *
 * Windows 下 npm 和 pnpm 是 `.cmd` 包装器，需要补全扩展名并交给命令
 * 解释器启动。这里的命令和参数均由脚本生成，不接收外部输入。
 *
 * @param {string} command 不含平台扩展名的命令名称。
 * @param {readonly string[]} args 命令参数。
 * @param {string} cwd 命令工作目录。
 * @returns {string} 去除首尾空白后的标准输出。
 */
function run(command, args, cwd) {
  const isWindows = process.platform === "win32";
  const isWindowsWrapper =
    isWindows && (command === "npm" || command === "pnpm");
  const executable = isWindowsWrapper ? `${command}.cmd` : command;
  const result = spawnSync(executable, args, {
    cwd,
    encoding: "utf8",
    env: process.env,
    shell: isWindowsWrapper,
  });

  if (result.error) {
    throw result.error;
  }

  if (result.status !== 0) {
    const commandLine = [executable, ...args].join(" ");
    throw new Error(
      [
        `命令执行失败（退出码 ${result.status ?? "unknown"}）：${commandLine}`,
        result.stdout.trim(),
        result.stderr.trim(),
      ]
        .filter(Boolean)
        .join("\n"),
    );
  }

  return result.stdout.trim();
}

/**
 * 验证打包产物能够被真实消费者通过 ESM 和 CJS 入口加载。
 *
 * 安装发生在系统临时目录中，不读取工作区的 `dist` 或 `node_modules`。
 * 无论成功与否都会清理临时目录；调用方必须先完成构建。
 */
async function main() {
  const packageJson = JSON.parse(
    await readFile(join(rootDir, "package.json"), "utf8"),
  );
  const rxjsRange = packageJson.peerDependencies?.rxjs;

  if (typeof rxjsRange !== "string") {
    throw new Error("package.json 缺少 rxjs peer dependency");
  }

  const workspaceDir = await mkdtemp(
    join(tmpdir(), "rx-child-process-smoke-"),
  );

  try {
    const packDir = join(workspaceDir, "pack");
    const consumerDir = join(workspaceDir, "consumer");
    await Promise.all([
      mkdir(packDir, { recursive: true }),
      mkdir(consumerDir, { recursive: true }),
    ]);

    const packOutput = run(
      "pnpm",
      ["pack", "--json", "--pack-destination", packDir],
      rootDir,
    );
    const packed = JSON.parse(packOutput);
    const tarballPath = isAbsolute(packed.filename)
      ? packed.filename
      : resolve(rootDir, packed.filename);
    const tarballDependency = `file:${join(
      "..",
      "pack",
      basename(tarballPath),
    ).replaceAll("\\", "/")}`;
    const packedFiles = new Set(
      packed.files.map((file) => file.path),
    );

    for (const requiredFile of [
      "dist/index.js",
      "dist/index.cjs",
      "dist/index.d.ts",
      "dist/index.d.cts",
      "package.json",
    ]) {
      if (!packedFiles.has(requiredFile)) {
        throw new Error(`打包产物缺少 ${requiredFile}`);
      }
    }

    await Promise.all([
      writeFile(
        join(consumerDir, "package.json"),
        JSON.stringify(
          {
            name: "rx-child-process-package-smoke",
            private: true,
            type: "module",
            dependencies: {
              "rx-child-process": tarballDependency,
              rxjs: rxjsRange,
            },
          },
          null,
          2,
        ),
      ),
      writeFile(
        join(consumerDir, "smoke.mjs"),
        [
          'import { rxCollect, rxSpawn } from "rx-child-process";',
          "",
          'if (typeof rxSpawn !== "function" || typeof rxCollect !== "function") {',
          '  throw new Error("ESM 包入口缺少预期导出");',
          "}",
          "",
        ].join("\n"),
      ),
      writeFile(
        join(consumerDir, "smoke.cjs"),
        [
          'const { rxCollect, rxSpawn } = require("rx-child-process");',
          "",
          'if (typeof rxSpawn !== "function" || typeof rxCollect !== "function") {',
          '  throw new Error("CJS 包入口缺少预期导出");',
          "}",
          "",
        ].join("\n"),
      ),
    ]);

    run(
      "npm",
      ["install", "--no-audit", "--no-fund", "--ignore-scripts"],
      consumerDir,
    );

    run(process.execPath, [join(consumerDir, "smoke.mjs")], consumerDir);
    run(process.execPath, [join(consumerDir, "smoke.cjs")], consumerDir);
    console.log("打包后 ESM/CJS 包名导入冒烟测试通过");
  } finally {
    await rm(workspaceDir, { recursive: true, force: true });
  }
}

await main();
