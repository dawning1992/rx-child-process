import { lastValueFrom } from "rxjs";
import { describe, expect, it, vi } from "vitest";

vi.mock("node:string_decoder", async (importOriginal) => {
  const actual = await importOriginal<typeof import("node:string_decoder")>();
  return {
    StringDecoder: class extends actual.StringDecoder {
      constructor(encoding?: BufferEncoding) {
        if (encoding === "utf8") {
          throw new Error("decoder unavailable");
        }
        super(encoding);
      }
    },
  };
});

import { execute } from "../src/runner.js";

describe("输出解码器兼容性", () => {
  it("StringDecoder 不兼容已识别编码时报告配置错误", async () => {
    const launch = vi.fn();
    const source = execute({
      command: {
        file: "fake-process",
        args: [],
        shell: false,
      },
      options: { encoding: "utf8" },
      launch,
      autoEndStdin: false,
      stdinPipeByDefault: true,
    });

    await expect(lastValueFrom(source)).rejects.toMatchObject({
      kind: "configuration",
      message: "无效的子进程配置",
      cause: {
        name: "TypeError",
        message: "当前 Node.js 不支持输出编码: utf8",
      },
    });
    expect(launch).not.toHaveBeenCalled();
  });
});
