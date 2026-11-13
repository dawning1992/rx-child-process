import { firstValueFrom } from "rxjs";
import { rxCollect, rxNodeCode } from "../src/index.js";

const result = await firstValueFrom(
  rxCollect(
    rxNodeCode(
      `
        const [name, count] = process.argv.slice(1);
        console.log(\`\${name} x \${count}\`);
      `,
      ["rx-child-process", "3"],
    ),
  ),
);

process.stdout.write(result.stdout);
