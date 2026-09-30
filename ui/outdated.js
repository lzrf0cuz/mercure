import { execFileSync } from "node:child_process";
import { before } from "./cooldown.js";

try {
  execFileSync("npm", ["outdated", `--before=${before}`], {
    cwd: new URL(".", import.meta.url),
    encoding: "utf8",
    stdio: ["ignore", "pipe", "inherit"],
  });
} catch (e) {
  console.error(
    `${e.stdout}\nDependencies are outdated: run 'npm run upgrade' in ui/ and commit the result.`,
  );
  process.exit(1);
}
