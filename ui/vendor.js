import { copyFile, cp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { build } from "esbuild";

const uiDir = dirname(fileURLToPath(import.meta.url));
const require = createRequire(import.meta.url);

// A separate destination lets check.js verify bytes without changing the tree.
export async function vendorAssets(outdir) {
  const { dependencies } = JSON.parse(await readFile(join(uiDir, "package.json")));
  await rm(outdir, { recursive: true, force: true });
  await mkdir(outdir, { recursive: true });
  await build({
    entryPoints: { "eventsource-parser": "eventsource-parser", highlight: "highlight.js/lib/common" },
    absWorkingDir: uiDir,
    bundle: true,
    format: "esm",
    outdir,
    logLevel: "warning",
  });

  const roots = Object.fromEntries(Object.keys(dependencies).map((name) => [
    name, dirname(require.resolve(`${name}/package.json`)),
  ]));
  for (const [name, source, target] of [
    ["bulma", "css/bulma.min.css", "bulma.min.css"],
    ["@creativebulma/bulma-tooltip", "dist/bulma-tooltip.min.css", "bulma-tooltip.min.css"],
    ["highlight.js", "styles/github.min.css", "highlight/github.min.css"],
    ["highlight.js", "styles/github-dark.min.css", "highlight/github-dark.min.css"],
    ["@fortawesome/fontawesome-free", "css/all.min.css", "fontawesome/css/all.min.css"],
  ]) {
    await mkdir(dirname(join(outdir, target)), { recursive: true });
    // Source maps are not shipped or fetched by the debugger.
    const css = await readFile(join(roots[name], source), "utf8");
    await writeFile(join(outdir, target), css.replace(/\/\*[#@] sourceMappingURL=.*?\*\//gs, ""));
  }
  await cp(join(roots["@fortawesome/fontawesome-free"], "webfonts"), join(outdir, "fontawesome/webfonts"), { recursive: true });
  await mkdir(join(outdir, "licenses"));
  for (const [name, root] of Object.entries(roots)) {
    await copyFile(join(root, name === "@fortawesome/fontawesome-free" ? "LICENSE.txt" : "LICENSE"),
      join(outdir, "licenses", `${name.replace("@", "").replace("/", "_")}.txt`));
  }
}

if (resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  await vendorAssets(fileURLToPath(new URL("../public/vendor/", import.meta.url)));
}
