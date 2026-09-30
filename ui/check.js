import assert from "node:assert/strict";
import { mkdtemp, readdir, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { vendorAssets } from "./vendor.js";

const publicDir = fileURLToPath(new URL("../public/", import.meta.url));

const inventory = async (dir) =>
  (await readdir(dir, { recursive: true, withFileTypes: true }))
    .filter((f) => f.isFile())
    .map((f) => join(f.parentPath, f.name).slice(dir.length + 1))
    .sort();

// Vendor into a scratch directory and compare with public/vendor/, so the check
// neither rewrites the tree nor needs public/vendor/ to be committed yet.
const stale =
  "public/vendor/ doesn't match package-lock.json: run 'npm run vendor' in ui/ and commit the result.";
const temp = await mkdtemp(join(tmpdir(), "mercure-ui-check-"));
let files;
try {
  const expected = join(temp, "vendor");
  await vendorAssets(expected);
  const actual = join(publicDir, "vendor");
  files = await inventory(expected);
  assert.deepEqual(await inventory(actual), files, stale);
  for (const file of files) {
    assert.deepEqual(await readFile(join(actual, file)), await readFile(join(expected, file)), `${file}: ${stale}`);
  }
} finally {
  await rm(temp, { recursive: true, force: true });
}

// The fork vendors stylesheets that load fonts through url(): the UI must not
// fetch a CDN stylesheet, script, font, or module, and every HTML asset and CSS
// url() must resolve to a shipped local file.
const html = await readFile(join(publicDir, "index.html"), "utf8");
assert.doesNotMatch(html, /(?:src|href)=["']https?:[^"']+["'][^>]*(?:rel=["']stylesheet|type=["']module)|<(?:script|link|img)[^>]+(?:src|href)=["']https?:/i);
assert.doesNotMatch(html, /\s(?:style|on\w+)=/i, "inline assets violate the debugger CSP");
assert.doesNotMatch(html, /<script\b(?![^>]*\bsrc=)[^>]*>|<style\b/i, "inline scripts and styles violate the debugger CSP");
for (const file of ["app.js", "app.css", "fallback.js", ...files.filter((f) => f.endsWith(".css")).map((f) => `vendor/${f}`)]) {
  const source = await readFile(join(publicDir, file), "utf8");
  assert.doesNotMatch(source, /(?:from\s*|@import\s*(?:url\()?|url\()["']?https?:/i, `${file} loads an external asset`);
}
for (const match of html.matchAll(/<(?:script|link|img)\b[^>]*\b(?:src|href)=["']([^"']+)["']/gi)) {
  await readFile(new URL(match[1], new URL("../public/", import.meta.url)));
}
for (const file of files.filter((f) => f.endsWith(".css"))) {
  const url = new URL(`../public/vendor/${file}`, import.meta.url);
  for (const match of (await readFile(url, "utf8")).matchAll(/url\(["']?([^)'"\s]+)["']?\)/g)) {
    if (!match[1].startsWith("data:")) await readFile(new URL(match[1].split(/[?#]/)[0], url));
  }
}
