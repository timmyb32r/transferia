import { mkdir, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { build } from "esbuild";

import { murmur3X64_128 } from "./murmur3-x64-128.mjs";

const outputDirectory = process.env.SERVER_UI_OUT_DIR ?? "dist";

await mkdir(outputDirectory, { recursive: true });
await build({
  entryPoints: ["src/app.tsx"],
  bundle: true,
  format: "iife",
  target: "es2022",
  minify: process.env.PROFILE === "release",
  sourcemap: process.env.PROFILE === "release" ? false : "inline",
  outfile: join(outputDirectory, "app.js"),
});
const [javascript, stylesheet, indexTemplate] = await Promise.all([
  readFile(join(outputDirectory, "app.js")),
  readFile("src/style.css"),
  readFile("src/index.html", "utf8"),
]);
const digest = (contents) => murmur3X64_128(contents).slice(0, 16);
const index = indexTemplate
  .replace("/app.js", `/app.js?v=${digest(javascript)}`)
  .replace("/style.css", `/style.css?v=${digest(stylesheet)}`);
await Promise.all([
  writeFile(join(outputDirectory, "index.html"), index),
  writeFile(join(outputDirectory, "style.css"), stylesheet),
]);
