import { mkdir, copyFile } from "node:fs/promises";
import { join } from "node:path";
import { build } from "esbuild";

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
await Promise.all([
  copyFile("src/index.html", join(outputDirectory, "index.html")),
  copyFile("src/style.css", join(outputDirectory, "style.css")),
]);
