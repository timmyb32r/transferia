import { readFile, readdir } from "node:fs/promises";
import { extname, relative, resolve } from "node:path";

const root = resolve(import.meta.dirname, "../src");
const sourceFiles = await collect(root);
const violations = [];

await checkCssCustomProperties();
await checkStableInteractiveHitTargets();

for (const file of sourceFiles) {
  const source = await readFile(file, "utf8");
  const path = relative(root, file).replaceAll("\\", "/");
  const layer = path.split("/", 1)[0];
  for (const match of source.matchAll(/from\s+["']([^"']+)["']/g)) {
    const dependency = match[1];
    if (!dependency.startsWith(".")) continue;
    const target = relative(root, resolve(file, "..", dependency)).replaceAll(
      "\\",
      "/",
    );
    const targetLayer = target.split("/", 1)[0];
    if (
      layer === "ui" &&
      ["schema", "delivery", "features", "application"].includes(targetLayer)
    )
      violations.push(
        `${path}: ui must not import ${targetLayer} (${dependency})`,
      );
    if (
      layer === "schema" &&
      ["bootstrap", "delivery", "features", "infrastructure"].includes(
        targetLayer,
      )
    )
      violations.push(
        `${path}: schema must not import ${targetLayer} (${dependency})`,
      );
    if (
      layer === "application" &&
      ["schema", "delivery", "features", "ui", "infrastructure"].includes(
        targetLayer,
      )
    )
      violations.push(
        `${path}: application must not import ${targetLayer} (${dependency})`,
      );
  }
  if (
    path !== "infrastructure/controlPlane/httpControlPlane.ts" &&
    /\bfetch\s*\(/.test(source)
  )
    violations.push(`${path}: network access belongs behind ControlPlanePort`);
}

if (violations.length > 0) {
  console.error(`Frontend architecture violations:\n${violations.join("\n")}`);
  process.exitCode = 1;
} else {
  console.log(`Frontend architecture OK (${sourceFiles.length} source files)`);
}

async function collect(directory) {
  const files = [];
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const path = resolve(directory, entry.name);
    if (entry.isDirectory()) files.push(...(await collect(path)));
    else if ([".ts", ".tsx"].includes(extname(entry.name))) files.push(path);
  }
  return files;
}

async function checkCssCustomProperties() {
  const css = await readFile(resolve(root, "style.css"), "utf8");
  const declared = new Set(
    [...css.matchAll(/(--[a-z0-9-]+)\s*:/gi)].map((match) => match[1]),
  );
  const used = new Set(
    [...css.matchAll(/var\(\s*(--[a-z0-9-]+)/gi)].map((match) => match[1]),
  );
  for (const property of used) {
    if (!declared.has(property))
      violations.push(`style.css: custom property ${property} is not declared`);
  }
  const inspector = css.match(/\.schema-inspector\s*\{([^}]*)\}/)?.[1] ?? "";
  if (!inspector.includes("overflow: scroll"))
    violations.push("style.css: schema inspector must always reserve scrollbars");
  if (!inspector.includes("scrollbar-gutter: stable both-edges"))
    violations.push("style.css: schema inspector must reserve a stable scrollbar gutter");
  if (!inspector.includes("max-height: calc(100vh - 48px)"))
    violations.push(
      "style.css: schema inspector must keep its resize handle inside the initial viewport",
    );
  if (
    !inspector.includes("scrollbar-color: auto") ||
    !inspector.includes("scrollbar-width: auto")
  )
    violations.push(
      "style.css: schema inspector must let its permanent WebKit scrollbar styling take precedence",
    );
  const inspectorScrollbar =
    css.match(/\.schema-inspector::\-webkit-scrollbar\s*\{([^}]*)\}/)?.[1] ??
    "";
  if (!inspectorScrollbar.includes("display: block"))
    violations.push("style.css: schema inspector scrollbar must remain visible without hover");
}

async function checkStableInteractiveHitTargets() {
  const css = await readFile(resolve(root, "style.css"), "utf8");
  for (const match of css.matchAll(/([^{}]*:active[^{}]*)\{([^}]*)\}/g)) {
    if (/\b(transform|scale)\s*:/.test(match[2]))
      violations.push(
        `style.css: active control '${match[1].trim()}' must not resize its hit target`,
      );
  }
}
