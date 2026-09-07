import { readFileSync } from "node:fs";
import { expect, it } from "vitest";
import { checkSchemaInspectorLayout } from "../scripts/check-schema-inspector-layout.mjs";

const css = readFileSync(new URL("../src/style.css", import.meta.url), "utf8");
const changeRule = (selector, before, after) => css.replace(
  new RegExp(`(${selector.replaceAll(".", "\\.")}\\s*\\{)([^}]*)(\\})`),
  (_, start, body, end) => start + body.replace(before, after) + end,
);

it("accepts the production inspector with an inner scroll area and dynamic viewport bounds", () => {
  expect(checkSchemaInspectorLayout(css)).toEqual([]);
});

it("rejects scrolling the entire inspector instead of its table", () => {
  expect(checkSchemaInspectorLayout(changeRule(".schema-inspector", "overflow: hidden", "overflow: scroll")))
    .toContain("style.css: schema inspector must keep its toolbar outside the scrolling table");
});

it("rejects losing the table scroll area", () => {
  expect(checkSchemaInspectorLayout(changeRule(".schema-inspector-table", "overflow: auto", "overflow: visible")))
    .toContain("style.css: schema inspector table must scroll independently");
});

it("rejects losing the table's reserved scrollbar gutter", () => {
  expect(checkSchemaInspectorLayout(changeRule(".schema-inspector-table", "scrollbar-gutter: stable", "scrollbar-gutter: auto")))
    .toContain("style.css: schema inspector table must reserve a stable scrollbar gutter");
});

it("rejects a viewport bound that can leave the initial resize handle offscreen", () => {
  expect(checkSchemaInspectorLayout(changeRule(".schema-inspector", "max-height: calc(100dvh - 48px)", "max-height: 100dvh")))
    .toContain("style.css: schema inspector must keep its resize handle inside the initial viewport");
});

it("makes Cargo track every script used by the frontend build checks", () => {
  const build = readFileSync(new URL("../../crates/transferia-server-ui/build.rs", import.meta.url), "utf8");
  expect(build).toContain('println!("cargo:rerun-if-changed=../../web/scripts");');
});
