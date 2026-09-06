import { readFileSync } from "node:fs";
import { expect, it } from "vitest";
import { checkTooltipPolicy } from "../scripts/check-tooltip-policy.mjs";

it("accepts the production Copy/Copied overlay", () => {
  const source = readFileSync(new URL("../src/ui/CopyButton.tsx", import.meta.url), "utf8");
  expect(checkTooltipPolicy("ui/CopyButton.tsx", source)).toEqual([]);
});

it.each(["ui/OtherButton.tsx", "features/CopyButton.tsx"])("rejects a copied overlay in %s", path => {
  const source = readFileSync(new URL("../src/ui/CopyButton.tsx", import.meta.url), "utf8");
  expect(checkTooltipPolicy(path, source)).toHaveLength(1);
});

it("does not exempt arbitrary visual tooltips inside CopyButton", () => {
  expect(checkTooltipPolicy("ui/CopyButton.tsx", '<span class="other-tooltip" role="tooltip" />')).toHaveLength(1);
});

it("keeps accessible native-title descriptions allowed", () => {
  expect(checkTooltipPolicy("ui/Help.tsx", '<span title="Help"><span class="visually-hidden" role="tooltip">Help</span></span>')).toEqual([]);
});

it("rejects a competing native title on the shared copy component", () => {
  expect(checkTooltipPolicy("ui/CopyButton.tsx", '<Button title="Copy" />')).toHaveLength(1);
});

it.each(["ui/CopyButton.tsx", "ui/Help.tsx"])("still rejects a second data-tooltip renderer in %s", path => {
  expect(checkTooltipPolicy(path, '<span data-tooltip="Copy" />')).toHaveLength(1);
});

it("keeps CSS-painted native-title duplicates forbidden", () => {
  expect(checkTooltipPolicy("ui/CopyButton.tsx", 'const css = "content: attr(title)";')).toHaveLength(1);
});
