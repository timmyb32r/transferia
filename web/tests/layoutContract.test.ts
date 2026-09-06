import { describe, expect, it } from "vitest";

const runtime = (
  globalThis as typeof globalThis & {
    process?: {
      getBuiltinModule?: (name: "fs") => {
        readFileSync: (path: URL, encoding: "utf8") => string;
      };
    };
  }
).process;
const styles =
  runtime?.getBuiltinModule?.("fs").readFileSync(
    new URL("../src/style.css", import.meta.url),
    "utf8",
  ) ?? "";

describe("delivery layout contract", () => {
  it("keeps the destination at content height while Source can stretch into its continuation", () => {
    expect(styles).not.toContain(".route-composition > .endpoint-card {\n  align-self: stretch;");
    expect(styles).toContain(".route-composition > .endpoint-card-source {\n  align-self: stretch;");
  });
  it("contains long schema names and types and anchors tabs inside the fixed inspector", () => {
    const rule = (selector: string) => styles.split(`${selector} {`)[1]?.split("}")[0];
    expect(rule(".schema-inspector")).toContain("height: min(560px, calc(100dvh - 48px));");
    expect(rule(".schema-inspector")).toContain("overflow: hidden;");
    expect(rule(".schema-inspector-content")).toContain("min-height: 0;");
    expect(rule(".schema-inspector-table")).toContain("overflow: auto;");
    expect(rule(".schema-inspector-row > *")).toContain("overflow-wrap: anywhere;");
    expect(rule(".schema-inspector-row > *")).toContain("min-width: 0;");
    expect(rule(".schema-inspector-type-tabs")).toContain("grid-template-columns: repeat(2, minmax(0, 1fr));");
  });
  it("puts source tables below both endpoints and compacts repeated rules without asynchronous movement", () => {
    expect(styles).toContain('"tables tables tables"');
    expect(styles).toContain(".source-tables-card { grid-area: tables;");
    expect(styles).toContain(":root .table-rule-compact .field-label { display: none; }");
    expect(styles).not.toContain(".table-rule-row ~ .table-rule-row { padding-top: 12px;");
    expect(styles).toContain("grid-template-rows: auto auto 40px 24px minmax(0, 1fr) 116px;");
    expect(styles).toMatch(/\.table-pattern-tooltip \{[^}]*position: fixed;[^}]*pointer-events: none;/);
  });
  it("joins Tables to Source like parser details instead of adding a separate island", () => {
    const tables = styles.split(".source-tables-card {")[1]?.split("}")[0];
    expect(tables).not.toMatch(/(?:margin|padding|background|border|box-shadow)\s*:/);
    expect(tables).not.toContain("margin-top:");
    const details = styles.split("\n.source-details-card {")[1]?.split("}")[0];
    expect(details).toContain("grid-column: 1 / -1;");
    expect(details).toContain("border-radius: 0 var(--radius-panel) var(--radius-panel);");
    expect(styles).toContain(".route-composition:has(> .source-details-card) > .endpoint-card-source {");
    const stacked = styles.split("@media (max-width: 1300px) {")[1];
    expect(stacked).toMatch(/grid-template-areas:\s*"source"\s*"tables"\s*"parser"\s*"arrow"\s*"sink"/);
  });
  it("reserves source metadata and exact-match slots and keeps the picker within narrow forms", () => {
    const rule = (selector: string) => styles.split(`${selector} {`)[1]?.split("}")[0];
    expect(rule(":root .available-tables-metadata > .table-matches-height-toggle")).toContain("height: 48px; width: 248px;");
    expect(rule(":root .available-tables-failures")).toContain("width: 56px; height: 18px;");
    expect(rule(".available-tables-summary")).toContain("height: 14px;");
    expect(rule(".available-table-row .available-table-schema")).toContain("flex: 0 0 70px;");
    expect(rule(".table-pattern-confirmation")).toContain("position: absolute;");
    expect(rule(".table-pattern-confirmation")).toContain("width: 22px;");
    expect(styles).toContain(".table-pattern-with-browser.table-pattern-with-confirmation input[type=\"text\"] { padding-right: calc(var(--control-height) * 2 + 22px); }");
    expect(styles).toContain("container: table-space / inline-size;");
    expect(rule(".table-rule-patterns.table-rule-with-exclude")).toContain("grid-template-columns: minmax(0, 1.15fr) minmax(0, 1fr) var(--control-height);");
    expect(rule(".table-rule-patterns")).toContain("grid-template-columns: minmax(0, 1fr) 78px var(--control-height);");
    expect(rule(".table-rule-patterns .field-label")).toContain("min-height: 20px;");
    expect(rule(".connection-dependent-fields")).toContain("padding: 8px;");
    expect(styles).toContain("@container table-space (max-width: 380px)");
    expect(styles).toContain(".table-rule-with-exclude > .icon-button { grid-column: 2; grid-row: 1; }");
    expect(styles).not.toContain(".table-rule-patterns .form-row:first-child { grid-column: 1 / -1; }");
    expect(rule(":root .matched-table-row .copy-action")).toContain("width: 24px; height: 24px;");
  });
  it("gives configuration and transform-preview tabs the same continuous slate backing", () => {
    const backing = styles.match(/:root\[data-design="airy-v0"\] \.editor-tabs,\s*:root\[data-design="airy-v0"\] \.transform-preview-tabs\s*\{([^}]+)\}/)?.[1];
    expect(backing).toContain("background: var(--panel2);");
    expect(backing).toContain("border-radius: 8px;");
    expect(backing).toContain("padding: 3px;");
    // The preview shares paint and the fixed inset, not the editor section's
    // outer margin. Switching tabs must not add/remove the backing.
    expect(backing).not.toContain("margin:");
    expect(styles).toContain('.transform-preview-tabs { display: flex; gap: 4px; align-items: center; }');
    const tabs = styles.split(":root .transform-preview-tabs button {")[1]?.split("}")[0];
    expect(tabs).toContain("flex: 0 0 120px;");
    expect(tabs).toContain("width: 120px;");
  });

  it("makes the transform disclosure fill the heading height without dead padding above or below it", () => {
    const rule = (selector: string) => styles.split(`${selector} {`)[1]?.split("}")[0];
    const heading = rule(".middleware-strip-heading");
    expect(heading).toContain("padding: 0 8px;");
    expect(heading).toContain("min-height: 58px;");
    expect(heading).toContain("align-items: center;");
    const toggle = rule(":root .middleware-strip-toggle");
    expect(toggle).toContain("align-self: stretch;");
    expect(toggle).toContain("min-height: 58px;");
    expect(toggle).toContain("height: auto;");
    expect(toggle).toContain("padding: 10px 8px;");
    // Moving padding inside the disclosure preserves the heading footprint and
    // leaves drag/clone/delete as independent, centered controls.
    expect(rule(":root .middleware-strip-heading .icon-button")).toContain("height: 32px;");
    expect(rule(":root .middleware-strip-heading .middleware-clone")).toContain("height: 32px;");
    for (const state of ["hover", "active"]) {
      expect(rule(`:root .middleware-strip-heading button:${state}:not(:disabled, .copy-action)`))
        .not.toMatch(/(?:width|height|padding|margin|transform|border-width)\s*:/);
    }
  });

  it("keeps Copy and Use aligned in a fixed-size action group beside available table names", () => {
    expect(styles).toMatch(/\.available-table-actions\s*\{[^}]*display: flex;[^}]*flex: 0 0 auto;[^}]*align-items: center;/s);
    expect(styles).toMatch(/:root \.available-table-use\s*\{[^}]*width: 44px;[^}]*min-width: 44px;[^}]*height: 28px;/s);
    expect(styles).toMatch(/\.available-tables-dialog\s*\{[^}]*height: min\(560px, calc\(100dvh - 48px\)\);/s);
  });
  it("shares neutral copy affordances without changing geometry between interaction states", () => {
    const rule = (selector: string) => styles.split(`${selector} {`)[1]?.split("}")[0];
    const idle = rule("button.copy-action");
    expect(idle).toContain("color: var(--text-secondary);");
    expect(idle).toContain("background: transparent;");
    expect(idle).toContain("border-color: transparent;");
    expect(rule(".copy-action.copy-action-framed")).toContain("border-color: var(--line-strong);");
    for (const state of ["hover", "active"]) {
      const paint = rule(`button.copy-action:${state}:not(:disabled, [aria-disabled=\"true\"])`);
      expect(paint).toContain("background:");
      expect(paint).not.toMatch(/(?:width|height|padding|margin|transform|border-width|font-size)\s*:/);
      expect(paint).not.toContain("var(--blue");
    }
    expect(rule(".transfer-id-copy.icon-button")).toContain("width: 24px;");
    expect(rule(".transfer-id-copy.icon-button")).toContain("height: 24px;");
    expect(rule(":root .available-table-row .icon-button")).toContain("width: 28px; height: 28px;");
    // The rear page is occluded by the front page, not drawn through it.
    expect(rule(".copy-icon::before")).toContain("clip-path: polygon(");
    expect(rule(".copy-icon-check")).toContain("position: absolute;");
    const tooltip = rule(".copy-tooltip");
    expect(tooltip).toContain("position: fixed;");
    expect(tooltip).toContain("pointer-events: none;");
    expect(tooltip).toContain("width: 100px;");
    expect(tooltip).toContain("height: 32px;");
  });
  it("reserves metadata action and status dimensions across async updates", () => {
    expect(styles).toMatch(/\.metadata-button-label\s*\{[^}]*display: grid;/s);
    expect(styles).toMatch(/\.metadata-button-label > span\s*\{[^}]*grid-area: 1 \/ 1;/s);
    expect(styles).toMatch(/\.metadata-button-label > span\[aria-hidden\]\s*\{[^}]*visibility: hidden;/s);
    expect(styles).toMatch(/\.connection-check-required \.connection-check-result\s*\{[^}]*height: 2\.7em;[^}]*overflow: auto;/s);
    expect(styles).toMatch(/\.transform-schema-loader\s*\{[^}]*height: 38px;/s);
    expect(styles).toMatch(/\.transform-schema-loader > span\s*\{[^}]*white-space: nowrap;/s);
    expect(styles).toMatch(/\.transform-load-schemas\s*\{[^}]*width: 126px;[^}]*height: 30px;/s);
  });
  it("shares high-contrast secondary action colors without recoloring disabled controls or resizing buttons", () => {
    const theme = ':root[data-design="airy-v0"][data-theme="light"]';
    const tokens = styles.split(`${theme} {`)[1]?.split("}")[0];
    for (const token of ["--secondary-action-surface: var(--control);", "--secondary-action-text: color-mix(in srgb, var(--blue-hover) 90%, var(--text-primary));",
      "--secondary-action-border: var(--blue);", "--secondary-action-hover: var(--surface-selected);"]) {
      expect(tokens).toContain(token);
    }
    const selector = `${theme} .secondary-button:where(:not(:disabled, .diagnostic-disabled))`;
    const idle = styles.split(`${selector} {`)[1]?.split("}")[0];
    const hover = styles.split(`${selector}:hover {`)[1]?.split("}")[0];
    expect(idle).toContain("color: var(--secondary-action-text);");
    expect(idle).toContain("background: var(--secondary-action-surface);");
    expect(idle).toContain("border-color: var(--secondary-action-border);");
    expect(hover).toContain("background: var(--secondary-action-hover);");
    expect(`${idle}${hover}`).not.toMatch(/(?:width|height|padding|margin|transform|border-width|font-size)\s*:/);
    expect(styles).not.toMatch(/data-theme="dark"[^{}]*\.secondary-button/);
  });
  it("keeps transform strip controls fixed while preview status and result change", () => {
    const output = styles.split(".transform-preview-output {")[1]?.split("}")[0];
    expect(output).toContain("height: 240px;");
    expect(output).toContain("overflow: auto;");
    const status = styles.split(".transform-preview-status {")[1]?.split("}")[0];
    expect(status).toContain("height: 2.8em;");
    expect(status).toContain("overflow: auto;");
    const strip = styles.split(".middleware-strip-heading {")[1]?.split("}")[0];
    expect(strip).toContain("min-width: 0;");
    expect(strip).toContain("minmax(0, 1fr)");
  });
  it("paints matched-list arrows as complete vector silhouettes without stitched borders", () => {
    const icon = styles.split(".table-matches-height-icon {")[1]?.split("}")[0] ?? "";
    const restore = styles.split(".table-matches-height-icon-restore {")[1]?.split("}")[0] ?? "";
    expect(icon).toContain("background: currentColor;");
    expect(icon).toContain("visibility: inherit;");
    for (const rule of [icon, restore]) {
      expect(rule).toMatch(/clip-path: path\(\s*"M[^"\n]+"\s*\);/);
      expect(rule).not.toMatch(/gradient\(|^\s*(?:border(?:-[a-z]+)*|transform)\s*:/m);
    }
    // One continuous outline for Show all, two detached arrowheads/stems for
    // Restore height. Browser crops verify the gap and rasterized silhouette.
    expect(icon.match(/M\d/g)).toHaveLength(1);
    expect(restore.match(/M\d/g)).toHaveLength(2);
    expect(styles).not.toMatch(/\.table-matches-height-icon(?:-restore)?::(?:before|after)/);
  });
  it("lets matched lists hand scrolling to the page when fully expanded or at either edge", () => {
    // Both per-rule and aggregate lists use this scroll container. Keeping
    // containment here traps wheel/touch scrolling even after Show all fits it.
    const list = styles.split(".table-rule-matches {")[1]?.split("}")[0];
    expect(list).toContain("overflow: auto;");
    expect(list).toContain("overscroll-behavior: auto;");
    expect(list).not.toMatch(/overscroll-behavior(?:-[xy])?:\s*(?:contain|none)/);
    expect(list).toContain("height: auto;");
    expect(list).toContain("max-height: 140px;");
    expect(list).toContain("min-height: 0;");
    expect(list).toContain("resize: none;");
  });
  it("reserves required connection feedback slots across idle, pending, success and failure", () => {
    for (const selector of [".connection-check-required .connection-check-result", ".connection-dependent-status"]) {
      const rule = styles.split(`${selector} {`)[1]?.split("}")[0];
      expect(rule).toContain("height: 2.7em;");
      expect(rule).toContain("overflow: auto;");
      expect(rule).toContain("overflow-wrap: anywhere;");
    }
    const fields = styles.split(".connection-dependent-fields {")[1]?.split("}")[0];
    expect(fields).toContain("min-width: 0;");
    expect(fields).toContain("grid-template-columns: minmax(0, 1fr);");
    expect(styles).toContain(".connection-check-required .connection-check {");
    expect(styles).toContain("grid-template-columns: max-content minmax(0, 1fr);");
    expect(styles).toContain('.segmented-control > button[aria-checked="true"]:not(:disabled)');
    const regex = styles.split('.connection-dependent-fields:disabled .table-pattern-input .regex-toggle[aria-pressed="true"]:disabled {')[1]?.split("}")[0];
    expect(regex).toContain("background: var(--disabled-surface);");
    // State colors must not change the footprint or hide the disabled form.
    expect(styles).not.toMatch(/\.connection-dependent[^{}]*\[aria-disabled[^{}]*\{[^}]*(?:display|height|padding|margin)\s*:/s);
  });
  it("stacks form labels in narrow containers without squeezing controls", () => {
    expect(styles).toContain("container: form-space / inline-size;");
    expect(styles).toMatch(/@container form-space \(max-width: 520px\)/);
    expect(styles).toMatch(/:root .form-row:not\(\.form-row-wide\)\s*\{[^}]*grid-template-columns: minmax\(0, 1fr\)/s);
    expect(styles).toContain("repeat(auto-fit, minmax(min(100%, 260px), 1fr))");
  });
  it("uses one cool neutral palette for the airy light editor and catalog", () => {
    const theme = styles.split(':root[data-design="airy-v0"][data-theme="light"] {')[1]?.split("}")[0];
    for (const token of ["--sidebar: #edf1f4;", "--panel2: #edf1f4;", "--line-strong: #cfd8de;", "--blue: #0d9488;", "--text-primary: #0b1220;"])
      expect(theme).toContain(token);
    expect(theme).not.toContain("#f8faf9");
    const tab = styles.split(':root[data-design="airy-v0"][data-theme="light"] .compatibility-tabs button[aria-selected="true"] {')[1]?.split("}")[0];
    expect(tab).toContain("background: var(--panel);");
    expect(tab).toContain("color: var(--text-primary);");
    expect(styles).toContain('.compatibility-tabs button[aria-selected="true"]::after');
  });
  it("does not spread a table validation error to all descendant controls", () => {
    expect(styles).not.toContain("\n.required-missing input,");
    expect(styles).not.toContain("\n.required-missing .select-trigger {");
    expect(styles).toContain(".required-missing .column-table td.required-incomplete .select-trigger");
  });
  it("reserves two lines for Arrow types without wrapping timestamp parameters", () => {
    const trigger = styles.split(':root .column-table td.arrow-type-cell .select-trigger {')[1]?.split("}")[0];
    expect(trigger).toContain("height: 48px;");
    expect(trigger).toContain("min-height: 48px;");
    const label = styles.split(':root .column-table td.arrow-type-cell .select-trigger > span:first-child {')[1]?.split("}")[0];
    expect(label).toContain("white-space: pre;");
    expect(label).toContain("word-break: normal;");
  });
  it("keeps action buttons 16px from the frame and centers their header", () => {
    expect(styles).toContain('.column-table .actions-column {\n  width: 80px;');
    expect(styles).toContain(':root .column-table th.actions-column {\n  padding-right: 16px;');
    expect(styles).toContain(':root .column-table th.actions-column {\n  padding-left: 16px;\n  text-align: center;');
  });
  it("has no CSS-painted tooltips competing with native titles", () => {
    expect(styles).not.toMatch(/content:\s*attr\((?:data-tooltip|title)\)/);
    expect(styles).not.toContain(".help-tooltip");
    expect(styles).not.toContain(".instant-tooltip-content");
  });
  it("does not reserve a blank scrollbar strip beside table headers", () => {
    const shell = styles.split(".table-shell {")[1]?.split("}")[0];
    expect(shell).toContain("scrollbar-gutter: auto;");
    expect(shell).toContain("overflow-x: auto;");
    expect(shell).not.toMatch(/(?:max-)?height\s*:/);
  });
  it("gives output columns a framed zebra surface without overriding cell states", () => {
    const shell = styles.split(':root[data-theme="light"] .table-shell:has(> .column-table) {')[1]?.split("}")[0];
    expect(shell).toContain("background: #ffffff;");
    expect(shell).toContain("border: 1px solid #cfd8de;");
    expect(shell).toContain("border-radius: var(--radius-panel);");
    expect(styles).toContain('.column-table tbody tr:nth-child(even) {\n  background: #f1f5f8;');
    // Backgrounds belong to rows, leaving selected/error/dragged cell colors on top.
    expect(styles).not.toMatch(/\.column-table tbody tr:nth-child\(even\) td/);
    const hover = styles.split('.column-table tbody tr:hover {')[1]?.split("}")[0];
    expect(hover?.trim()).toBe("background: #e5f2f0;");
  });
  it("shares the destination gaps and panel radius with the parser join", () => {
    expect(styles).toContain("grid-template-columns: minmax(450px, 1fr) var(--route-gap) minmax(450px, 1fr);");
    const bridge = styles.split(".source-details-bridge {")[1]?.split("}")[0];
    // The one-pixel overlap hides the parser top border without shrinking the gap.
    expect(bridge).toContain("height: calc(var(--route-gap) + 1px);");
    expect(bridge).toContain("margin-bottom: -1px;");
    expect(styles).toContain("border-bottom-left-radius: var(--radius-panel);");
    expect(styles).toContain("border-radius: 0 var(--radius-panel) var(--radius-panel);");
    expect(styles).toContain(".source-details-bridge {\n    display: none;");
  });
  it("shades the three delivery islands without changing control geometry or dark themes", () => {
    const selectors = [
      ':root[data-theme="light"] .identity-card',
      ':root[data-theme="light"] .route-composition > .endpoint-card',
      ':root[data-theme="light"] .route-composition > .source-details-bridge',
      ':root[data-theme="light"] .route-composition > .source-details-card',
    ].join(",\n");
    const rule = styles.split(`${selectors} {`)[1]?.split("}")[0];
    expect(rule?.trim()).toBe("background: #edf1f4;\n  border-color: #cfd8de;");
    for (const design of ["classic", "airy-v0"]) {
      const theme = styles.split(`:root[data-design="${design}"][data-theme="light"] {`)[1]?.split("}")[0];
      expect(theme).toContain("--control: #ffffff;");
      expect(theme).toContain("--canvas: #ffffff;");
    }
    expect(styles).toContain(':root[data-theme="light"] .route-composition:has(> .source-details-card) > .endpoint-card-source {\n  border-bottom-color: transparent;');
  });
  it("uses identical field spacing for ordinary and advanced settings in every endpoint", () => {
    expect(styles).toMatch(/\.foldout-content,\s*\.schema-object\s*\{\s*display:\s*grid;\s*gap:\s*10px;/s);
    expect(styles).toMatch(/:root\[data-design="airy-v0"\] \.foldout-content,\s*:root\[data-design="airy-v0"\] \.schema-object\s*\{\s*gap:\s*14px;/s);
  });
  it("uses one airy spacing token between editor tabs and islands, including stacked endpoints", () => {
    const airy = ':root[data-design="airy-v0"]';
    const rule = (selector: string) => styles.split(`${selector} {`)[1]?.split("}")[0];
    expect(rule(airy)).toContain("--editor-section-gap: 20px;");
    expect(rule(`${airy} .editor-tabs`)).toContain("margin: 0 0 var(--editor-section-gap);");
    expect(rule(`${airy} .editor-view`)).toContain("display: grid;");
    expect(rule(`${airy} .editor-view`)).toContain("grid-template-columns: minmax(0, 1fr);");
    expect(rule(`${airy} .editor-view`)).toContain("gap: var(--editor-section-gap);");
    expect(rule(`${airy} .identity-card`)).toContain("margin-bottom: 0;");
    expect(rule(`${airy} .editor-view > .route-feedback,\n${airy} .editor-view > .pipeline-section`)).toContain("margin: 0;");
    const stacked = styles.split("@media (max-width: 1300px) {")[1];
    expect(stacked).toContain(`${airy} .route-arrow {\n    height: var(--editor-section-gap);`);
    // Source and parser remain one joined island, with no gap inside their grid.
    expect(rule(".route-composition")).not.toMatch(/^[ \t]*(?:row-)?gap\s*:/m);
  });
  it("keeps parser support columns fixed and hover feedback dimension-neutral", () => {
    expect(styles).toMatch(/\.parser-support-table\s*\{[^}]*width: 100%;[^}]*table-layout: fixed;/s);
    expect(styles).toMatch(/\.parser-support-status-column\s*\{[^}]*width: 38px;/s);
    const hover = styles.match(/\.parser-support-table tbody tr:hover\s*\{([^}]*)\}/)?.[1];
    expect(hover).toContain("background:");
    expect(hover).not.toMatch(/\b(width|height|padding|margin|border|transform)\s*:/);
  });
  it("gives nested serializer fields full width instead of repeated label columns", () => {
    const row = styles.match(/\.serializer-inline-settings \.schema-object \.form-row:not\(\.form-row-wide\)\s*\{([^}]*)\}/)?.[1];
    expect(row).toContain("grid-template-columns: minmax(0, 1fr);");
    expect(row).toContain("align-items: start;");
    const control = styles.match(/\.serializer-inline-settings \.form-row > \.field-control\s*\{([^}]*)\}/)?.[1];
    expect(control).toContain("width: 100%;");
    expect(control).toContain("min-width: 0;");
    // The layout must not depend on hover/focus: typing cannot move controls.
    expect(styles).not.toMatch(/\.serializer-inline-settings[^{}]*:(?:hover|focus|focus-within)[^{}]*\{[^}]*(?:grid-template-columns|width|padding)\s*:/s);
  });
  it("collapses empty route feedback and lets errors use their natural height", () => {
    expect(styles).toMatch(/\.route-feedback:empty\s*\{[^}]*display: none;/s);
    const feedback = styles.match(/\.route-feedback\s*\{([^}]*)\}/)?.[1];
    const error = styles.match(/\.route-feedback > \.compatibility-error\s*\{([^}]*)\}/)?.[1];
    expect(feedback).toContain("margin-bottom: 14px;");
    expect(error).toContain("margin: 0;");
    expect(`${feedback}${error}`).not.toMatch(/\b(height|min-height|max-height|overflow)\s*:/);
  });
  it("lets both property membership lists use their full content height", () => {
    expect(styles).toMatch(/\.property-members\s*\{[^}]*grid-template-rows: auto max-content auto max-content;[^}]*align-content: start;[^}]*overflow: auto;/s);
    expect(styles).toMatch(/\.property-entity-names\s*\{[^}]*overflow: visible;/s);
    expect(styles).not.toContain(".property-members.expanded-members");
  });
  it("highlights the whole entity tile without changing its dimensions", () => {
    expect(styles).toMatch(/\.property-entity-names li:not\(\.entity-empty\):hover,\s*\.entity-catalog-list li:not\(\.entity-empty\):hover/);
    const rule = styles.match(/\.entity-catalog-list li:not\(\.entity-empty\):hover\s*\{([^}]*)\}/)?.[1];
    expect(rule).toContain("background: var(--surface-selected)");
    expect(rule).toContain("box-shadow: inset");
    expect(rule).not.toMatch(/\b(padding|margin|width|height|border|transform|scale)\s*:/);
  });
  it("distinguishes green batch badges from cyan stream badges in both themes", () => {
    expect(styles).toMatch(/\.compatibility-badge.batch\s*\{[^}]*color: #bbf7d0;[^}]*background: #14532d;/s);
    expect(styles).toMatch(/\.compatibility-badge.stream\s*\{[^}]*color: #67e8f9;[^}]*background: #164e63;/s);
    expect(styles).toMatch(/:root\[data-theme="light"\] \.compatibility-badge.batch\s*\{[^}]*color: #166534;[^}]*background: #dcfce7;/s);
    expect(styles).toMatch(/:root\[data-theme="light"\] \.compatibility-badge.stream\s*\{[^}]*color: #0e7490;[^}]*background: #cffafe;/s);
  });
  it("keeps matrix header transforms out of pressed-state rules", () => {
    const pressedRule = styles.match(/\.compatibility-table th > button:active:not\(:disabled\)\s*\{([^}]*)\}/);
    expect(pressedRule).not.toBeNull();
    expect(pressedRule?.[1]).not.toMatch(/\b(transform|scale)\s*:/);
  });
  it("distinguishes strong blue matrix selection from green search matches", () => {
    expect(styles).toMatch(/\.compatibility-table tr.selected-row > td\s*\{[^}]*#3b82f6 32%/s);
    expect(styles).toMatch(/\.compatibility-table tr.active-row > td\s*\{[^}]*#3b82f6 10%/s);
    expect(styles).toMatch(/\.compatibility-table td.active-intersection\s*\{[^}]*#3b82f6 18%/s);
    expect(styles).toMatch(/\.compatibility-table tr.search-match-row > td.selected-column\s*\{[^}]*#a855f7 24%/s);
    expect(styles).toMatch(/\.compatibility-table tr.search-match-row > td\s*\{[^}]*var\(--success-surface\)/s);
  });
  it("fits the matrix without scrollbars and makes the entire header cell pressable", () => {
    expect(styles).toMatch(/\.compatibility-matrix-viewport\s*\{[^}]*overflow: hidden;/s);
    expect(styles).toMatch(/\.compatibility-matrix-content\s*\{[^}]*position: absolute;[^}]*transform-origin: top left;/s);
    expect(styles).toMatch(/\.compatibility-table thead th\s*\{[^}]*white-space: normal;[^}]*overflow-wrap: anywhere;/s);
    expect(styles).toMatch(/\.compatibility-table th > button\s*\{[^}]*width: 100%;[^}]*height: 100%;[^}]*border-radius: 0;/s);
    expect(styles).toContain("th:has(> button:active)");
    expect(styles).not.toContain(".compatibility-legend");
  });

  it("reserves tab icon space and overlays disabled locks without shifting labels", () => {
    expect(styles).toMatch(/:root \.editor-view-tabs button\s*\{[^}]*position: relative;[^}]*padding-inline: 25px;/s);
    expect(styles).toMatch(/\.disabled-lock-icon\s*\{[^}]*position: absolute;/s);
    expect(styles).not.toContain("--disabled-lock:");
    expect(styles).toContain("--disabled-text: #89939d");
    expect(styles).toContain("--text-primary: #0b1220");
  });
  it("does not paint a duplicate action tooltip inside the page", () => {
    expect(styles).not.toContain(".instant-tooltip-content");
  });
  it("keeps detached parser layout without a detached serializer row or bridge", () => {
    expect(styles).toContain('"sourcebridge . ."');
    expect(styles).toContain('"parser parser parser"');
    expect(styles).not.toContain(".serializer-details-card");
    expect(styles).not.toContain(".sink-serializer-bridge");
    expect(styles).toMatch(/\.serializer-inline-settings > \.field-control,/);
  });

  it("keeps notice copy controls fixed-width across clipboard states", () => {
    expect(styles).toMatch(/\.notice button\.notice-copy\s*\{[^}]*flex:\s*0 0 28px;[^}]*width:\s*28px;/s);
  });

  it("keeps dynamic selects compact and option errors outside document flow", () => {
    expect(styles).toMatch(
      /\.dynamic-select\s*\{[^}]*position:\s*relative;/s,
    );
    expect(styles).toMatch(
      /\.dynamic-select-status\s*\{[^}]*position:\s*absolute;[^}]*top:\s*100%;/s,
    );
    expect(styles).toMatch(
      /\.dynamic-select-status:empty\s*\{[^}]*display:\s*none;/s,
    );
  });

  it("does not paint a browser focus highlight around source detail settings", () => {
    expect(styles).toMatch(
      /\.source-details-card\s*\{[^}]*outline:\s*none;/s,
    );
  });

  it("never truncates selected dropdown labels", () => {
    const triggerLabel = styles.match(
      /\.select-trigger > span:first-child\s*\{([^}]*)\}/,
    );
    expect(triggerLabel?.[1]).toContain("white-space: normal");
    expect(triggerLabel?.[1]).toContain("overflow-wrap: anywhere");
    expect(triggerLabel?.[1]).not.toContain("text-overflow: ellipsis");
  });

  it("lets the workspace shrink instead of creating page-level horizontal overflow", () => {
    expect(styles).toMatch(
      /\.workspace\s*\{[^}]*width:\s*min\(1680px, 100%\);[^}]*min-width:\s*0;/s,
    );
    expect(styles).toMatch(
      /\.table-shell\s*\{[^}]*min-width:\s*0;[^}]*overflow-x:\s*auto;/s,
    );
  });
});
