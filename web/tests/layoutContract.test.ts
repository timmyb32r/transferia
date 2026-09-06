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
    expect(list).toContain("height: 140px;");
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
    const bridge = styles.split(".source-parser-bridge {")[1]?.split("}")[0];
    // The one-pixel overlap hides the parser top border without shrinking the gap.
    expect(bridge).toContain("height: calc(var(--route-gap) + 1px);");
    expect(bridge).toContain("margin-bottom: -1px;");
    expect(styles).toContain("border-bottom-left-radius: var(--radius-panel);");
    expect(styles).toContain("border-radius: 0 var(--radius-panel) var(--radius-panel);");
    expect(styles).toContain(".source-parser-bridge {\n    display: none;");
  });
  it("shades the three delivery islands without changing control geometry or dark themes", () => {
    const selectors = [
      ':root[data-theme="light"] .identity-card',
      ':root[data-theme="light"] .route-composition > .endpoint-card',
      ':root[data-theme="light"] .route-composition > .source-parser-bridge',
      ':root[data-theme="light"] .route-composition > .parser-details-card',
    ].join(",\n");
    const rule = styles.split(`${selectors} {`)[1]?.split("}")[0];
    expect(rule?.trim()).toBe("background: #edf1f4;\n  border-color: #cfd8de;");
    for (const design of ["classic", "airy-v0"]) {
      const theme = styles.split(`:root[data-design="${design}"][data-theme="light"] {`)[1]?.split("}")[0];
      expect(theme).toContain("--control: #ffffff;");
      expect(theme).toContain("--canvas: #ffffff;");
    }
    expect(styles).toContain(':root[data-theme="light"] .route-composition:has(> .parser-details-card) > .endpoint-card-source {\n  border-bottom-color: transparent;');
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
    expect(styles).toMatch(/\.notice button\.notice-copy\s*\{[^}]*flex:\s*0 0 80px;[^}]*width:\s*80px;/s);
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

  it("does not paint a browser focus highlight around parser settings", () => {
    expect(styles).toMatch(
      /\.parser-details-card\s*\{[^}]*outline:\s*none;/s,
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
