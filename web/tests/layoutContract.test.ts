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
    expect(styles).toMatch(/\.property-members.expanded-members\s*\{[^}]*grid-template-rows: auto max-content auto max-content;/s);
    expect(styles).toMatch(/\.property-members.expanded-members \.property-entity-names\s*\{[^}]*overflow: visible;[^}]*scrollbar-gutter: auto;/s);
  });
  it("highlights the whole entity tile without changing its dimensions", () => {
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
    expect(styles).toContain("--disabled-text: #8a9099");
    expect(styles).toContain("--text-primary: #0b1220");
  });
  it("anchors action tooltips inside the right edge of the page", () => {
    expect(styles).toMatch(
      /\.action-disabled-tooltip > \.instant-tooltip-content\.bottom\s*\{[^}]*left:\s*auto;[^}]*right:\s*0;[^}]*transform:\s*none;/s,
    );
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
