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
  it("reserves stable feedback space above endpoint settings in every validation state", () => {
    expect(styles).toMatch(/\.route-feedback\s*\{[^}]*height: 80px;/s);
    expect(styles).toMatch(/\.route-feedback > \.compatibility-error\s*\{[^}]*box-sizing: border-box;[^}]*height: 100%;[^}]*margin: 0;[^}]*overflow: auto;/s);
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
  it("keeps serializer settings in the destination column on wide screens", () => {
    expect(styles).toContain('"parser parser parser"\n    ". . serializer"');
    expect(styles).toMatch(
      /\.serializer-details-card\s*\{[^}]*grid-area:\s*serializer;[^}]*grid-column:\s*3;/s,
    );
    expect(styles).toMatch(
      /@media \(max-width: 1300px\)[\s\S]*?\.serializer-details-card\s*\{[^}]*grid-column:\s*1;/,
    );
  });

  it("uses the same panel corner radius for source and destination cards", () => {
    const connectedRadius =
      "border-radius: var(--radius-panel) var(--radius-panel) 0 0;";
    const sourceRule = styles.match(
      /:has\(> \.parser-details-card\) > \.endpoint-card-source\s*\{([^}]*)\}/,
    );
    const sinkRule = styles.match(
      /:has\(> \.serializer-details-card\) > \.endpoint-card-sink\s*\{([^}]*)\}/,
    );
    expect(sourceRule?.[1]).toContain(connectedRadius);
    expect(sinkRule?.[1]).toContain(connectedRadius);
  });

  it("reserves a fixed-height region for dynamic option errors", () => {
    expect(styles).toMatch(
      /\.dynamic-select-status\s*\{[^}]*height:\s*38px;[^}]*overflow-y:\s*auto;/s,
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
