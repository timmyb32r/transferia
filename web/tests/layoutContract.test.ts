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
