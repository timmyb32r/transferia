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
});
