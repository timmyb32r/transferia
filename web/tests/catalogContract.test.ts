import { describe, expect, it } from "vitest";

import { compileSchema } from "../src/schema/compiler";
import type { UiCatalog } from "../src/types";

const catalogJson = (
  globalThis as typeof globalThis & {
    process?: { env?: Record<string, string | undefined> };
  }
).process?.env?.TRANSFERIA_CATALOG_CONTRACT;

describe("Rust catalog contract", () => {
  const contractTest = catalogJson === undefined ? it.skip : it;

  contractTest("compiles every schema emitted by the Rust catalog", () => {
    const catalog = JSON.parse(catalogJson!) as UiCatalog;

    expect(() => compileSchema(catalog.common_schema)).not.toThrow();
    let endpointCount = 0;
    for (const provider of catalog.providers) {
      for (const endpoint of [provider.source, provider.sink]) {
        if (endpoint === undefined) continue;
        endpointCount += 1;
        expect(
          () => compileSchema(endpoint.schema),
          `${provider.key} schema must satisfy the UI compiler contract`,
        ).not.toThrow();
      }
    }
    expect(endpointCount).toBeGreaterThan(0);
  });
});
