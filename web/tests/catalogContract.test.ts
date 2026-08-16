import { describe, expect, it } from "vitest";

import { decodeApi } from "../src/api/contractDecoder";
import { acceptsDraftSeed, compileSchema, draftSeedError } from "../src/schema/compiler";

const catalogJson = (
  globalThis as typeof globalThis & {
    process?: { env?: Record<string, string | undefined> };
  }
).process?.env?.TRANSFERIA_CATALOG_CONTRACT;

describe("Rust catalog contract", () => {
  const contractTest = catalogJson === undefined ? it.skip : it;

  contractTest("compiles every schema emitted by the Rust catalog", () => {
    const catalog = decodeApi(
      "catalog_response",
      JSON.parse(catalogJson!),
      "catalog",
    );

    const common = compileSchema(catalog.common_schema);
    if (common.kind !== "object") throw new Error("common schema must be an object");
    const commonInitial = Object.fromEntries(
      Object.entries(catalog.initial).filter(([name]) => common.properties[name] !== undefined),
    );
    expect(acceptsDraftSeed(common, commonInitial)).toBe(true);
    let endpointCount = 0;
    for (const provider of catalog.providers) {
      for (const endpoint of [provider.source, provider.sink]) {
        if (endpoint === undefined) continue;
        endpointCount += 1;
        const compiled = compileSchema(endpoint.schema);
        expect(
          acceptsDraftSeed(compiled, endpoint.initial),
          `${provider.key} initial value must be a valid partial schema value: ${draftSeedError(compiled, endpoint.initial)}`,
        ).toBe(true);
      }
    }
    expect(endpointCount).toBeGreaterThan(0);
  });
});
