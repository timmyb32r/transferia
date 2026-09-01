import { describe, expect, it } from "vitest";

import catalogFixture from "../../crates/transferia-server-contracts/contracts/connector-catalog.fixture.json";
import { decodeApi } from "../src/api/contractDecoder";
import { validateCatalogSchemas } from "../src/delivery/editorConfig";
import { productionWidgetRegistry } from "../src/features/formWidgetRegistry";
import {
  acceptsDraftSeed,
  compileSchema,
  draftSeedError,
} from "../src/schema/compiler";

describe("Rust catalog contract", () => {
  it("compiles every schema emitted by the Rust catalog", () => {
    const runtime = (
      globalThis as typeof globalThis & {
        process?: {
          env?: Record<string, string | undefined>;
          getBuiltinModule?: (name: "fs") => {
            readFileSync: (path: string, encoding: "utf8") => string;
          };
        };
      }
    ).process;
    const environment = runtime?.env;
    const injectedCatalog = environment?.TRANSFERIA_CATALOG_CONTRACT;
    const injectedCatalogFile = environment?.TRANSFERIA_CATALOG_CONTRACT_FILE;
    const catalog = decodeApi(
      "catalog_response",
      injectedCatalogFile !== undefined
        ? JSON.parse(
            runtime?.getBuiltinModule?.("fs").readFileSync(
              injectedCatalogFile,
              "utf8",
            ) ?? "null",
          )
        : injectedCatalog === undefined
          ? catalogFixture
          : JSON.parse(injectedCatalog),
      "catalog",
    );

    expect(() =>
      validateCatalogSchemas(catalog, productionWidgetRegistry),
    ).not.toThrow();

    const common = compileSchema(
      catalog.common_schema,
      productionWidgetRegistry,
    );
    if (common.kind !== "object")
      throw new Error("common schema must be an object");
    const commonInitial = Object.fromEntries(
      Object.entries(catalog.initial).filter(
        ([name]) => common.properties[name] !== undefined,
      ),
    );
    expect(acceptsDraftSeed(common, commonInitial)).toBe(true);
    let endpointCount = 0;
    for (const connector of catalog.connectors) {
      for (const endpoint of [connector.source, connector.sink]) {
        if (endpoint === undefined) continue;
        endpointCount += 1;
        const compiled = compileSchema(
          endpoint.schema,
          productionWidgetRegistry,
        );
        expect(
          acceptsDraftSeed(compiled, endpoint.initial),
          `${connector.key} initial value must be a valid partial schema value: ${draftSeedError(compiled, endpoint.initial)}`,
        ).toBe(true);
      }
    }
    expect(endpointCount).toBeGreaterThan(0);
  });
});
