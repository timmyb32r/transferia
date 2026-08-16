import { configDefaults, defineConfig } from "vitest/config";

const catalogContract = "tests/catalogContract.test.ts";
const hasRustCatalog = process.env.TRANSFERIA_CATALOG_CONTRACT !== undefined;

export default defineConfig({
  test: {
    exclude: hasRustCatalog
      ? configDefaults.exclude
      : [...configDefaults.exclude, catalogContract],
  },
});
