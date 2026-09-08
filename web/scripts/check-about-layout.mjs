import assert from "node:assert/strict";
import { fileURLToPath, pathToFileURL } from "node:url";
import { createServer } from "vite";

const modulePath = process.env.TRANSFERIA_PLAYWRIGHT_MODULE;
const { chromium } = await import(modulePath ? pathToFileURL(modulePath).href : "playwright");
const server = await createServer({ root: fileURLToPath(new URL("../", import.meta.url)), configFile: false,
  server: { host: "127.0.0.1" } });
let browser;
const stable = (before, after, message) => {
  assert(before && after, `${message}: missing target`);
  for (const key of ["x", "y", "width", "height"]) assert(Math.abs(before[key] - after[key]) < 0.7, `${message}: ${key} moved`);
};
try {
  await server.listen();
  const address = server.httpServer.address();
  browser = await chromium.launch({ headless: true,
    ...(process.env.TRANSFERIA_BROWSER_EXECUTABLE ? { executablePath: process.env.TRANSFERIA_BROWSER_EXECUTABLE } : {}) });
  for (const width of [1440, 800]) {
    const page = await browser.newPage({ viewport: { width, height: 900 } });
    const errors = [];
    page.on("pageerror", e => errors.push(e.message));
    await page.goto(`http://127.0.0.1:${address.port}/tests/fixtures/about-layout-smoke.html`);
    const link = page.getByRole("button", { name: "About", exact: true });
    const linkBefore = await link.boundingBox();
    await link.click();
    await page.getByRole("dialog", { name: "About" }).waitFor();
    await page.evaluate(() => document.fonts.ready);
    await page.getByRole("tab", { name: "Source types", exact: true }).click();
    const search = page.getByRole("searchbox", { name: "Find type" });
    const close = page.getByRole("button", { name: "Close About" });
    const before = await search.boundingBox();
    const closeBefore = await close.boundingBox();
    for (const tab of ["Source types", "Destination types"]) {
      await page.getByRole("tab", { name: tab, exact: true }).click();
      for (const connector of ["PostgreSQL", "ClickHouse", "OpenSearch"]) {
        await page.getByRole("button", { name: connector, exact: true }).click();
        for (const query of ["", "Int32", "no-such-type"]) {
          await search.fill(query);
          stable(before, await search.boundingBox(), `${width}/${tab}/${connector}: search`);
          stable(closeBefore, await close.boundingBox(), `${width}/${tab}/${connector}: close`);
          const browserBox = await page.locator(".type-mapping-browser").boundingBox();
          const tableBox = await page.locator(".type-mapping-table-scroll").boundingBox();
          assert(tableBox.height > browserBox.height * 0.7, "mapping table must use most of the available content height");
        }
      }
      for (const connector of ["Kafka", "Logbroker", "S3", tab === "Source types" ? "Data generator (for benchmarks)" : "Discard (for benchmarks)"]) {
        await page.getByRole("button", { name: connector, exact: true }).click();
        assert.equal(await page.locator(".type-mapping-table").count(), 0, `${connector}: no meaningless table`);
        assert.equal(await search.count(), 0, `${connector}: no type search`);
        assert(await page.locator(".type-mapping-explanation").isVisible());
        stable(closeBefore, await close.boundingBox(), `${connector}: close remains stationary`);
      }
    }
    await close.click();
    stable(linkBefore, await link.boundingBox(), "closing About preserves launcher");
    assert(await link.evaluate(e => e === document.activeElement), "focus must return to About");
    assert.deepEqual(errors, []);
    await page.close();
  }
  console.log("PASS: About navigation, filter and close geometry.");
} finally {
  await browser?.close();
  await server.close();
}
