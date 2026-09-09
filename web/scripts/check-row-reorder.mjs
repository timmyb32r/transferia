import assert from "node:assert/strict";
import { fileURLToPath, pathToFileURL } from "node:url";
import { createServer } from "vite";

const modulePath = process.env.TRANSFERIA_PLAYWRIGHT_MODULE;
const { chromium } = await import(modulePath ? pathToFileURL(modulePath).href : "playwright");
const server = await createServer({ root: fileURLToPath(new URL("../", import.meta.url)), configFile: false,
  server: { host: "127.0.0.1" } });
let browser;
try {
  await server.listen(0);
  browser = await chromium.launch({ headless: true,
    ...(process.env.TRANSFERIA_BROWSER_EXECUTABLE ? { executablePath: process.env.TRANSFERIA_BROWSER_EXECUTABLE } : {}) });
  const base = `http://127.0.0.1:${server.httpServer.address().port}/tests/fixtures/`;
  for (const kind of ["transforms", "json_parser", "tskv"]) {
    const page = await browser.newPage({ viewport: { width: 1440, height: 1100 } });
    const errors = [];
    page.on("pageerror", error => errors.push(error.message));
    await page.goto(base + (kind === "transforms" ? "middleware-smoke.html?last-transform=rename"
      : `parser-layout-smoke.html?connector=kafka&parser=${kind}`));
    const rowSelector = kind === "transforms" ? ".middleware-list > .middleware-strip" : ".column-table tbody > .config-table-row";
    const handleSelector = kind === "transforms" ? ".middleware-drag" : ".drag-handle";
    if (kind !== "transforms") {
      const add = page.getByRole("button", { name: "Add column", exact: true });
      await add.waitFor();
      while (await page.locator(rowSelector).count() < 3) await add.click();
    }
    const rows = page.locator(rowSelector);
    await rows.first().waitFor();
    // DOM identity is preserved by both editors' keyed rows.
    await rows.first().evaluate(row => row.dataset.reorderProbe = "original");
    const handle = rows.first().locator(handleSelector);
    await handle.scrollIntoViewIfNeeded();
    const before = await rows.first().boundingBox();
    const nextBefore = await rows.nth(1).boundingBox();
    const grip = await handle.boundingBox();
    const x = grip.x + grip.width / 2, y = grip.y + grip.height / 2;
    await page.mouse.move(x, y); await page.mouse.down();
    await page.mouse.move(x + 1, y + 1);
    const ghost = await page.locator(".row-reorder-overlay").boundingBox();
    assert(Math.abs(ghost.x - before.x - 1) < 0.7 && Math.abs(ghost.y - before.y - 1) < 0.7, `${kind}: follows first pixel`);
    assert.deepEqual(await rows.nth(1).boundingBox(), nextBefore, `${kind}: preserves following controls`);
    await page.keyboard.press("Escape"); await page.mouse.up();
    assert.equal(await rows.first().getAttribute("data-reorder-probe"), "original");
    assert.equal(await page.locator(".row-reorder-overlay").count(), 0);
    await page.mouse.move(x, y); await page.mouse.down();
    const last = await rows.last().boundingBox();
    await page.mouse.move(x, last.y + last.height - 1, { steps: 10 });
    await page.waitForFunction(selector => {
      const row = document.querySelectorAll(selector)[1];
      return parseFloat(getComputedStyle(row).translate.split(" ").at(-1)) < -1;
    }, rowSelector);
    assert.equal(await page.locator(".row-reorder-marker").count(), 0, `${kind}: no insertion line`);
    assert.equal(await rows.first().getAttribute("data-reorder-probe"), "original", `${kind}: data order waits for release`);
    await page.mouse.up();
    assert.equal(await rows.last().getAttribute("data-reorder-probe"), "original", `${kind}: commits order`);
    assert.equal(await page.locator(".row-reorder-marker").count(), 0);
    assert.deepEqual(errors, []);
    await page.close();
  }
  console.log("PASS: pointer row reordering in transforms, JSON and TSKV");
} finally { await browser?.close(); await server.close(); }
