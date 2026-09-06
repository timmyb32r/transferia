import assert from "node:assert/strict";
import { mkdir } from "node:fs/promises";
import { pathToFileURL } from "node:url";

const modulePath = process.env.TRANSFERIA_PLAYWRIGHT_MODULE;
const { chromium } = await import(modulePath ? pathToFileURL(modulePath).href : "playwright");
const browser = await chromium.launch({
  headless: true,
  ...(process.env.TRANSFERIA_BROWSER_EXECUTABLE ? { executablePath: process.env.TRANSFERIA_BROWSER_EXECUTABLE } : {}),
});
const page = await browser.newPage({ viewport: { width: 1440, height: 1100 }, deviceScaleFactor: 1 });
const errors = [];
page.on("pageerror", error => errors.push(error.message));
const output = process.env.TRANSFERIA_UI_ARTIFACTS ?? "/tmp/transferia-transform-ui";
await mkdir(output, { recursive: true });
const rect = locator => locator.boundingBox();
const unchanged = (before, after, label) => {
  for (const key of ["x", "y", "width", "height"]) assert(Math.abs(before[key] - after[key]) < 0.6, `${label} moved on ${key}: ${before[key]} -> ${after[key]}`);
};
try {
  await page.goto(process.env.TRANSFERIA_UI_URL ?? "http://127.0.0.1:5184/tests/fixtures/middleware-smoke.html");
  await page.getByRole("button", { name: "Expand transform 2", exact: true }).waitFor();
  await page.evaluate(() => document.fonts.ready);
  assert.equal(await page.getByRole("article").count(), 3);
  for (const strip of await page.getByRole("article").all()) assert((await rect(strip)).height <= 80);
  await page.screenshot({ path: `${output}/collapsed.png`, fullPage: true });
  const header = page.locator(".middleware-strip-heading").nth(1);
  const headerBefore = await rect(header);
  await page.getByRole("button", { name: "Expand transform 2", exact: true }).click();
  unchanged(headerBefore, await rect(header), "expanded strip header");
  assert.equal(await page.getByRole("button", { name: "Run preview", exact: true }).count(), 0);
  assert((await rect(page.getByRole("textbox", { name: "Column", exact: true }))).height >= 32, "filter fields must use the shared styled text input");
  const matched = page.getByRole("button", { name: "Matched tables for transform 2", exact: true });
  const available = page.getByRole("button", { name: "Available tables for transform 2", exact: true });
  assert.equal(await available.textContent(), "Available tables (3)");
  assert((await rect(available)).height <= 24, "available tables must be a thin control");
  await page.waitForFunction(() => document.querySelector('.middleware-table-scope .table-match-count')?.textContent === '(3)' &&
    document.querySelector('.middleware-table-scope .matched-toggle .table-match-count')?.textContent === '3');
  assert.equal(await matched.evaluate(button => getComputedStyle(button).borderTopColor), "rgba(0, 0, 0, 0)");
  const beforeDialog = await rect(header);
  await available.click();
  const dialog = page.getByRole("dialog", { name: "Available tables", exact: true });
  await dialog.waitFor();
  unchanged(beforeDialog, await rect(header), "opening catalog dialog");
  const list = page.getByRole("region", { name: "Available table names", exact: true });
  const listBefore = await rect(list);
  await page.getByRole("textbox", { name: "Search tables", exact: true }).fill("analytics.reports_d*");
  await page.getByRole("button", { name: "Copy analytics.reports_daily", exact: true }).waitFor();
  await page.waitForFunction(() => document.querySelectorAll('.available-table-row').length === 1);
  unchanged(listBefore, await rect(list), "catalog search result");
  await page.screenshot({ path: `${output}/available-tables.png`, fullPage: true });
  await page.getByRole("textbox", { name: "Search tables", exact: true }).press("Escape");
  assert.equal(await dialog.count(), 0);
  assert.equal(await available.evaluate(button => document.activeElement === button), true);
  unchanged(beforeDialog, await rect(header), "closing catalog dialog");
  await matched.click();
  await page.getByRole("region", { name: "Matched tables for transform 2", exact: true }).waitFor();
  assert.equal(await page.getByRole("button", { name: "Show all", exact: true }).count(), 1);
  await page.screenshot({ path: `${output}/matched-tables.png`, fullPage: true });
  await matched.click();
  await page.screenshot({ path: `${output}/settings.png`, fullPage: true });
  await page.getByRole("button", { name: "Preview transform 2", exact: true }).click();
  const load = page.getByRole("button", { name: "Load tables", exact: true });
  const loadBefore = await rect(load);
  await load.click();
  assert.equal(await load.getAttribute("aria-busy"), "true");
  unchanged(loadBefore, await rect(load), "pending catalog button");
  await page.waitForFunction(() => document.querySelector(".transform-preview-controls > button")?.getAttribute("aria-busy") === "false");
  unchanged(loadBefore, await rect(load), "completed catalog button");
  await page.getByRole("button", { name: "Sample table", exact: true }).click();
  await page.getByRole("option", { name: "analytics.reports_daily", exact: true }).click();
  const run = page.getByRole("button", { name: "Run preview", exact: true });
  const following = page.getByRole("article").nth(2);
  const nextBefore = await rect(following), runBefore = await rect(run);
  await run.click();
  assert.equal(await run.getAttribute("aria-busy"), "true");
  unchanged(runBefore, await rect(run), "pending preview button");
  unchanged(nextBefore, await rect(following), "next strip during preview");
  await page.getByText("2 rows", { exact: true }).waitFor();
  unchanged(runBefore, await rect(run), "completed preview button");
  unchanged(nextBefore, await rect(following), "next strip after preview");
  await page.screenshot({ path: `${output}/preview.png`, fullPage: true });
  await page.getByRole("tab", { name: "Before step", exact: true }).click();
  await page.getByText("3 rows", { exact: true }).waitFor();
  unchanged(nextBefore, await rect(following), "next strip after tab switch");
  for (const width of [760, 390]) {
    await page.setViewportSize({ width, height: 1000 });
    await page.screenshot({ path: `${output}/width-${width}.png`, fullPage: true });
    const overflow = await page.evaluate(() => document.documentElement.scrollWidth - window.innerWidth);
    assert(overflow <= 1, `page overflows by ${overflow}px at ${width}px`);
    for (const strip of await page.getByRole("article").all()) {
      const bounds = await rect(strip);
      for (const button of await strip.locator(".middleware-strip-heading button, .transform-preview-controls > button").all()) {
        const box = await rect(button);
        assert(box.x >= bounds.x && box.x + box.width <= bounds.x + bounds.width + 1, "strip button is outside its frame");
      }
    }
  }
  assert.deepEqual(errors, [], "browser page errors");
  console.log(JSON.stringify({ passed: true, screenshots: output, widths: [1440, 760, 390], checks: ["compact strips", "optional preview", "stable pending and result geometry", "no narrow overflow", "no browser errors"] }));
} catch (error) {
  console.error(JSON.stringify({ browserErrors: errors, body: (await page.locator("body").innerText()).slice(0, 2000) }));
  await page.screenshot({ path: `${output}/failure.png`, fullPage: true });
  throw error;
} finally {
  await browser.close();
}
