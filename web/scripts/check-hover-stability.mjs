import assert from "node:assert/strict";
import { fileURLToPath, pathToFileURL } from "node:url";
import { createServer } from "vite";

const modulePath = process.env.TRANSFERIA_PLAYWRIGHT_MODULE;
const { chromium } = await import(modulePath ? pathToFileURL(modulePath).href : "playwright");
const server = await createServer({ root: fileURLToPath(new URL("../", import.meta.url)), configFile: false,
  server: { host: "127.0.0.1" } });
const stable = (before, after) => {
  assert(before && after, "hit target must exist");
  for (const key of ["x", "y", "width", "height"]) assert(Math.abs(before[key] - after[key]) < 0.7, `${key} moved`);
};
async function sweep(page, locator, expectedCursor) {
  await locator.scrollIntoViewIfNeeded();
  const box = await locator.boundingBox();
  const points = await locator.evaluate(element => {
    const rect = element.getBoundingClientRect();
    const points = [1, .25 * rect.width, .5 * rect.width, rect.width - 1]
      .map(x => [rect.x + x, rect.y + rect.height / 2]);
    for (const child of element.querySelectorAll("input, span, strong, svg")) {
      if (child.closest(".help")) continue;
      const r = child.getBoundingClientRect();
      if (r.width && r.height) points.push([r.x + r.width / 2, r.y + r.height / 2]);
    }
    return points;
  });
  for (const [x, y] of points) {
    await page.mouse.move(x, y);
    const hit = await locator.evaluate((element, [x, y]) => {
      const target = document.elementFromPoint(x, y);
      return { owns: element.contains(target), help: !!target?.closest(".help"), cursor: target && getComputedStyle(target).cursor };
    }, [x, y]);
    assert(hit.owns, "hover must not replace or cover the hit target");
    assert.equal(hit.cursor, hit.help ? "help" : expectedCursor, `inconsistent cursor within ${await locator.textContent()}`);
    stable(box, await locator.boundingBox());
  }
}
let browser;
try {
  await server.listen();
  browser = await chromium.launch({ headless: true,
    ...(process.env.TRANSFERIA_BROWSER_EXECUTABLE ? { executablePath: process.env.TRANSFERIA_BROWSER_EXECUTABLE } : {}) });
  const base = `http://127.0.0.1:${server.httpServer.address().port}/tests/fixtures/`;
  for (const design of ["airy-v0", "classic"]) for (const theme of ["light", "dark"]) {
    const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
    const errors = [];
    page.on("pageerror", error => errors.push(error.message));
    await page.goto(base + "hover-stability-smoke.html");
    await page.getByRole("button", { name: "Browse tables" }).waitFor();
    await page.evaluate(async ([design, theme]) => {
      Object.assign(document.documentElement.dataset, { design, theme });
      await document.fonts.ready;
    }, [design, theme]);
    const booleans = await page.locator('input:is([type="checkbox"], [type="radio"])').all();
    assert.equal(booleans.length, 12, "all boolean-label fixtures must be mounted");
    for (const input of booleans) {
      const expected = await input.isDisabled() ? "not-allowed" : "pointer";
      await sweep(page, input, expected);
      const id = await input.getAttribute("id");
      for (const label of await page.locator(`label:has(> #${id}), label[for="${id}"]`).all()) {
        await sweep(page, label, expected);
        for (const help of await label.locator(".help").all()) await sweep(page, help, "help");
      }
    }
    assert.notEqual(await page.locator('label[for="compound"]').evaluate(el => getComputedStyle(el).cursor), "pointer",
      "a nested checkbox must not change the outer text-field label's cursor");

    await page.getByRole("button", { name: "Browse tables" }).click();
    const details = page.locator(".available-table-details");
    const copy = page.getByRole("button", { name: "Copy public.events" });
    const use = page.getByRole("button", { name: "Use public.events in Include" });
    const search = page.getByRole("textbox", { name: "Search tables" });
    await details.hover();
    const original = await details.elementHandle();
    const box = await details.boundingBox();
    const copyBox = await copy.boundingBox(), useBox = await use.boundingBox();
    const copyTarget = await copy.elementHandle(), useTarget = await use.elementHandle();
    const cursor = await details.evaluate(el => getComputedStyle(el).cursor);
    await page.evaluate(() => window.dispatchEvent(new CustomEvent("hover-fixture-status", { detail: "failed" })));
    await page.getByRole("radio", { name: "Failed (1)" }).waitFor(); // Confirm the async result has rendered.
    assert(await details.evaluate((el, original) => el === original, original), "polling replaced the hovered row");
    assert.equal(await details.evaluate(el => getComputedStyle(el).cursor), cursor);
    assert.equal(await details.evaluate(el => el.tagName), "DIV");
    stable(box, await details.boundingBox());
    stable(copyBox, await copyTarget.boundingBox());
    stable(useBox, await useTarget.boundingBox());
    await copy.focus(); // Do not refresh the held snapshot when focus enters a hovered list.
    await search.hover();
    assert.equal(await details.evaluate(el => el.tagName), "DIV");
    await search.focus();
    const failure = page.getByRole("button", { name: "Show schema error for public.events" });
    await failure.waitFor();
    await failure.focus();
    await page.evaluate(() => window.dispatchEvent(new CustomEvent("hover-fixture-status", { detail: "loaded" })));
    await page.getByRole("radio", { name: "Failed (0)" }).waitFor();
    assert(await failure.evaluate(el => document.activeElement === el), "polling removed the focused error action");
    await failure.press("Enter");
    await page.getByRole("region", { name: "Schema error" }).waitFor();
    assert.equal(await page.getByLabel("Full schema error").textContent(), "Schema failure for events");
    stable(copyBox, await copyTarget.boundingBox());
    stable(useBox, await useTarget.boundingBox());
    await page.getByRole("button", { name: "Close schema error" }).click();
    assert(await failure.evaluate(el => document.activeElement === el), "error close must restore focus");
    await search.focus();
    await page.getByLabel("Schema Loaded for public.events").waitFor();
    assert.equal(await details.evaluate(el => el.tagName), "DIV");
    await page.close();

    for (const width of [1440, 800, 390]) {
      const sidebar = await browser.newPage({ viewport: { width, height: 1000 } });
      sidebar.on("pageerror", error => errors.push(error.message));
      await sidebar.goto(base + "data-schema-dialog-smoke.html");
      await sidebar.evaluate(async ([design, theme]) => {
        Object.assign(document.documentElement.dataset, { design, theme });
        await document.fonts.ready;
      }, [design, theme]);
      for (const name of ["Data viewer", "Schema viewer", "Schema widget", "About"]) {
        await sweep(sidebar, sidebar.getByRole("button", { name, exact: true }), "pointer");
      }
      await sidebar.close();
    }
    assert.deepEqual(errors, []);
  }
  console.log("PASS: Boolean label cursors, stable catalog hit targets through polling/focus, and sidebar hover in both designs/themes at 1440/800/390px.");
} finally { await browser?.close(); await server.close(); }
