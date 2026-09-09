import assert from "node:assert/strict";
import { fileURLToPath, pathToFileURL } from "node:url";
import { createServer } from "vite";

const modulePath = process.env.TRANSFERIA_PLAYWRIGHT_MODULE;
const { chromium } = await import(modulePath ? pathToFileURL(modulePath).href : "playwright");
const server = await createServer({ root: fileURLToPath(new URL("../", import.meta.url)), configFile: false,
  server: { host: "127.0.0.1" } });
const stable = (before, after, message) => {
  assert(before && after, `${message}: missing target`);
  for (const key of ["x", "y", "width", "height"]) assert(Math.abs(before[key] - after[key]) < 0.7, `${message}: ${key} moved`);
};
let browser;
try {
  await server.listen();
  browser = await chromium.launch({ headless: true,
    ...(process.env.TRANSFERIA_BROWSER_EXECUTABLE ? { executablePath: process.env.TRANSFERIA_BROWSER_EXECUTABLE } : {}) });
  for (const mode of ["tables", "parsed"]) for (const width of [1440, 800, 390]) {
    const page = await browser.newPage({ viewport: { width, height: 900 } });
    const errors = [];
    page.on("pageerror", error => errors.push(error.message));
    let receive;
    let count = 0;
    await page.route("**/api/v1/source/preview", route => { count++; receive(route); });
    await page.goto(`http://127.0.0.1:${server.httpServer.address().port}/tests/fixtures/source-data-viewer-smoke.html?mode=${mode}`);
    await page.evaluate(() => document.fonts.ready);
    const launcher = page.getByRole("button", { name: "Data viewer", exact: true });
    const launcherBox = await launcher.boundingBox();
    const schema = page.getByRole("button", { name: "Schema widget", exact: true });
    const schemaBox = await schema.boundingBox();
    await launcher.click();
    const dialog = page.getByRole("dialog", { name: "Data viewer", exact: true });
    await dialog.waitFor();
    const sample = page.getByRole("button", { name: "Load sample", exact: true });
    const close = page.getByRole("button", { name: "Close data viewer", exact: true });
    if (mode === "tables") await page.getByRole("button", { name: /public\.events/ }).waitFor();
    const targets = [dialog, sample, close, page.getByLabel("Sample rows"), page.getByLabel("Source sample")];
    const boxes = await Promise.all(targets.map(target => target.boundingBox()));
    for (const state of ["success", "error"]) {
      const next = new Promise(resolve => { receive = resolve; });
      await sample.click();
      const route = await next;
      assert.equal(await sample.getAttribute("aria-busy"), "true");
      await sample.evaluate(element => element.click());
      assert.equal(count, state === "success" ? 1 : 2, "pending requests must be deduplicated");
      for (let i = 0; i < targets.length; i++) stable(boxes[i], await targets[i].boundingBox(), `${width}/pending/${i}`);
      if (state === "success") {
        await route.fulfill({ json: { frames: [{ table: { namespace: "public", name: "events" },
          columns: [{ name: "id", arrow_type: "Int64", nullable: false, metadata: {} }],
          rows: Array.from({ length: 20 }, () => ({ id: "9007199254740993" })) }] } });
        await page.getByRole("cell", { name: "9007199254740993", exact: true }).first().waitFor();
      } else {
        await route.fulfill({ status: 500, contentType: "text/plain", body: "Sample failed: " + "detail ".repeat(300) });
        await page.locator(".source-data-viewer .transform-preview-status.error").waitFor();
      }
      for (let i = 0; i < targets.length; i++) stable(boxes[i], await targets[i].boundingBox(), `${width}/${state}/${i}`);
    }
    await close.click();
    stable(launcherBox, await launcher.boundingBox(), "launcher after close");
    stable(schemaBox, await schema.boundingBox(), "schema widget after close");
    assert(await launcher.evaluate(element => element === document.activeElement), "focus returns to launcher");
    assert.deepEqual(errors, []);
    await page.close();
  }
  console.log("PASS: Data viewer pending/result/error geometry and duplicate-activation protection.");
} finally {
  await browser?.close();
  await server.close();
}
