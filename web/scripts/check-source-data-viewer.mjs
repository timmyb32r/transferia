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
  for (const width of [1440, 800, 390]) {
    const page = await browser.newPage({ viewport: { width, height: 900 } });
    await page.goto(`http://127.0.0.1:${server.httpServer.address().port}/tests/fixtures/source-data-viewer-smoke.html`);
    await page.evaluate(() => document.fonts.ready);
    const names = ["Data viewer", "Schema viewer", "Schema widget", "About"];
    const buttons = names.map(name => page.getByRole("button", { name, exact: true }));
    const boxes = await Promise.all(buttons.map(button => button.boundingBox()));
    assert(boxes.every(Boolean), "all sidebar tools must be visible");
    const [data, schema, widget, about] = boxes;
    const near = (a, b, message) => assert(Math.abs(a - b) < 0.7, `${width}: ${message}`);
    near(data.x, schema.x, "viewers share the left edge");
    near(data.x + data.width, widget.x + widget.width, "widget completes the full-width schema row");
    near(schema.y, widget.y, "schema actions share one row");
    near(schema.height, widget.height, "schema actions have equal height");
    near(widget.width, widget.height, "widget button is square");
    near(data.x, about.x, "About stays full-width");
    near(data.width, about.width, "Data viewer occupies the full tools width");
    const gap = schema.y - data.y - data.height;
    assert(gap > 0, "viewer rows must be separated");
    near(widget.x - schema.x - schema.width, gap, "schema buttons use the common gap");
    near(about.y - schema.y - schema.height, gap, "About uses the common vertical gap");
    const icon = buttons[2].locator(".schema-widget-icon");
    const iconBox = await icon.boundingBox();
    assert(iconBox, "widget icon must be visible");
    near(iconBox.x + iconBox.width / 2, widget.x + widget.width / 2, "widget icon is horizontally centered");
    near(iconBox.y + iconBox.height / 2, widget.y + widget.height / 2, "widget icon is vertically centered");
    for (const state of [{ available: false, pending: false }, { available: true, pending: true }, { available: true, pending: false }]) {
      await page.evaluate(state => window.dispatchEvent(new CustomEvent("sidebar-fixture-state", { detail: state })), state);
      await page.waitForFunction(state => {
        const button = document.querySelector(".sidebar-tools > .sidebar-tool-tooltip > button");
        return button?.disabled === !state.available && button.getAttribute("aria-busy") === String(state.pending);
      }, state);
      for (let i = 0; i < buttons.length; i++) stable(boxes[i], await buttons[i].boundingBox(), `${width}/${names[i]}/${JSON.stringify(state)}`);
      assert.equal(await buttons[1].isDisabled(), !state.available || state.pending, "Schema viewer exposes its diagnostic/pending state");
      assert.equal(await buttons[2].isDisabled(), !state.available);
    }
    await page.close();
  }
  for (const mode of ["tables", "parsed"]) for (const width of [1440, 800, 390]) {
    const page = await browser.newPage({ viewport: { width, height: 900 } });
    const errors = [];
    page.on("pageerror", error => errors.push(error.message));
    let receive;
    let next = new Promise(resolve => { receive = resolve; });
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
    assert.equal(await page.getByLabel("Max sample MiB").count(), 0);
    assert.equal(await page.getByLabel("Timeout seconds").count(), 0);
    const targets = [dialog, sample, close, page.getByLabel("Sample rows"), page.getByLabel("Source sample")];
    if (mode === "tables") targets.push(page.locator(".source-data-viewer-controls .select-trigger"));
    const boxes = await Promise.all(targets.map(target => target.boundingBox()));
    for (const state of ["success", "error"]) {
      if (state === "error") {
        next = new Promise(resolve => { receive = resolve; });
        if (mode === "tables") {
          await page.getByRole("button", { name: "public.events", exact: true }).click();
          await page.getByRole("option", { name: "public.users", exact: true }).click();
        } else {
          await page.getByLabel("Sample rows").fill("42");
          assert.equal(count, 1, "row edits must not load automatically");
          await sample.click();
        }
      }
      const route = await next;
      assert.equal(route.request().postDataJSON().table?.name, mode === "tables" ? state === "success" ? "events" : "users" : undefined);
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
  console.log("PASS: Two-row sidebar viewers, stable readiness/pending geometry, Data viewer automatic opening/table loads and duplicate-activation protection.");
} finally {
  await browser?.close();
  await server.close();
}
