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
    const errors = [];
    page.on("pageerror", error => errors.push(error.message));
    await page.goto(`http://127.0.0.1:${server.httpServer.address().port}/tests/fixtures/data-schema-dialog-smoke.html`);
    await page.evaluate(() => document.fonts.ready);
    const launcher = page.getByRole("button", { name: "Schema viewer", exact: true });
    const editor = page.getByRole("button", { name: "Editor target" });
    const launcherBox = await launcher.boundingBox(), editorBox = await editor.boundingBox();
    assert.equal(await page.getByRole("tab", { name: "Schema viewer" }).count(), 0);
    await launcher.click();
    const dialog = page.getByRole("dialog", { name: "Schema viewer" });
    const close = page.getByRole("button", { name: "Close Schema viewer" });
    await dialog.waitFor();
    const targets = [dialog, close, page.getByLabel("Discovered schemas")];
    const boxes = await Promise.all(targets.map(target => target.boundingBox()));
    for (const table of ["users", "events"]) {
      await page.locator(".contract-dataset-selector .select-trigger").click();
      await page.getByRole("option", { name: table, exact: true }).click();
      for (let i = 0; i < targets.length; i++) stable(boxes[i], await targets[i].boundingBox(), `${width}/${table}/${i}`);
    }
    for (const state of ["loading", "error", "ready"]) {
      await page.evaluate(state => window.dispatchEvent(new CustomEvent("schema-fixture-state", { detail: state })), state);
      if (state === "ready") await page.getByRole("button", { name: "events", exact: true }).waitFor();
      else await page.getByRole("status").waitFor();
      for (let i = 0; i < targets.length; i++) stable(boxes[i], await targets[i].boundingBox(), `${width}/${state}/${i}`);
    }
    await close.focus();
    await page.keyboard.press("Shift+Tab");
    assert(await dialog.evaluate(element => element.contains(document.activeElement)), "focus remains inside popup");
    await page.keyboard.press("Tab");
    assert(await close.evaluate(element => element === document.activeElement), "Tab wraps back to Close");
    let reachedLimits = false;
    let wrapped = false;
    for (let step = 0; step < 10; step++) {
      await page.keyboard.press("Tab");
      assert(await dialog.evaluate(element => element.contains(document.activeElement)), "forward Tab cannot leave popup");
      if (await page.locator(".sink-limits > summary").evaluate(element => element === document.activeElement)) reachedLimits = true;
      if (await close.evaluate(element => element === document.activeElement)) { wrapped = true; break; }
    }
    assert(reachedLimits, "Destination limits disclosure must be reachable with Tab");
    assert(wrapped, "forward Tab wraps from Destination limits back to Close");
    await page.keyboard.press("Escape");
    assert.equal(await dialog.count(), 0);
    stable(launcherBox, await launcher.boundingBox(), "sidebar launcher");
    stable(editorBox, await editor.boundingBox(), "underlying editor");
    assert(await launcher.evaluate(element => element === document.activeElement), "focus returns to sidebar launcher");
    assert.deepEqual(errors, []);
    await page.close();
  }
  console.log("PASS: Schema viewer popup geometry, editor preservation and keyboard focus.");
} finally {
  await browser?.close();
  await server.close();
}
