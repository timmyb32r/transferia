import assert from "node:assert/strict";
import { mkdir } from "node:fs/promises";
import { pathToFileURL } from "node:url";

const modulePath = process.env.TRANSFERIA_PLAYWRIGHT_MODULE;
const { chromium } = await import(modulePath ? pathToFileURL(modulePath).href : "playwright");
const browser = await chromium.launch({ headless: true,
  ...(process.env.TRANSFERIA_BROWSER_EXECUTABLE ? { executablePath: process.env.TRANSFERIA_BROWSER_EXECUTABLE } : {}),
});
const page = await browser.newPage({ viewport: { width: 1100, height: 1100 } });
const errors = [];
page.on("pageerror", error => errors.push(error.message));
const output = process.env.TRANSFERIA_UI_ARTIFACTS ?? "/tmp/transferia-button-ui";
await mkdir(output, { recursive: true });
const paint = locator => locator.evaluate(element => {
  const style = getComputedStyle(element);
  const canvas = document.createElement("canvas");
  canvas.width = canvas.height = 1;
  const context = canvas.getContext("2d");
  const rgb = value => {
    context.clearRect(0, 0, 1, 1);
    context.fillStyle = value;
    context.fillRect(0, 0, 1, 1);
    return [...context.getImageData(0, 0, 1, 1).data];
  };
  return { text: rgb(style.color), surface: rgb(style.backgroundColor), border: rgb(style.borderTopColor),
    shadow: style.boxShadow, outline: Number.parseFloat(style.outlineWidth) };
});
const sameBox = (before, after) => {
  for (const key of ["x", "y", "width", "height"]) {
    assert(Math.abs(before[key] - after[key]) < 0.6, `${key} moved: ${before[key]} -> ${after[key]}`);
  }
};
const luminance = rgb => rgb.slice(0, 3).map(value => value / 255)
  .map(value => value <= 0.04045 ? value / 12.92 : ((value + 0.055) / 1.055) ** 2.4)
  .reduce((sum, value, index) => sum + value * [0.2126, 0.7152, 0.0722][index], 0);
const contrast = colors => (luminance(colors.surface) + 0.05) / (luminance(colors.text) + 0.05);
try {
  await page.goto(process.env.TRANSFERIA_UI_URL ?? "http://127.0.0.1:5184/tests/fixtures/button-style-smoke.html");
  await page.locator('[data-action="add"]').waitFor();
  const report = [];
  for (const action of await page.locator("[data-action]").all()) {
    await page.mouse.move(0, 0);
    await page.waitForTimeout(180); // Let the shared color transition settle.
    const box = await action.boundingBox(), idle = await paint(action);
    assert.deepEqual(idle.surface, [255, 255, 255, 255]);
    assert(contrast(idle) >= 4.5);
    await action.hover();
    await page.waitForTimeout(180);
    const hover = await paint(action);
    assert(contrast(hover) >= 4.5);
    assert.notDeepEqual(hover.surface, idle.surface);
    sameBox(box, await action.boundingBox());
    if (await action.getAttribute("data-action") === "add") {
      await page.screenshot({ path: `${output}/hover.png`, fullPage: true });
      await page.mouse.down();
      assert.notEqual((await paint(action)).shadow, "none");
      sameBox(box, await action.boundingBox());
      await page.mouse.up();
      await action.focus();
      assert((await paint(action)).outline >= 2);
      sameBox(box, await action.boundingBox());
    }
    report.push({ action: await action.getAttribute("data-action"), idleContrast: contrast(idle), hoverContrast: contrast(hover) });
  }
  for (const control of [page.getByRole("button", { name: "Add transform", exact: true }).nth(1),
    page.getByRole("button", { name: "Add locked rule" })]) {
    const idle = await paint(control);
    assert.deepEqual(idle.text, [137, 147, 157, 255]);
    assert.deepEqual(idle.surface, [232, 237, 241, 255]);
    await control.hover({ force: true });
    await page.waitForTimeout(180);
    assert.deepEqual(await paint(control), idle);
  }
  const enabledAdd = await page.locator('[data-action="add"]').boundingBox();
  const disabledAdd = await page.getByRole("button", { name: "Add transform", exact: true }).nth(1).boundingBox();
  for (const key of ["width", "height"]) assert.equal(enabledAdd[key], disabledAdd[key]);
  const pending = page.locator('[data-action="pending"]');
  const primary = page.getByRole("button", { name: "Check connection" });
  const pendingBox = await pending.boundingBox(), primaryBox = await primary.boundingBox();
  await pending.click();
  assert.equal(await pending.getAttribute("aria-busy"), "true");
  assert.equal(await pending.evaluate(element => getComputedStyle(element, "::after").content), '""');
  sameBox(pendingBox, await pending.boundingBox());
  sameBox(primaryBox, await primary.boundingBox());
  // A real repeat click must be rejected immediately, not deferred until idle.
  await page.mouse.click(pendingBox.x + pendingBox.width / 2, pendingBox.y + pendingBox.height / 2);
  assert.equal(await page.getByRole("status").textContent(), "Requests: 1");
  await page.waitForFunction(() => document.querySelector('[data-action="pending"]').getAttribute("aria-busy") === "false");
  sameBox(pendingBox, await pending.boundingBox());
  sameBox(primaryBox, await primary.boundingBox());
  assert.deepEqual((await paint(primary)).surface, [13, 148, 136, 255]);
  assert.deepEqual((await paint(page.getByRole("button", { name: "Delete", exact: true }))).text, [169, 55, 67, 255]);
  for (const control of [page.getByRole("tab", { name: "YAML" }), page.getByRole("radio", { name: "All tables" }),
    page.getByRole("button", { name: /^Matched tables/ })]) {
    assert.equal(await control.evaluate(element => element.classList.contains("secondary-button")), false);
  }
  await page.mouse.move(0, 0);
  await page.waitForTimeout(180);
  for (const width of [1100, 760, 390]) {
    await page.setViewportSize({ width, height: 1100 });
    assert(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1));
    await page.screenshot({ path: `${output}/width-${width}.png`, fullPage: true });
  }
  for (const [design, theme] of [["airy-v0", "dark"], ["classic", "light"], ["classic", "dark"]]) {
    await page.evaluate(([design, theme]) => {
      document.documentElement.dataset.design = design;
      document.documentElement.dataset.theme = theme;
    }, [design, theme]);
    const action = page.locator('[data-action="add"]');
    await page.waitForTimeout(180);
    const colors = await paint(action);
    await action.evaluate(element => element.classList.remove("secondary-button"));
    await page.waitForTimeout(180);
    assert.deepEqual(await paint(action), colors);
    await action.evaluate(element => element.classList.add("secondary-button"));
  }
  assert.deepEqual(errors, []);
  console.log(JSON.stringify({ passed: true, report, screenshots: output, widths: [1100, 760, 390] }));
} finally {
  await browser.close();
}
