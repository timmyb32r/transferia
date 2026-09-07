import assert from "node:assert/strict";
import { fileURLToPath, pathToFileURL } from "node:url";
import { createServer } from "vite";

const modulePath = process.env.TRANSFERIA_PLAYWRIGHT_MODULE;
const { chromium } = await import(modulePath ? pathToFileURL(modulePath).href : "playwright");
const server = await createServer({
  root: fileURLToPath(new URL("../", import.meta.url)), configFile: false,
  server: { host: "127.0.0.1" },
});
let browser;
try {
  await server.listen(0);
  const address = server.httpServer.address();
  browser = await chromium.launch({ headless: true,
    ...(process.env.TRANSFERIA_BROWSER_EXECUTABLE ? { executablePath: process.env.TRANSFERIA_BROWSER_EXECUTABLE } : {}),
  });
  let cycles = 0;
  for (const viewport of [{ width: 1440, height: 900 }, { width: 900, height: 720 }]) {
    for (const kind of ["sql", "unselected"]) {
      for (const bottomAnchor of [false, true]) {
        const page = await browser.newPage({ viewport });
        const errors = [];
        page.on("pageerror", error => errors.push(error.message));
        await page.goto(`http://127.0.0.1:${address.port}/tests/fixtures/middleware-smoke.html?last-transform=${kind}`);
        const toggle = page.locator(".middleware-strip-toggle").last();
        await toggle.waitFor();
        await page.addStyleTag({ content: ".route-composition { min-height: 1100px; }" });
        if (bottomAnchor) {
          // Deterministically select a native scroll anchor below the strip.
          // Real anchor selection depends on prior focus/scroll history. Do not
          // simulate it by calling scrollTo on expansion: let the browser do it.
          await page.addStyleTag({ content: ".route-composition, .middleware-island { overflow-anchor: none; }" });
          await toggle.evaluate(element => element.addEventListener("pointerdown", event => event.preventDefault()));
        }
        await page.evaluate(() => document.fonts.ready);
        await page.evaluate(() => window.scrollTo(0, document.documentElement.scrollHeight));
        await checkRenameGeometry(page);
        for (const activation of ["pointer", "Enter", "Space", "pointer", "Enter", "Space"]) {
          await page.getByLabel("Following field", { exact: true }).focus();
          if (activation !== "pointer") await toggle.focus();
          await page.evaluate(() => window.scrollTo(0, document.documentElement.scrollHeight));
          await page.evaluate(() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve))));
          const before = await toggle.boundingBox();
          const scroll = await page.evaluate(() => window.scrollY);
          assert(scroll > 0, "fixture must overflow the viewport");
          assert(before.y > 0 && before.y + before.height < viewport.height, "last header must be visible at page bottom");
          const pointer = { x: before.x + before.width / 2, y: before.y + before.height / 2 };
          // Start sampling BEFORE activation to catch even a single-frame jump.
          await toggle.evaluate(element => {
            window.transformFrames = [];
            window.stopTransformFrames = false;
            const sample = () => {
              const rect = element.getBoundingClientRect();
              window.transformFrames.push({ y: rect.y, height: rect.height, scroll: window.scrollY });
              if (!window.stopTransformFrames) requestAnimationFrame(sample);
            };
            sample();
          });
          if (activation === "pointer") await page.mouse.click(pointer.x, pointer.y);
          else await page.keyboard.press(activation);
          assert.equal(await toggle.getAttribute("aria-expanded"), "true");
          // Includes the delayed matched-table response, not just the DOM commit.
          await page.evaluate(() => new Promise(resolve => {
            let frames = 0;
            const next = () => ++frames === 24 ? resolve() : requestAnimationFrame(next);
            requestAnimationFrame(next);
          }));
          const frames = await page.evaluate(() => {
            window.stopTransformFrames = true;
            return window.transformFrames;
          });
          const context = `${viewport.width}px, ${kind}, bottomAnchor=${bottomAnchor}, ${activation}`;
          for (const frame of frames) {
            assert(Math.abs(frame.y - before.y) < 0.6,
              `${context}: header jumped ${before.y} -> ${frame.y}; scroll ${scroll} -> ${frame.scroll}`);
            assert(Math.abs(frame.height - before.height) < 0.6, `${context}: header resized`);
          }
          assert(await toggle.evaluate((element, point) => element.contains(document.elementFromPoint(point.x, point.y)), pointer),
            `${context}: the header must remain under the original pointer coordinates`);
          assert(await toggle.evaluate(element => document.activeElement === element), `${context}: focus must stay on the toggle`);
          assert.equal(await page.evaluate(() => document.documentElement.style.getPropertyValue("overflow-anchor")), "",
            `${context}: native scroll anchoring must be restored before the next interaction`);
          // Scroll remains usable; no persistent offset-restoration loop.
          await page.mouse.wheel(0, 60);
          await page.waitForFunction(previous => window.scrollY > previous, scroll);
          await page.evaluate(offset => window.scrollTo(0, offset), scroll);
          await page.mouse.click(pointer.x, pointer.y);
          assert.equal(await toggle.getAttribute("aria-expanded"), "false");
          const collapsed = await toggle.evaluate(element => new Promise(resolve => {
            const frames = [];
            const sample = () => {
              frames.push(element.getBoundingClientRect().y);
              if (frames.length === 4) resolve(frames);
              else requestAnimationFrame(sample);
            };
            sample();
          }));
          assert(collapsed.every(y => Math.abs(y - before.y) < 0.6), `${context}: header jumped when closing at the original position`);
          assert(await toggle.evaluate((element, point) => element.contains(document.elementFromPoint(point.x, point.y)), pointer),
            `${context}: collapsed header must remain under the pointer`);
          cycles++;
        }
        assert.deepEqual(errors, [], "browser errors");
        await page.close();
      }
    }
  }
  console.log(`PASS: transform naming keeps controls stationary; last transform expands downward (${cycles} bottom-of-page cycles).`);
} finally {
  await browser?.close();
  await server.close();
}

async function checkRenameGeometry(page) {
  const strip = page.locator(".middleware-strip").last();
  const trigger = strip.getByRole("button", { name: /Actions for transform/ });
  const toggle = strip.locator(".middleware-strip-toggle");
  const measure = () => page.locator(".middleware-strip-heading, .middleware-strip-actions > button, .middleware-name-action > button, .middleware-add").evaluateAll(elements =>
    elements.map(element => {
      const { x, y, width, height } = element.getBoundingClientRect();
      return { x, y, width, height };
    }));
  const before = await measure();
  const unchanged = async () => {
    // Sample several paints, including the focus and state commits.
    for (let frame = 0; frame < 4; frame++) {
      await page.evaluate(() => new Promise(resolve => requestAnimationFrame(resolve)));
      const after = await measure();
      assert.equal(after.length, before.length);
      after.forEach((rect, index) => {
        for (const key of ["x", "y", "width", "height"])
          assert(Math.abs(rect[key] - before[index][key]) < 0.6, `rename moved control ${index}: ${key}`);
      });
      assert.equal(await toggle.getAttribute("aria-expanded"), "false");
    }
  };
  const name = "  Подготовка событий 🦀 — ".repeat(20);
  for (const value of [name, ""]) {
    await trigger.click();
    await unchanged();
    await page.getByRole("menuitem", { name: value ? "Add name" : "Rename", exact: true }).click();
    await unchanged();
    const dialog = page.getByRole("dialog", { name: "Transformation name" });
    const bounds = await dialog.boundingBox();
    const viewport = page.viewportSize();
    assert(bounds.x >= 0 && bounds.y >= 0 && bounds.x + bounds.width <= viewport.width && bounds.y + bounds.height <= viewport.height,
      "rename dialog must stay inside the viewport");
    const field = dialog.getByRole("textbox", { name: "Transformation name" });
    assert(await field.evaluate(element => document.activeElement === element), "rename input must receive focus");
    await field.fill(value);
    await unchanged();
    await dialog.getByRole("button", { name: "Save", exact: true }).click();
    await unchanged();
    assert(await trigger.evaluate(element => document.activeElement === element), "save must return focus to overflow");
    if (value) {
      assert.equal(await strip.locator(".middleware-strip-title").textContent(), value);
      assert.equal(await strip.locator(".middleware-strip-title").getAttribute("title"), value);
      assert.equal(await strip.locator(".middleware-strip-type").count(), 1);
    } else assert.equal(await strip.locator(".middleware-strip-type").count(), 0);
  }
}
