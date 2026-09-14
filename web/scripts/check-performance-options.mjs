import assert from "node:assert/strict";
import { pathToFileURL } from "node:url";

const modulePath = process.env.TRANSFERIA_PLAYWRIGHT_MODULE;
const { chromium } = await import(modulePath ? pathToFileURL(modulePath).href : "playwright");
const browser = await chromium.launch({ headless: true,
  ...(process.env.TRANSFERIA_BROWSER_EXECUTABLE ? { executablePath: process.env.TRANSFERIA_BROWSER_EXECUTABLE } : {}),
});
const base = process.env.TRANSFERIA_UI_URL ?? "http://127.0.0.1:5184/tests/fixtures/performance-options-smoke.html";
const sameBox = (before, after) => {
  assert(before && after);
  for (const key of ["x", "y", "width", "height"])
    assert(Math.abs(before[key] - after[key]) < 0.6, `${key}: ${before[key]} -> ${after[key]}`);
};
try {
  for (const width of [1200, 375]) for (const theme of ["light", "dark"]) {
    const page = await browser.newPage({ viewport: { width, height: 800 } });
    const errors = [];
    page.on("pageerror", error => errors.push(error.message));
    for (const endpoint of ["Source", "Destination"]) for (const first of ["advanced", "performance"]) {
      await page.goto(`${base}?theme=${theme}`);
      const card = page.locator(`[data-endpoint="${endpoint}"]`);
      const headers = card.locator(".options-foldouts > details > summary");
      await headers.last().waitFor();
      await headers.first().scrollIntoViewIfNeeded();
      // Destination is the last card on mobile: this also exercises opening at
      // the exact document bottom, where browser scroll anchoring caused jumps.
      if (endpoint === "Destination") await page.evaluate(() => window.scrollTo(0, document.documentElement.scrollHeight));
      const boxes = await Promise.all((await headers.all()).map(header => header.boundingBox()));
      assert(Math.abs(boxes[0].y - boxes[1].y) < 0.6);
      assert(boxes[0].x + boxes[0].width <= boxes[1].x);
      const order = first === "advanced" ? [0, 1] : [1, 0];
      for (const index of order) {
        const header = headers.nth(index);
        if (index === order[0]) {
          const box = await header.boundingBox();
          await page.mouse.move(box.x + box.width / 2, box.y + box.height / 2);
          await page.mouse.down();
          sameBox(box, await header.boundingBox());
          await page.mouse.up();
        } else {
          await header.focus();
          await page.keyboard.press("Enter");
        }
        assert(await header.evaluate(element => element.parentElement.open));
        await page.evaluate(() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve))));
        for (let i = 0; i < 2; i++) sameBox(boxes[i], await headers.nth(i).boundingBox());
        const body = card.locator(".foldout-content").nth(index);
        assert((await body.boundingBox()).width > boxes[index].width * 1.5, "body must use the full form width");
      }
      assert(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth));
    }
    assert.deepEqual(errors, []);
    await page.close();
  }
  console.log("Performance options: adjacent headers stay anchored in both themes and viewport sizes.");
} finally {
  await browser.close();
}
