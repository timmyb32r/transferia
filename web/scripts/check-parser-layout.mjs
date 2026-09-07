import assert from "node:assert/strict";
import { fileURLToPath, pathToFileURL } from "node:url";
import { createServer } from "vite";

const modulePath = process.env.TRANSFERIA_PLAYWRIGHT_MODULE;
const { chromium } = await import(modulePath ? pathToFileURL(modulePath).href : "playwright");
const server = await createServer({ root: fileURLToPath(new URL("../", import.meta.url)), configFile: false,
  server: { host: "127.0.0.1" } });
let browser;
const variants = [
  ["s3", "s3_json"],
  ...["kafka", "logbroker"].flatMap(connector => ["json_parser", "tskv", "schema_registry", "debezium", "raw_to_table"]
    .map(parser => [connector, parser])),
];
const close = (actual, expected, message) => assert(Math.abs(actual - expected) < 0.7, `${message}: ${actual} != ${expected}`);

async function measure(page) {
  return page.locator(".parser-details-card").evaluate(card => {
    const rect = element => {
      const { x, y, width, height, bottom } = element.getBoundingClientRect();
      return { x, y, width, height, bottom };
    };
    const style = getComputedStyle(card);
    const cardRect = rect(card);
    const left = cardRect.x + parseFloat(style.borderLeftWidth) + parseFloat(style.paddingLeft);
    const width = cardRect.width - parseFloat(style.borderLeftWidth) - parseFloat(style.borderRightWidth)
      - parseFloat(style.paddingLeft) - parseFloat(style.paddingRight);
    const rows = [...card.querySelectorAll(".form-row")].filter(row => !row.closest(".column-editor") && row.getBoundingClientRect().width > 0)
      .map(row => {
        const field = row.querySelector(":scope > .field-control");
        const trigger = [...field.querySelectorAll(".select-trigger")]
          .find(button => !button.closest(".column-editor") && button.closest(".form-row") === row);
        const ancestors = [];
        for (let parent = row; parent && parent !== card; parent = parent.parentElement) {
          if (parent.matches(".form-row")) ancestors.unshift(parent.dataset.fieldName);
        }
        return { path: ancestors.join("/"), name: row.dataset.fieldName, field: rect(field),
          label: rect(row.querySelector(":scope > .field-label")), trigger: trigger ? rect(trigger) : null,
          containsSchema: field.querySelector(".column-editor") !== null };
      });
    const schema = card.querySelector(".column-editor");
    return { left, width, card: cardRect, rows, schema: schema ? rect(schema) : null,
      compact: [...card.querySelectorAll(".parser-scalar-section")].map(rect),
      pageWidth: document.documentElement.scrollWidth,
      overflowing: [...card.querySelectorAll("*")].filter(element => element.getBoundingClientRect().right > innerWidth)
        .slice(0, 8).map(element => ({ tag: element.tagName, class: element.className, ...rect(element) })) };
  });
}

function checkLayout(layout, viewport, context) {
  const expectedWidth = Math.max(layout.width * 0.6, Math.min(layout.width, 320));
  assert(layout.pageWidth <= viewport.width, `${context}: page overflows horizontally: ${JSON.stringify(layout.overflowing)}`);
  for (const section of layout.compact) {
    close(section.x, layout.left, `${context}: scalar section alignment`);
    close(section.width, expectedWidth, `${context}: scalar width must be applied exactly once`);
  }
  for (const row of layout.rows) {
    close(row.field.x, row.label.x, `${context}/${row.path}: label/control alignment`);
    close(row.field.y - row.label.bottom, 6, `${context}/${row.path}: label/control gap`);
    assert(row.field.x >= layout.left - 0.7 && row.field.x + row.field.width <= layout.left + layout.width + 0.7,
      `${context}/${row.path}: field escapes island`);
    if (row.trigger) {
      close(row.trigger.x, row.field.x, `${context}/${row.path}: dropdown alignment`);
      close(row.trigger.width, row.field.width, `${context}/${row.path}: dropdown width`);
    }
  }
  if (layout.schema) {
    close(layout.schema.x, layout.left, `${context}: output schema alignment`);
    close(layout.schema.width, layout.width, `${context}: output schema must remain full-width`);
  }
  const error = layout.rows.find(row => row.name === "conversion_error");
  const unknown = layout.rows.find(row => row.name === "unknown_fields");
  if (error && unknown && unknown.field.x > error.field.x + 1) {
    close(error.label.y, unknown.label.y, `${context}: policy labels must align despite nested options`);
    close(error.trigger.y, unknown.trigger.y, `${context}: policy dropdowns must align`);
    close(error.field.width, unknown.field.width, `${context}: policy columns must be equal`);
  }
}

try {
  await server.listen(0);
  const address = server.httpServer.address();
  browser = await chromium.launch({ headless: true,
    ...(process.env.TRANSFERIA_BROWSER_EXECUTABLE ? { executablePath: process.env.TRANSFERIA_BROWSER_EXECUTABLE } : {}) });
  let cases = 0;
  for (const width of [1440, 800, 390]) {
    const viewport = { width, height: 1000 };
    const references = new Map();
    let tableNaming;
    for (const [connector, parser] of variants) {
      const page = await browser.newPage({ viewport });
      const errors = [];
      page.on("pageerror", error => errors.push(error.message));
      await page.goto(`http://127.0.0.1:${address.port}/tests/fixtures/parser-layout-smoke.html?connector=${connector}&parser=${parser}`);
      await page.locator(".parser-details-card").waitFor();
      await page.evaluate(() => document.fonts.ready);
      const context = `${connector}/${parser}/${width}px`;
      const layout = await measure(page);
      checkLayout(layout, viewport, context);
      const family = parser === "s3_json" ? "json_parser" : parser;
      const reference = references.get(family);
      if (!reference) references.set(family, layout);
      else for (const row of reference.rows) {
        const counterpart = layout.rows.find(candidate => candidate.path === row.path);
        assert(counterpart, `${context}: missing shared field ${row.path}`);
        close(counterpart.field.x - layout.left, row.field.x - reference.left, `${context}/${row.path}: source-independent x`);
        close(counterpart.field.width, row.field.width, `${context}/${row.path}: source-independent width`);
      }
      const naming = layout.rows.find(row => row.name === "table_naming");
      if (naming) {
        if (!tableNaming) tableNaming = { x: naming.field.x, width: naming.field.width, y: naming.field.y - layout.card.y };
        close(naming.field.x, tableNaming.x, `${context}: shared table-name x across parsers`);
        close(naming.field.width, tableNaming.width, `${context}: shared table-name width across parsers`);
        close(naming.field.y - layout.card.y, tableNaming.y, `${context}: shared table-name y across parsers`);
        const selector = page.locator('[data-field-name="table_naming"] .select-trigger').first();
        const before = await selector.boundingBox();
        await selector.click();
        const open = await selector.boundingBox();
        close(open.x, before.x, `${context}: opening a menu must not move its trigger`);
        close(open.y, before.y, `${context}: opening a menu must not shift its trigger vertically`);
        close(open.width, before.width, `${context}: opening a menu must not resize its trigger`);
        await page.getByRole("option", { name: "From config", exact: true }).click();
        const selected = await selector.boundingBox();
        close(selected.x, before.x, `${context}: nested settings must not move their selector`);
        close(selected.y, before.y, `${context}: nested settings must not shift their selector vertically`);
        close(selected.width, before.width, `${context}: nested settings must not narrow their selector`);
        await page.locator('[data-field-name="table_naming"] [data-field-name="name"] input').fill("analytics_events");
        checkLayout(await measure(page), viewport, `${context}/nested naming`);
      }
      if (["schema_registry", "debezium"].includes(parser)) {
        await page.locator('[data-field-name="auth"] .select-trigger').first().click();
        await page.getByRole("option", { name: "Username and password", exact: true }).click();
        checkLayout(await measure(page), viewport, `${context}/nested authentication`);
      }
      assert.deepEqual(errors, [], `${context}: browser errors`);
      await page.close();
      cases++;
    }
  }
  console.log(`PASS: ${cases} actual-catalog parser layouts; shared geometry, nested controls, dropdown stability and full-width schemas.`);
} finally {
  await browser?.close();
  await server.close();
}
