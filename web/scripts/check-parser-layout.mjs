import assert from "node:assert/strict";
import { fileURLToPath, pathToFileURL } from "node:url";
import { createServer } from "vite";

const modulePath = process.env.TRANSFERIA_PLAYWRIGHT_MODULE;
const { chromium } = await import(modulePath ? pathToFileURL(modulePath).href : "playwright");
const server = await createServer({ root: fileURLToPath(new URL("../", import.meta.url)), configFile: false,
  server: { host: "127.0.0.1" } });
let browser;
const columnsOnly = process.argv.includes("--columns-only");
const variants = [
  ["s3", "s3_json"],
  ...["kafka", "logbroker"].flatMap(connector => ["json_parser", "tskv", "schema_registry", "debezium", "raw_to_table"]
    .map(parser => [connector, parser])),
].filter(([, parser]) => !columnsOnly || ["s3_json", "json_parser", "tskv"].includes(parser));
const close = (actual, expected, message) => assert(Math.abs(actual - expected) < 0.7, `${message}: ${actual} != ${expected}`);

async function checkColumnControls(page, context) {
  const rows = page.locator(".column-table .config-table-row");
  if (await rows.count() === 0) await page.getByRole("button", { name: "+ Add column", exact: true }).click();
  const row = rows.first();
  const arrow = row.locator(".arrow-type-cell .select-trigger");
  if (await arrow.count() === 0) return;
  const key = row.getByRole("checkbox", { name: /^Key / });
  assert(await key.isEnabled(), `${context}: unnamed Key must be interactive`);
  await key.check();
  assert(await key.isChecked(), `${context}: unnamed Key must show its selection immediately`);
  await row.locator('input[type="text"]').first().fill("event_time");
  assert(await key.isChecked(), `${context}: Key must survive entering the column name`);
  await arrow.scrollIntoViewIfNeeded();
  await arrow.hover();
  const geometry = () => row.evaluate(element => {
    const rect = node => {
      const { x, y, width, height } = node.getBoundingClientRect();
      return { x, y, width, height };
    };
    return { row: rect(element), arrow: rect(element.querySelector(".arrow-type-cell .select-trigger")),
      input: rect(element.querySelector('input[type="text"]')),
      selects: [...element.querySelectorAll(".select-trigger")].map(rect) };
  });
  const before = await geometry();
  close(before.arrow.height, before.input.height, `${context}: Arrow/input height`);
  for (const select of before.selects) close(select.height, before.arrow.height, `${context}: adjacent select height`);
  const unchanged = async phase => {
    const after = await geometry();
    for (const target of ["row", "arrow", "input"]) for (const dimension of ["x", "y", "width", "height"]) {
      close(after[target][dimension], before[target][dimension], `${context}/${phase}: stable ${target}.${dimension}`);
    }
  };
  await arrow.click();
  await unchanged("open");
  const options = row.getByRole("option");
  const labels = await options.allTextContents();
  const sizing = await arrow.evaluate((trigger, labels) => {
    const style = getComputedStyle(trigger);
    const probe = document.createElement("span");
    Object.assign(probe.style, { position: "fixed", visibility: "hidden", pointerEvents: "none", whiteSpace: "pre", width: "max-content" });
    trigger.append(probe);
    const widths = labels.map(label => {
      probe.textContent = label;
      return { label, width: probe.getBoundingClientRect().width };
    });
    probe.remove();
    const widest = widths.sort((a, b) => b.width - a.width)[0];
    const indicator = trigger.querySelector(".select-trigger-indicator");
    const chrome = parseFloat(style.paddingLeft) + parseFloat(style.paddingRight)
      + parseFloat(style.borderLeftWidth) + parseFloat(style.borderRightWidth)
      + indicator.getBoundingClientRect().width + parseFloat(getComputedStyle(indicator).marginLeft);
    const cell = trigger.closest("td");
    const cellStyle = getComputedStyle(cell);
    const available = cell.clientWidth - parseFloat(cellStyle.paddingLeft) - parseFloat(cellStyle.paddingRight);
    return { widest, chrome, available };
  }, labels);
  const longest = sizing.widest.label;
  close(before.arrow.width, Math.min(sizing.available, sizing.widest.width + sizing.chrome + 8),
    `${context}: trigger fits widest rendered option with 8px breathing room ${JSON.stringify(sizing)}`);
  assert(longest, `${context}: Arrow options must exist`);
  const option = row.getByRole("option", { name: longest, exact: true });
  await option.scrollIntoViewIfNeeded();
  const popup = await row.locator(".select-menu").evaluate(menu => {
    const rect = menu.getBoundingClientRect();
    return { left: rect.left, right: rect.right, width: rect.width,
      overflow: menu.scrollWidth > menu.clientWidth,
      optionsOverflow: [...menu.querySelectorAll('[role="option"]')].some(option => option.scrollWidth > option.clientWidth) };
  });
  const viewport = page.viewportSize();
  assert(popup.left >= 11 && popup.right <= viewport.width - 11, `${context}: menu must fit viewport`);
  close(popup.width, Math.min(before.arrow.width, viewport.width - 24), `${context}: Arrow menu matches trigger width`);
  assert(!popup.overflow && !popup.optionsOverflow, `${context}: long option labels must wrap without clipping`);
  await option.click();
  await unchanged("select longest");
  assert.equal(await arrow.getAttribute("title"), longest, `${context}: full selected type remains available`);
  if (sizing.available >= sizing.widest.width + sizing.chrome + 8) {
    assert(await arrow.locator(".select-value").evaluate(label => label.scrollWidth <= label.clientWidth),
      `${context}: widest selected label must fit without truncation`);
  }
}

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
  const expectedWidth = Math.min(480, Math.max(layout.width * 0.6, Math.min(layout.width, 320)));
  assert(layout.pageWidth <= viewport.width, `${context}: page overflows horizontally: ${JSON.stringify(layout.overflowing)}`);
  for (const section of layout.compact) {
    close(section.x, layout.left, `${context}: scalar section alignment`);
    close(section.width, expectedWidth, `${context}: scalar width must be applied exactly once`);
  }
  for (const row of layout.rows) {
    if (!row.containsSchema) assert(row.field.width <= expectedWidth + 0.7,
      `${context}/${row.path}: scalar controls must not exceed the compact width cap`);
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
  for (const width of [2560, 1440, 800, 390]) {
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
      if (columnsOnly) {
        await checkColumnControls(page, context);
        assert.deepEqual(errors, [], `${context}: browser errors`);
        await page.close();
        cases++;
        continue;
      }
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
      if (["s3_json", "json_parser", "tskv"].includes(parser)) await checkColumnControls(page, context);
      assert.deepEqual(errors, [], `${context}: browser errors`);
      await page.close();
      cases++;
    }
  }
  console.log(columnsOnly
    ? `PASS: ${cases} actual-catalog column editors; compact Arrow controls, readable menus, stable row geometry and unnamed Keys.`
    : `PASS: ${cases} actual-catalog parser layouts; shared geometry, nested controls, dropdown stability and full-width schemas.`);
} finally {
  await browser?.close();
  await server.close();
}
