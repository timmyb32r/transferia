// @vitest-environment jsdom

import { cleanup } from "@testing-library/preact";
import { afterEach, expect, it } from "vitest";
import catalog from "../../crates/transferia-server-contracts/contracts/connector-catalog.fixture.json";
import { EndpointCard } from "../src/delivery/EndpointCard";
import { TableSelectionEditor } from "../src/features/tableSelection/TableSelectionEditor";
import type { ConnectorDefinition } from "../src/types";
import { render } from "./support/render";

afterEach(cleanup);

function Source({ connectorKey }: { connectorKey: string }) {
  const connector = catalog.connectors.find(item => item.key === connectorKey)! as unknown as ConnectorDefinition;
  const endpoint = connector.source!;
  return <EndpointCard title="Source" role="source" selectedKey={connectorKey} connectors={[connector]}
    endpoint={endpoint} config={{ source: { [connectorKey]: endpoint.initial } }} readOnly={false}
    showRequiredErrors={false} onChoose={() => undefined} onConfig={() => undefined} />;
}

it.each([
  { connector: "postgres", namespace: "schema", unrelated: "database" },
  { connector: "mysql", namespace: "database", unrelated: "schema" },
  { connector: "clickhouse", namespace: "database", unrelated: "schema" },
])("uses only $connector naming in both field tips and the placeholder before connecting", ({ connector, namespace, unrelated }) => {
  const view = render(<Source connectorKey={connector} />);
  const fields = view.container.querySelectorAll(".table-rule-patterns .form-row");
  expect(fields).toHaveLength(2);
  for (const field of fields) {
    const help = field.querySelector(".help")!;
    expect(help.getAttribute("title")).toContain(`Use ${namespace}.table or ${namespace}.*.`);
    expect(help.getAttribute("title")).not.toContain(`${unrelated}.`);
    expect(help.getAttribute("title")).not.toMatch(/PostgreSQL|MySQL|ClickHouse/);
    expect(help.querySelector('[role="tooltip"]')?.textContent).toBe(help.getAttribute("title"));
    expect(help.getAttribute("title")).toContain("Default: glob / wildcard");
    expect(help.getAttribute("title")).toContain("* matches any number of characters and ? one character");
  }
  const include = view.getByLabelText("Include rule 1") as HTMLInputElement;
  expect(include.placeholder).toBe(`${namespace}.table or ${namespace}.*`);
  expect(include.disabled).toBe(true);
  expect((view.getByLabelText("Exclude rule 1") as HTMLInputElement).placeholder).toBe("Optional pattern");
});

it("updates naming immediately when switching the selected database", () => {
  const view = render(<Source connectorKey="postgres" />);
  view.rerender(<Source connectorKey="clickhouse" />);
  expect((view.getByLabelText("Include rule 1") as HTMLInputElement).placeholder)
    .toBe("database.table or database.*");
  for (const help of view.container.querySelectorAll(".table-rule-patterns .help")) {
    expect(help.getAttribute("title")).toContain("Use database.table or database.*.");
    expect(help.getAttribute("title")).not.toContain("schema.");
  }
});

it("does not guess a database dialect for a standalone table editor", () => {
  const view = render(<TableSelectionEditor value={{ type: "selected", rules: [] }} onChange={() => undefined} />);
  expect((view.getByLabelText("Include rule 1") as HTMLInputElement).placeholder)
    .toBe("namespace.table or namespace.*");
});
