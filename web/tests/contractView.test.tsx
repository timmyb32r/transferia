// @vitest-environment jsdom

import { cleanup, fireEvent, render } from "@testing-library/preact";
import { afterEach, describe, expect, it } from "vitest";

import {
  ContractView,
  DataSchemaWorkspace,
} from "../src/delivery/EditorViews";

afterEach(cleanup);

describe("data schema view", () => {
  it("shows intermediate Arrow and final destination types separately", () => {
    const view = render(
      <ContractView
        result={{
          source: "logbroker",
          sink: "clickhouse",
          pipeline_count: 1,
          datasets: [
            {
              role: "Main",
              name: "events",
              intermediate_columns: [
                {
                  name: "name",
                  arrow_type: "Utf8",
                  nullable: true,
                  primary_key: false,
                  low_cardinality: true,
                },
              ],
              final_columns: [
                {
                  name: "name",
                  arrow_type: "Utf8",
                  destination_type: "Nullable(LowCardinality(String))",
                  nullable: true,
                  primary_key: false,
                  low_cardinality: true,
                },
              ],
            },
          ],
          sink_limits: {
            sink: "clickhouse",
            supported_arrow_types: ["utf8"],
          },
        }}
      />,
    );

    expect(
      view.getByRole("table", { name: "Intermediate schema" }).textContent,
    ).toContain("Utf8");
    expect(
      view.getByRole("table", { name: "Final · clickhouse schema" })
        .textContent,
    ).toContain("Nullable(LowCardinality(String))");
    expect(view.container.textContent).not.toContain("DISCOVERED CONTRACT");
  });

  it("offers a searchable, hideable final-schema inspector", () => {
    const view = render(
      <DataSchemaWorkspace
        result={{
          source: "logbroker",
          sink: "clickhouse",
          pipeline_count: 1,
          datasets: [
            {
              role: "Main",
              name: "events",
              intermediate_columns: [],
              final_columns: [
                {
                  name: "id",
                  arrow_type: "Int64",
                  destination_type: "Int64",
                  nullable: false,
                  primary_key: true,
                  low_cardinality: false,
                },
              ],
            },
          ],
          sink_limits: { sink: "clickhouse", supported_arrow_types: ["signed_integer"] },
        }}
      />,
    );

    expect(view.getByRole("table", { name: "Selected table schema" }).textContent)
      .toContain("Int64");
    fireEvent.click(view.getByRole("button", { name: "Hide schema inspector" }));
    expect(view.queryByLabelText("Schema inspector")).toBeNull();
    fireEvent.click(view.getByRole("button", { name: "Show schema inspector" }));
    expect(view.getByLabelText("Schema inspector")).not.toBeNull();
  });
});
