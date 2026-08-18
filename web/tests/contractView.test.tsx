// @vitest-environment jsdom

import { cleanup, render } from "@testing-library/preact";
import { afterEach, describe, expect, it } from "vitest";

import { ContractView } from "../src/delivery/EditorViews";

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
});
