// @vitest-environment jsdom

import { act, cleanup, fireEvent, render } from "@testing-library/preact";
import { afterEach, describe, expect, it, vi } from "vitest";

import {
  ContractView,
  DataSchemaInspector,
  DataSchemaWorkspace,
} from "../src/delivery/EditorViews";
import type { DiscoveryResult } from "../src/types";

afterEach(cleanup);

describe("data schema view", () => {
  it("shows one selected table instead of a scrolling list", () => {
    const dataset = (name: string) => ({
      role: "Main" as const,
      name,
      intermediate_columns: [],
      final_columns: [],
    });
    const view = render(
      <ContractView
        result={{
          source: "source",
          sink: "sink",
          pipeline_count: 1,
          datasets: [dataset("first"), dataset("second")],
          sink_limits: { sink: "sink", supported_arrow_types: [] },
        }}
      />,
    );

    expect(view.container.querySelectorAll(".dataset")).toHaveLength(1);
    expect(view.container.querySelector(".dataset")?.textContent).toContain(
      "first",
    );
    fireEvent.pointerDown(view.getByRole("button", { name: /first/i }), {
      button: 0,
    });
    fireEvent.pointerDown(view.getByRole("option", { name: /second/i }), {
      button: 0,
    });
    expect(view.container.querySelector(".dataset")?.textContent).toContain(
      "second",
    );
  });

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
    const onHide = vi.fn();
    const result: DiscoveryResult = {
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
              destination_type: "UInt64",
              nullable: false,
              primary_key: true,
              low_cardinality: false,
            },
          ],
        },
      ],
      sink_limits: {
        sink: "clickhouse",
        supported_arrow_types: ["signed_integer"],
      },
    };
    const view = render(
      <DataSchemaInspector result={result} onHide={onHide} />,
    );

    expect(
      view.getByRole("table", { name: "Selected table schema" }).textContent,
    ).toContain("Int64");
    fireEvent.click(view.getByRole("tab", { name: "Destination types" }));
    expect(
      view.getByRole("table", { name: "Selected table schema" }).textContent,
    ).toContain("UInt64");
    fireEvent.click(
      view.getByRole("button", { name: "Hide schema inspector" }),
    );
    expect(onHide).toHaveBeenCalledOnce();

    const workspace = render(<DataSchemaWorkspace result={result} />);
    expect(
      workspace.queryByRole("button", { name: "Show schema inspector" }),
    ).toBeNull();
  });

  it("positions the draggable inspector without transforming dropdown coordinates", () => {
    const result: DiscoveryResult = {
      source: "logbroker",
      sink: "clickhouse",
      pipeline_count: 1,
      datasets: [
        {
          role: "Main",
          name: "events",
          intermediate_columns: [],
          final_columns: [],
        },
      ],
      sink_limits: {
        sink: "clickhouse",
        supported_arrow_types: [],
      },
    };
    const view = render(
      <DataSchemaInspector result={result} onHide={() => undefined} />,
    );
    const inspector = view.getByRole("complementary", {
      name: "Schema inspector",
    });

    expect(inspector.style.transform).toBe("");
    expect(inspector.style.left).toBe(`${window.innerWidth - 404}px`);
    expect(inspector.style.top).toBe("24px");
  });

  it("collapses to its header and expands without losing the widget", () => {
    const result: DiscoveryResult = {
      source: "source",
      sink: "unselected",
      pipeline_count: 1,
      datasets: [
        {
          role: "Main",
          name: "events",
          intermediate_columns: [],
          final_columns: [],
        },
      ],
      sink_limits: { sink: "unselected", supported_arrow_types: [] },
    };
    const view = render(
      <DataSchemaInspector result={result} loading onHide={() => undefined} />,
    );

    expect(view.getByRole("status", { name: "Updating schema" })).toBeTruthy();
    fireEvent.click(
      view.getByRole("button", { name: "Collapse schema inspector" }),
    );
    expect(view.queryByRole("table")).toBeNull();
    fireEvent.click(
      view.getByRole("button", { name: "Expand schema inspector" }),
    );
    expect(
      view.getByRole("table", { name: "Selected table schema" }),
    ).toBeTruthy();
  });

  it("highlights only changed rows after a schema refresh", () => {
    vi.useFakeTimers();
    const result: DiscoveryResult = {
      source: "source",
      sink: "unselected",
      pipeline_count: 1,
      datasets: [
        {
          role: "Main",
          name: "events",
          intermediate_columns: [],
          final_columns: [
            {
              name: "id",
              arrow_type: "Utf8",
              destination_type: "Utf8",
              nullable: false,
              primary_key: true,
              low_cardinality: false,
            },
          ],
        },
      ],
      sink_limits: { sink: "unselected", supported_arrow_types: [] },
    };
    const view = render(
      <DataSchemaInspector result={result} onHide={() => undefined} />,
    );
    expect(view.container.querySelector(".schema-row-updated")).toBeNull();

    view.rerender(
      <DataSchemaInspector
        result={{
          ...result,
          datasets: [
            {
              ...result.datasets[0]!,
              final_columns: [
                { ...result.datasets[0]!.final_columns[0]!, nullable: true },
              ],
            },
          ],
        }}
        onHide={() => undefined}
      />,
    );

    expect(view.container.querySelector(".schema-row-updated")).not.toBeNull();
    act(() => {
      vi.advanceTimersByTime(1000);
    });
    expect(view.container.querySelector(".schema-row-updated")).toBeNull();
    vi.useRealTimers();
  });

  it("can be dragged all the way to the viewport origin", () => {
    const result: DiscoveryResult = {
      source: "source",
      sink: "unselected",
      pipeline_count: 1,
      datasets: [],
      sink_limits: { sink: "unselected", supported_arrow_types: [] },
    };
    const view = render(
      <DataSchemaInspector result={result} onHide={() => undefined} />,
    );
    const inspector = view.getByRole("complementary", {
      name: "Schema inspector",
    });
    const handle = inspector.querySelector<HTMLElement>(
      ".schema-inspector-drag-handle",
    )!;
    handle.setPointerCapture = vi.fn();

    fireEvent.pointerDown(handle, {
      pointerId: 1,
      clientX: window.innerWidth - 394,
      clientY: 34,
    });
    fireEvent.pointerMove(handle, { pointerId: 1, clientX: 0, clientY: 0 });

    expect(inspector.style.left).toBe("0px");
    expect(inspector.style.top).toBe("0px");
  });
});
