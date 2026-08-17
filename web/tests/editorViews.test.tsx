// @vitest-environment jsdom

import { cleanup, fireEvent, render, waitFor } from "@testing-library/preact";
import { afterEach, describe, expect, it, vi } from "vitest";

import { api } from "../src/api";
import { EndpointCard } from "../src/delivery/EditorViews";
import type { EndpointDefinition, JsonObject } from "../src/types";

const endpoint: EndpointDefinition = {
  schema: {
    type: "object",
    additionalProperties: false,
    properties: {
      host: { type: "string", title: "Host" },
      shard_group: {
        type: "string",
        title: "Shard group",
        "x-ui": { section: "shard_group" },
      },
    },
    required: ["host"],
  },
  initial: { host: "", shard_group: "" },
  delivery_modes: [],
  partitioned: false,
  connection_check: true,
};

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe("endpoint connection check", () => {
  it("shows progress, success, and checked ClickHouse shard groups", async () => {
    let resolve!: (value: { options: Record<string, string[]> }) => void;
    vi.spyOn(api, "checkConnection").mockReturnValue(
      new Promise((done) => {
        resolve = done;
      }),
    );
    const config: JsonObject = {
      sink: { clickhouse: { host: "db.example", shard_group: "" } },
    };
    const view = render(
      <EndpointCard
        title="Destination"
        role="sink"
        selectedKey="clickhouse"
        providers={[{ key: "clickhouse", title: "ClickHouse", sink: endpoint }]}
        endpoint={endpoint}
        config={config}
        readOnly={false}
        showRequiredErrors={false}
        onChoose={() => undefined}
        onConfig={() => undefined}
      />,
    );

    fireEvent.click(view.getByRole("button", { name: "Check connection" }));
    expect(
      (view.getByRole("button", {
        name: "Checking connection…",
      }) as HTMLButtonElement).disabled,
    ).toBe(true);
    resolve({ options: { "#/shard_group": ["default", "analytics"] } });
    await waitFor(() =>
      expect(view.getByText("Connection successful")).toBeTruthy(),
    );

    fireEvent.click(view.container.querySelector("summary")!);
    fireEvent.click(view.container.querySelector("#field---shard_group")!);
    expect(view.getByText("analytics")).toBeTruthy();
    view.unmount();
  });

  it("renders a backend authentication error inline", async () => {
    vi.spyOn(api, "checkConnection").mockRejectedValue(
      new Error("authentication failed"),
    );
    const view = render(
      <EndpointCard
        title="Source"
        role="source"
        selectedKey="postgres"
        providers={[{ key: "postgres", title: "PostgreSQL", source: endpoint }]}
        endpoint={endpoint}
        config={{ source: { postgres: { host: "db.example" } } }}
        readOnly
        showRequiredErrors={false}
        onChoose={() => undefined}
        onConfig={() => undefined}
      />,
    );

    fireEvent.click(view.getByRole("button", { name: "Check connection" }));
    expect((await view.findByRole("alert")).textContent).toContain(
      "authentication failed",
    );
    view.unmount();
  });
});
