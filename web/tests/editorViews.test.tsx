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
      password: {
        type: "string",
        title: "Password",
        "x-ui": { widget: "password" },
      },
      shard_group: {
        type: "string",
        title: "Shard group",
        "x-ui": { section: "shard_group" },
      },
      timeout_ms: {
        type: "integer",
        title: "Timeout",
        "x-ui": { section: "advanced" },
      },
    },
    required: ["host"],
  },
  initial: { host: "", password: "", shard_group: "", timeout_ms: 1000 },
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
      sink: {
        clickhouse: {
          host: "db.example",
          password: "secret",
          shard_group: "",
          timeout_ms: 1000,
        },
      },
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
      (
        view.getByRole("button", {
          name: "Checking connection…",
        }) as HTMLButtonElement
      ).disabled,
    ).toBe(true);
    resolve({ options: { "#/shard_group": ["default", "analytics"] } });
    await waitFor(() =>
      expect(view.getByText("Connection successful")).toBeTruthy(),
    );

    const password = view.container.querySelector("#field---password")!;
    const connectionCheck = view.container.querySelector(".connection-check")!;
    const summaries = [...view.container.querySelectorAll("summary")];
    const shardGroup = summaries.find(
      (summary) => summary.textContent === "Shard group",
    )!;
    const advanced = summaries.find(
      (summary) => summary.textContent === "Advanced settings",
    )!;
    expect(password.compareDocumentPosition(connectionCheck)).toBe(
      Node.DOCUMENT_POSITION_FOLLOWING,
    );
    expect(connectionCheck.compareDocumentPosition(shardGroup)).toBe(
      Node.DOCUMENT_POSITION_FOLLOWING,
    );
    expect(shardGroup.compareDocumentPosition(advanced)).toBe(
      Node.DOCUMENT_POSITION_FOLLOWING,
    );

    fireEvent.click(shardGroup);
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
