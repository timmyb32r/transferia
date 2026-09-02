// @vitest-environment jsdom

import { cleanup, fireEvent, render } from "@testing-library/preact";
import { afterEach, describe, expect, it, vi } from "vitest";

import { AppearanceSettings } from "../src/ui/AppearanceSettings";
import {
  catalogCapabilityGroups,
  CompatibilityMatrixLauncher,
  compatibilityRoutes,
} from "../src/ui/CompatibilityMatrixDialog";
import {
  APPEARANCE_STORAGE_KEY,
  applyAppearance,
  loadAppearance,
  saveAppearance,
  type Appearance,
} from "../src/ui/appearance";
import type {
  EndpointDefinition,
  RecordSemantics,
  UiCatalog,
} from "../src/generated/apiContract";

const endpoint = (
  record_semantics: RecordSemantics[],
  delivery_modes: EndpointDefinition["delivery_modes"] = [],
): EndpointDefinition => ({
  schema: {},
  initial: {},
  delivery_modes,
  record_semantics,
  partitioned: false,
  connection_check: false,
  message_preview: false,
});

const CATALOG: UiCatalog = {
  common_schema: {},
  initial: {},
  connectors: [
    {
      key: "postgres",
      title: "PostgreSQL",
      source: endpoint(["append_only", "changelog"], ["batch", "stream"]),
      sink: endpoint(["append_only", "changelog"]),
    },
    {
      key: "kafka",
      title: "Kafka",
      source: endpoint(["append_only"], ["stream"]),
    },
    {
      key: "s3",
      title: "S3",
      sink: endpoint(["append_only"]),
    },
  ],
};

describe("appearance preferences", () => {
  const values = new Map<string, string>();
  const storage = {
    getItem: (key: string) => values.get(key) ?? null,
    setItem: (key: string, value: string) => values.set(key, value),
  };

  afterEach(() => {
    cleanup();
    values.clear();
    delete document.documentElement.dataset.design;
    delete document.documentElement.dataset.theme;
    document.documentElement.style.colorScheme = "";
  });

  it("uses the existing dark classic design by default", () => {
    expect(loadAppearance(storage)).toEqual({
      design: "classic",
      theme: "dark",
    });
  });

  it("rejects corrupt or unsupported persisted values", () => {
    storage.setItem(APPEARANCE_STORAGE_KEY, "not-json");
    expect(loadAppearance(storage)).toEqual({
      design: "classic",
      theme: "dark",
    });

    storage.setItem(
      APPEARANCE_STORAGE_KEY,
      JSON.stringify({ design: "unknown", theme: "light" }),
    );
    expect(loadAppearance(storage)).toEqual({
      design: "classic",
      theme: "dark",
    });
  });

  it("persists and applies both independent dimensions", () => {
    const appearance: Appearance = {
      design: "airy-v0",
      theme: "light",
    };
    saveAppearance(storage, appearance);
    applyAppearance(document.documentElement, appearance);

    expect(loadAppearance(storage)).toEqual(appearance);
    expect(document.documentElement.dataset.design).toBe("airy-v0");
    expect(document.documentElement.dataset.theme).toBe("light");
    expect(document.documentElement.style.colorScheme).toBe("light");
  });

  it("offers every design and theme from the sidebar settings", () => {
    const onChange = vi.fn();
    const view = render(
      <AppearanceSettings
        value={{
          design: "classic",
          theme: "dark",
        }}
        onChange={onChange}
      />,
    );

    fireEvent.click(view.getByRole("button", { name: /Settings/ }));

    expect(view.getByRole("radio", { name: /classic/i })).toBeTruthy();
    expect(view.getByRole("radio", { name: /airy \(adopted\)/ })).toBeTruthy();
    expect(view.getByRole("radio", { name: "Light" })).toBeTruthy();
    expect(view.getByRole("radio", { name: "Dark" })).toBeTruthy();

    fireEvent.click(view.getByRole("radio", { name: /airy \(adopted\)/ }));
    expect(onChange).toHaveBeenCalledWith({
      design: "airy-v0",
      theme: "dark",
    });

    fireEvent.click(view.getByRole("radio", { name: "Light" }));
    expect(onChange).toHaveBeenCalledWith({
      design: "classic",
      theme: "light",
    });
    expect(
      view.queryByRole("checkbox", {
        name: "Automatically open schema widget",
      }),
    ).toBeNull();
  });

  it("derives delivery modes without confusing them with record semantics", () => {
    const routes = compatibilityRoutes(CATALOG);

    expect(routes).toHaveLength(4);
    expect(
      routes.find(
        (route) => route.source.key === "postgres" && route.sink.key === "s3",
      ),
    ).toMatchObject({
      supported: ["batch"],
      unsupported: ["stream"],
      partial: [],
    });
    expect(
      routes.find(
        (route) =>
          route.source.key === "postgres" && route.sink.key === "postgres",
      ),
    ).toMatchObject({
      supported: ["batch", "stream"],
      unsupported: [],
      partial: [],
    });
    expect(
      routes.find(
        (route) => route.source.key === "kafka" && route.sink.key === "s3",
      ),
    ).toMatchObject({
      supported: ["stream"],
      unsupported: [],
      partial: [],
    });
  });

  it("groups catalog components by their declared properties", () => {
    const catalog = structuredClone(CATALOG);
    catalog.connectors[1]!.source!.schema = {
      title: "Debezium parser",
      "x-ui": {
        capabilities: {
          component: "parser",
          key: "debezium",
          record_semantics: ["changelog"],
        },
      },
    };
    catalog.connectors[2]!.source = endpoint(["append_only"], ["batch"]);
    catalog.connectors[2]!.source!.schema = {
      title: "Parquet parser",
      "x-ui": {
        capabilities: {
          component: "parser",
          key: "parquet",
          record_semantics: ["append_only"],
        },
      },
    };
    const groups = catalogCapabilityGroups(catalog);
    const appendOnly = groups.find(
      (group) => group.key === "record_semantics.append_only",
    )!;

    expect([...appendOnly.members.get("source")!].sort()).toEqual([
      "Kafka",
      "PostgreSQL",
      "S3",
    ]);
    expect([...appendOnly.members.get("destination")!]).toEqual(["PostgreSQL", "S3"]);
    expect([...appendOnly.nonMembers.get("parser")!]).toEqual(["Debezium parser"]);
    expect(groups.some((group) => group.key === "record_semantics.changelog")).toBe(false);
    expect(groups.some((group) => group.key === "record_semantics.only_changelog")).toBe(false);
    expect(groups.find((group) => group.key === "delivery_mode.batch")?.members.has("destination")).toBe(false);
    expect(groups.some((group) => group.key === "component.parser")).toBe(false);
    expect(
      [...groups.find((group) => group.key === "component.parser.queue")!.members.get("parser")!],
    ).toEqual(["Debezium parser"]);
    expect(
      [...groups.find((group) => group.key === "component.parser.s3")!.members.get("parser")!],
    ).toEqual(["Parquet parser"]);
  });

  it("opens a stable accessible compatibility dialog and restores focus", () => {
    const catalog = structuredClone(CATALOG);
    catalog.connectors[1]!.source!.schema = {
      title: "JSON parser",
      "x-ui": {
        capabilities: {
          component: "parser",
          key: "json_parser",
          record_semantics: ["append_only"],
        },
      },
    };
    const view = render(
      <CompatibilityMatrixLauncher catalog={catalog} />,
    );
    const launcher = view.getByRole("button", {
      name: "Transfer compatibility",
    });
    launcher.focus();
    fireEvent.mouseDown(launcher);
    fireEvent.click(launcher);

    const dialog = view.getByRole("dialog", {
      name: "Transfer compatibility",
    });
    expect(dialog).toBeTruthy();
    expect(dialog.parentElement?.parentElement).toBe(document.body);
    fireEvent.click(view.getByRole("tab", { name: "Properties" }));
    fireEvent.click(
      view.getByRole("button", { name: "Only append-only records" }),
    );
    expect(view.getAllByText("Only append-only records")).toHaveLength(2);
    expect(view.getByText("Has property")).toBeTruthy();
    expect(view.getByText("Does not have property")).toBeTruthy();
    expect(view.getByText("Parsers — append-only output")).toBeTruthy();
    fireEvent.click(view.getByRole("button", { name: "Kafka / Logbroker parsers" }));
    expect(view.queryByText("Does not have property")).toBeNull();
    fireEvent.click(view.getByRole("tab", { name: "Matrix" }));
    expect(document.activeElement).toBe(
      view.getByRole("button", { name: "Close compatibility matrix" }),
    );
    expect(
      view
        .getByLabelText(
          "PostgreSQL to S3: Batch supported; Stream not supported",
        )
        .classList.contains("partial"),
    ).toBe(true);
    expect(
      view.getByLabelText(
        "PostgreSQL to PostgreSQL: Batch and Stream supported",
      ),
    ).toBeTruthy();

    const intersection = view.getByLabelText(
      "PostgreSQL to S3: Batch supported; Stream not supported",
    );
    fireEvent.mouseEnter(intersection);
    expect(intersection.classList.contains("active-intersection")).toBe(true);
    expect(intersection.closest("tr")?.classList.contains("active-row")).toBe(
      true,
    );
    expect(
      view
        .getByRole("columnheader", { name: "S3" })
        .classList.contains("active-column"),
    ).toBe(true);

    fireEvent.mouseLeave(view.getByRole("table"));
    expect(intersection.classList.contains("active-intersection")).toBe(false);
    fireEvent.keyDown(document, { key: "Escape" });
    expect(view.queryByRole("dialog")).toBeNull();
    expect(document.activeElement).toBe(launcher);
  });
});
