// @vitest-environment jsdom

import { cleanup, fireEvent, render, within } from "@testing-library/preact";
import { afterEach, describe, expect, it, vi } from "vitest";

import { AppearanceSettings } from "../src/ui/AppearanceSettings";
import {
  catalogCapabilityGroups,
  CompatibilityMatrixLauncher,
  CompatibilityMatrixDialog,
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
      source: endpoint(
        ["append_only", "changelog"],
        ["batch", "stream", "batch_and_stream"],
      ),
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
  it("puts the source-only generator and sink-only discard last, regardless of catalog order", () => {
    const catalog: UiCatalog = { ...CATALOG, connectors: [
      { key: "discard", title: "Discard (for benchmarks)", sink: endpoint(["append_only"]) },
      { key: "data_generator", title: "Data generator (for benchmarks)", source: endpoint(["append_only"], ["batch"]) },
      ...CATALOG.connectors,
    ] };
    const view = render(<CompatibilityMatrixDialog catalog={catalog} onClose={() => {}} />);
    expect(view.getAllByRole("rowheader").at(-1)?.textContent).toContain("Data generator");
    expect(view.getAllByRole("columnheader").at(-1)?.textContent).toContain("Discard");
    expect(view.queryByRole("rowheader", { name: /Discard/ })).toBeNull();
    expect(view.queryByRole("columnheader", { name: /Data generator/ })).toBeNull();
  });

  it("fits the entire matrix in both dimensions and keeps its footprint stable while searching", () => {
    let resize = () => {};
    const disconnect = vi.fn();
    vi.stubGlobal("ResizeObserver", class {
      constructor(callback: () => void) { resize = callback; }
      observe() {}
      disconnect = disconnect;
    });
    const view = render(<CompatibilityMatrixDialog catalog={CATALOG} onClose={() => {}} />);
    try {
      const viewport = document.querySelector<HTMLElement>(".compatibility-matrix-viewport")!;
      const content = document.querySelector<HTMLElement>(".compatibility-matrix-content")!;
      Object.defineProperties(content, { offsetWidth: { value: 1000 }, offsetHeight: { value: 500 } });
      Object.defineProperties(viewport, { clientWidth: { value: 500, configurable: true }, clientHeight: { value: 500, configurable: true } });
      resize();
      expect(content.style.transform).toBe("scale(0.5)");
      Object.defineProperty(viewport, "clientHeight", { value: 125 });
      resize();
      expect(content.style.transform).toBe("scale(0.25)");
      fireEvent.input(view.getByRole("searchbox"), { target: { value: "post" } });
      expect(document.querySelector(".compatibility-matrix-content")).toBe(content);
      expect(content.style.transform).toBe("scale(0.25)");
      view.unmount();
      expect(disconnect).toHaveBeenCalledOnce();
    } finally {
      view.unmount();
      vi.unstubAllGlobals();
    }
  });

  it("keeps matrix search focus and caret through typing and parent rerenders", () => {
    const firstClose = vi.fn();
    const latestClose = vi.fn();
    const view = render(<CompatibilityMatrixDialog catalog={CATALOG} onClose={firstClose} />);
    const search = view.getByRole("searchbox") as HTMLInputElement;
    search.focus();
    for (const value of ["p", "po", "pos", "post"]) {
      fireEvent.input(search, { target: { value } });
      search.setSelectionRange(1, 1);
      view.rerender(<CompatibilityMatrixDialog catalog={CATALOG} onClose={latestClose} />);
      expect(view.getByRole("searchbox")).toBe(search);
      expect(document.activeElement).toBe(search);
      expect(search.selectionStart).toBe(1);
    }
    fireEvent.keyDown(search, { key: "Escape" });
    expect(firstClose).not.toHaveBeenCalled();
    expect(latestClose).toHaveBeenCalledOnce();
  });

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
      unsupported: ["stream", "batch_and_stream"],
      partial: [],
    });
    expect(
      routes.find(
        (route) =>
          route.source.key === "postgres" && route.sink.key === "postgres",
      ),
    ).toMatchObject({
      supported: ["batch", "stream", "batch_and_stream"],
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
    catalog.connectors[0]!.source!.partitioned = true;
    catalog.connectors[0]!.source!.message_preview = true;
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
    const batchDelivery = groups.find(
      (group) => group.key === "delivery_mode.batch",
    )!;
    expect(batchDelivery.members.has("destination")).toBe(false);
    expect(batchDelivery.nonMembers.has("destination")).toBe(false);
    expect(
      groups
        .find((group) => group.key === "partitioned")
        ?.nonMembers.has("destination"),
    ).toBe(false);
    const messagePreview = groups.find(
      (group) => group.key === "message_preview",
    )!;
    expect(messagePreview.members.has("destination")).toBe(false);
    expect(messagePreview.nonMembers.has("destination")).toBe(false);
    const combined = groups.find(
      (group) => group.key === "delivery_mode.batch_and_stream",
    );
    expect(combined).toMatchObject({ label: "Batch + stream delivery" });
    expect([...combined!.members.get("source")!]).toEqual(["PostgreSQL"]);
    expect(
      [
        ...groups
          .find((group) => group.key === "component.parser")!
          .members.get("parser")!,
      ].sort(),
    ).toEqual(["Debezium parser", "Parquet parser"]);
    expect(
      groups.some((group) => group.key.startsWith("component.parser.")),
    ).toBe(false);
  });

  it("opens a stable accessible compatibility dialog and restores focus", () => {
    const catalog = structuredClone(CATALOG);
    catalog.connectors[2]!.source = endpoint(["append_only"], ["batch"]);
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
      name: "Matrix",
    });
    const previousBodyPadding = document.body.style.paddingRight;
    launcher.focus();
    fireEvent.mouseDown(launcher);
    fireEvent.click(launcher);

    const dialog = view.getByRole("dialog", {
      name: "Matrix",
    });
    expect(dialog).toBeTruthy();
    expect(dialog.parentElement?.parentElement).toBe(document.body);
    expect(document.body.style.overflow).toBe("hidden");
    fireEvent.click(view.getByRole("tab", { name: "Entities" }));
    expect(
      [
        "All sources",
        "All destinations",
        "All parsers",
        "All transformers",
        "All serializers",
      ].map((name) => view.getByRole("region", { name }).tagName),
    ).toEqual([
      "SECTION",
      "SECTION",
      "SECTION",
      "SECTION",
      "SECTION",
    ]);
    expect(
      within(view.getByRole("region", { name: "All parsers" })).getByText(
        "JSON parser",
      ),
    ).toBeTruthy();
    expect(view.queryByText("Has property")).toBeNull();

    fireEvent.click(view.getByRole("tab", { name: "Properties" }));
    const deliveryType = view.getByRole("region", { name: "Delivery type" });
    expect(
      within(deliveryType)
        .getAllByRole("button")
        .map((button) => button.textContent),
    ).toEqual([
      "Batch + stream delivery",
      "Batch delivery",
      "Stream delivery",
    ]);
    fireEvent.click(
      view.getByRole("button", { name: "Only append-only records" }),
    );
    expect(view.getAllByText("Only append-only records")).toHaveLength(1);
    const membership = view.getByRole("region", { name: "Property membership" });
    expect(
      view
        .getByRole("navigation", { name: "Properties" })
        .classList.contains("always-visible-scrollbar"),
    ).toBe(true);
    expect(
      within(membership)
        .getByRole("list", { name: "Destinations with property" })
        .textContent,
    ).toBe("S3");
    expect(
      within(membership)
        .getByRole("list", { name: "Parsers with property" })
        .textContent,
    ).toBe("JSON parser");
    expect(within(membership).getByText("Does not have property")).toBeTruthy();
    fireEvent.click(
      view.getByRole("button", { name: "Batch + stream delivery" }),
    );
    expect(
      within(membership)
        .getByRole("list", { name: "Parsers without property" })
        .textContent,
    ).toBe("None");
    expect(
      within(membership)
        .getByRole("list", { name: "Sources without property" })
        .textContent,
    ).toContain("S3");
    fireEvent.click(view.getByRole("tab", { name: "Matrix" }));
    expect(document.activeElement).toBe(
      view.getByRole("button", { name: "Close compatibility matrix" }),
    );
    expect(
      view
        .getByLabelText(
          "PostgreSQL to S3: Batch supported; Stream and Batch + stream not supported",
        )
        .classList.contains("partial"),
    ).toBe(true);
    expect(
      view.getByLabelText(
        "PostgreSQL to PostgreSQL: Batch and Stream and Batch + stream supported",
      ),
    ).toBeTruthy();
    expect(view.queryByLabelText("Legend")).toBeNull();
    expect(view.queryByRole("columnheader", { name: "Incomplete modes" })).toBeNull();
    expect(view.getByRole("complementary", { name: "Incomplete modes" })).toBeTruthy();
    const gaps = view.getByRole("complementary", { name: "Incomplete modes" });
    const sourceRows = view.getAllByRole("rowheader");
    expect(gaps.querySelectorAll(".compatibility-mode-gaps")).toHaveLength(sourceRows.length);
    expect(gaps.querySelectorAll(".compatibility-gap-check").length).toBeGreaterThan(0);
    expect(gaps.querySelector("strong")).toBeNull();
    expect(within(gaps).getByLabelText("PostgreSQL: incomplete modes").textContent).toContain("✓");
    expect(within(gaps).getByLabelText("Kafka: incomplete modes").querySelector(".incomplete")?.textContent).toContain("×");
    expect(
      within(view.getByLabelText("PostgreSQL to PostgreSQL: Batch and Stream and Batch + stream supported"))
        .getAllByText(/^(B|S|B\+S)$/)
        .map((badge) => badge.textContent),
    ).toEqual(["B+S"]);

    const postgresRowHeader = view.getByRole("rowheader", {
      name: /PostgreSQL/,
    });
    const postgresRowButton = within(postgresRowHeader).getByRole("button");
    fireEvent.click(postgresRowButton);
    expect(postgresRowButton.getAttribute("aria-pressed")).toBe("true");
    const s3ColumnHeader = view.getByRole("columnheader", { name: "S3" });
    const s3ColumnButton = within(s3ColumnHeader).getByRole("button");
    fireEvent.click(s3ColumnButton);
    expect(s3ColumnButton.getAttribute("aria-pressed")).toBe("true");
    fireEvent.input(view.getByRole("searchbox", { name: "Find source or destination" }), {
      target: { value: "s3" },
    });
    expect(s3ColumnHeader.classList.contains("search-match-column")).toBe(true);
    expect(
      view
        .getByRole("rowheader", { name: /S3/ })
        .closest("tr")
        ?.classList.contains("search-match-row"),
    ).toBe(true);
    expect(postgresRowButton.getAttribute("aria-pressed")).toBe("true");
    expect(s3ColumnButton.getAttribute("aria-pressed")).toBe("true");
    fireEvent.click(postgresRowButton);
    fireEvent.click(s3ColumnButton);
    expect(postgresRowButton.getAttribute("aria-pressed")).toBe("false");
    expect(s3ColumnButton.getAttribute("aria-pressed")).toBe("false");
    expect(view.getByRole("tooltip", { name: "Snapshot is not implemented" })).toBeTruthy();
    expect(view.getByRole("tooltip", { name: "Replication is not implemented" })).toBeTruthy();

    const intersection = view.getByLabelText(
      "PostgreSQL to S3: Batch supported; Stream and Batch + stream not supported",
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
    expect(document.body.style.overflow).toBe("");
    expect(document.body.style.paddingRight).toBe(previousBodyPadding);
    expect(document.activeElement).toBe(launcher);
  });
});
