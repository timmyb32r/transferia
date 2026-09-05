// @vitest-environment jsdom

import {
  act,
  cleanup,
  fireEvent,
  render,
  waitFor,
  within,
} from "@testing-library/preact";
import { afterEach, describe, expect, it, vi } from "vitest";

import { httpControlPlane as api } from "../src/infrastructure/controlPlane/httpControlPlane";
import { App } from "../src/app";
import type {
  DeliveryRecord,
  DeliverySummary,
  DiscoveryResult,
  UiCatalog,
} from "../src/types";

const CATALOG: UiCatalog = {
  common_schema: {
    type: "object",
    properties: {
      delivery_type: {
        type: "string",
        enum: ["batch", "stream", "batch_and_stream"],
      },
    },
  },
  initial: {},
  connectors: [],
};

describe("App request orchestration", () => {
  afterEach(() => {
    cleanup();
    vi.restoreAllMocks();
    vi.useRealTimers();
  });

  it("boots through an injected control-plane port without global transport mocks", async () => {
    const catalog = vi.fn().mockResolvedValue(CATALOG);
    const deliveries = vi.fn().mockResolvedValue([]);
    const view = render(<App controlPlane={{ ...api, catalog, deliveries }} />);

    await within(view.container as HTMLElement).findByRole("heading", {
      name: "Untitled delivery",
    });
    expect(catalog).toHaveBeenCalledOnce();
    expect(deliveries).toHaveBeenCalledOnce();
  });

  it("shows the server-assigned transfer id in a stable header slot after first save", async () => {
    installApiMocks([]);
    const writeText = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(navigator, "clipboard", {
      configurable: true,
      value: { writeText },
    });
    const transferId = "dttabcdefghijklmnopq";
    vi.mocked(api.create).mockResolvedValue(delivery(transferId, "Saved"));
    const view = render(<App />);
    const app = within(view.container as HTMLElement);
    await app.findByRole("heading", { name: "Untitled delivery" });
    const slot = view.container.querySelector(".transfer-id-slot");
    expect(slot?.textContent).toBe("TRANSFER ID · assigned on save");

    fireEvent.input(app.getByLabelText("Delivery name"), {
      target: { value: "Saved" },
    });
    fireEvent.click(app.getByRole("button", { name: "Save" }));

    await waitFor(() =>
      expect(slot?.textContent).toBe(`TRANSFER ID · ${transferId}`),
    );
    fireEvent.click(app.getByRole("button", { name: "Copy transfer ID" }));
    await waitFor(() => expect(writeText).toHaveBeenCalledWith(transferId));
    expect(app.getByRole("button", { name: "Transfer ID copied" })).toBeTruthy();
    expect(view.container.querySelector(".transfer-id-slot")).toBe(slot);
    expect(api.create).toHaveBeenCalledWith(
      "Saved",
      "",
      expect.not.objectContaining({ delivery_id: expect.anything() }),
    );
  });

  it("lists delivery types as batch, stream, then batch and stream", async () => {
    installApiMocks([]);
    const view = render(<App />);
    const app = within(view.container as HTMLElement);
    await app.findByRole("heading", { name: "Untitled delivery" });
    const deliveryType = within(
      app.getByText("Delivery type").closest("label")!,
    );

    fireEvent.pointerDown(
      deliveryType.getByRole("button", { name: "Delivery type" }),
      { button: 0 },
    );

    expect(
      deliveryType.getAllByRole("option").map((option) => option.textContent),
    ).toEqual(["Not selected", "Batch", "Stream", "Batch + stream"]);
  });

  it("keeps all route choices visible and explains incompatible selections", async () => {
    installApiMocks([]);
    vi.mocked(api.catalog).mockResolvedValue({
      ...CATALOG,
      connectors: [
        connector("batch-source", "Batch source", {
          source: endpoint(["batch"], ["append_only"]),
        }),
        connector("stream-source", "Stream source", {
          source: endpoint(["stream"], ["changelog"]),
        }),
        connector("hybrid-source", "Hybrid source", {
          source: endpoint(["batch", "stream"], ["append_only", "changelog"]),
        }),
        connector("append-sink", "Append sink", {
          sink: endpoint([], ["append_only"]),
        }),
        connector("change-sink", "Change sink", {
          sink: endpoint([], ["changelog"]),
        }),
        connector("both-sink", "Both sink", {
          sink: endpoint([], ["append_only", "changelog"]),
        }),
      ],
    });
    const view = render(<App />);
    const app = within(view.container as HTMLElement);
    await app.findByRole("heading", { name: "Untitled delivery" });

    expect(app.getByRole("heading", { name: "Source" })).toBeTruthy();
    expect(app.getByRole("heading", { name: "Destination" })).toBeTruthy();

    chooseFromSelect(app, "Delivery type", "Stream");
    expect(selectOptions(app, "Source")).toEqual([
      "Not selected",
      "Batch source",
      "Hybrid source",
      "Stream source",
    ]);

    chooseFromSelect(app, "Source", "Stream source");
    expect(selectOptions(app, "Destination")).toEqual([
      "Not selected",
      "Append sink",
      "Both sink",
      "Change sink",
    ]);

    chooseFromSelect(app, "Destination", "Change sink");
    expect(selectOptions(app, "Delivery type")).toEqual([
      "Not selected",
      "Batch",
      "Stream",
      "Batch + stream",
    ]);
    chooseFromSelect(app, "Delivery type", "Batch");
    const compatibilityError = app
      .getByText("Incompatible route")
      .closest<HTMLElement>(".compatibility-error")!;
    expect(
      within(compatibilityError).getByText(
        "Stream source does not support 'batch' delivery.",
      ),
    ).toBeTruthy();
  });

  it("keeps every delivery type visible for a conditional source", async () => {
    installApiMocks([]);
    vi.mocked(api.catalog).mockResolvedValue({
      ...CATALOG,
      connectors: [
        connector("mysql", "MySQL", {
          source: conditionalReplicationSource(),
        }),
      ],
    });
    const view = render(<App />);
    const app = within(view.container as HTMLElement);
    await app.findByRole("heading", { name: "Untitled delivery" });

    chooseFromSelect(app, "Source", "MySQL");

    expect(selectOptions(app, "Delivery type")).toEqual([
      "Not selected",
      "Batch",
      "Stream",
      "Batch + stream",
    ]);
  });

  it("keeps every delivery type visible for an active replication configuration", async () => {
    const existing = {
      ...delivery("replication", "MySQL replication"),
      config: {
        delivery_id: "replication",
        delivery_type: "stream",
        source: { mysql: { replication: { server_id: 42 } } },
        sink: {},
      },
    } satisfies DeliveryRecord;
    installApiMocks([existing]);
    vi.mocked(api.catalog).mockResolvedValue({
      ...CATALOG,
      connectors: [
        connector("mysql", "MySQL", {
          source: conditionalReplicationSource(),
        }),
      ],
    });
    vi.mocked(api.delivery).mockResolvedValue(existing);
    const view = render(<App />);
    const app = within(view.container as HTMLElement);
    await app.findByText("MySQL replication");
    fireEvent.click(app.getByText("MySQL replication").closest("button")!);
    await app.findByRole("heading", { name: "MySQL replication" });
    fireEvent.click(app.getByRole("button", { name: "Edit" }));

    expect(selectOptions(app, "Delivery type")).toEqual([
      "Not selected",
      "Batch",
      "Stream",
      "Batch + stream",
    ]);
  });

  it("guides the user through one missing required field at a time", async () => {
    installApiMocks([]);
    const view = render(<App />);
    const app = within(view.container as HTMLElement);
    await app.findByRole("heading", { name: "Untitled delivery" });

    const name = app.getByLabelText("Delivery name").closest("label")!;
    const deliveryType = app.getByText("Delivery type").closest("label")!;
    await waitFor(() =>
      expect(name.classList.contains("required-next")).toBe(true),
    );
    expect(deliveryType.classList.contains("required-next")).toBe(false);

    const nameInput = app.getByLabelText("Delivery name");
    nameInput.focus();
    fireEvent.input(nameInput, {
      target: { value: "guided-delivery" },
    });

    await waitFor(() =>
      expect(name.classList.contains("required-next")).toBe(true),
    );
    expect(deliveryType.classList.contains("required-next")).toBe(false);

    nameInput.blur();

    await waitFor(() =>
      expect(deliveryType.classList.contains("required-next")).toBe(true),
    );
    expect(name.classList.contains("required-next")).toBe(false);

    const deliveryTypeTrigger = within(deliveryType).getByRole("button", {
      name: "Delivery type",
    });
    chooseFromSelect(app, "Delivery type", "Batch");

    await waitFor(() =>
      expect(
        app
          .getByRole("heading", { name: "Source" })
          .closest(".endpoint-card")
          ?.querySelector(".required-next"),
      ).not.toBeNull(),
    );
    expect(document.activeElement).not.toBe(deliveryTypeTrigger);
  });

  it("highlights missing required fields when inactive Activate is clicked", async () => {
    installApiMocks([]);
    const view = render(<App />);
    const app = within(view.container as HTMLElement);
    await app.findByRole("heading", { name: "Untitled delivery" });

    fireEvent.click(app.getByRole("button", { name: "Activate" }));

    expect(
      app
        .getByLabelText("Delivery name")
        .closest("label")
        ?.classList.contains("required-missing"),
    ).toBe(true);
    expect(
      app
        .getByText("Delivery type")
        .closest("label")
        ?.classList.contains("required-missing"),
    ).toBe(true);
    expect(app.getByRole("heading", { name: "Source" })).toBeTruthy();
    expect(app.getByRole("heading", { name: "Destination" })).toBeTruthy();
    expect(api.activate).not.toHaveBeenCalled();
  });

  it("keeps Validate clickable and highlights missing required fields", async () => {
    installApiMocks([]);
    const view = render(<App />);
    const app = within(view.container as HTMLElement);
    await app.findByRole("heading", { name: "Untitled delivery" });

    const validate = app.getByRole("button", { name: "Validate" });
    expect((validate as HTMLButtonElement).disabled).toBe(false);
    fireEvent.click(validate);

    expect(
      app
        .getByLabelText("Delivery name")
        .closest("label")
        ?.classList.contains("required-missing"),
    ).toBe(true);
    expect(
      app
        .getByText("Delivery type")
        .closest("label")
        ?.classList.contains("required-missing"),
    ).toBe(true);
    expect(api.validate).not.toHaveBeenCalled();
  });

  it("highlights incomplete required fields in both endpoints and scrolls to the first", async () => {
    installApiMocks([]);
    vi.mocked(api.catalog).mockResolvedValue({
      ...CATALOG,
      connectors: [
        {
          key: "source",
          title: "Test source",
          source: {
            schema: {
              type: "object",
              properties: { host: { type: "string", title: "Host" } },
              required: ["host"],
            },
            initial: { host: "" },
            delivery_modes: ["batch"],
            record_semantics: ["append_only"],
            partitioned: false,
            connection_check: false,
            message_preview: false,
          },
        },
        {
          key: "sink",
          title: "Test destination",
          sink: {
            schema: {
              type: "object",
              properties: {
                database: { type: "string", title: "Database" },
              },
              required: ["database"],
            },
            initial: { database: "" },
            delivery_modes: ["batch"],
            record_semantics: ["append_only"],
            partitioned: false,
            connection_check: false,
            message_preview: false,
          },
        },
      ],
    });
    const scrollIntoView = vi.fn();
    Object.defineProperty(HTMLElement.prototype, "scrollIntoView", {
      configurable: true,
      value: scrollIntoView,
    });
    const view = render(<App />);
    const app = within(view.container as HTMLElement);
    await app.findByRole("heading", { name: "Untitled delivery" });
    fireEvent.input(app.getByLabelText("Delivery name"), {
      target: { value: "Incomplete source" },
    });
    chooseFromSelect(app, "Delivery type", "Batch");
    chooseFromSelect(app, "Source", "Test source");
    expect(app.queryByText("Host")).toBeNull();
    expect(app.queryByText("Pipeline settings")).toBeNull();
    chooseFromSelect(app, "Destination", "Test destination");
    expect(app.getByText("Host")).toBeTruthy();
    expect(app.getByText("Database")).toBeTruthy();
    expect(app.getByText("Pipeline settings")).toBeTruthy();
    chooseFromSelect(app, "Delivery type", "Stream");
    expect(app.getByText("Incompatible route")).toBeTruthy();
    expect(app.queryByText("Host")).toBeNull();
    expect(app.queryByText("Database")).toBeNull();
    expect(app.queryByText("Pipeline settings")).toBeNull();
    expect(app.getByRole("heading", { name: "Source" })).toBeTruthy();
    expect(app.getByRole("heading", { name: "Destination" })).toBeTruthy();
    chooseFromSelect(app, "Delivery type", "Batch");
    expect(app.queryByText("Incompatible route")).toBeNull();
    expect(app.getByText("Host")).toBeTruthy();
    expect(app.getByText("Database")).toBeTruthy();
    scrollIntoView.mockClear();

    fireEvent.click(app.getByRole("tab", { name: "Data schema" }));

    await waitFor(() =>
      expect(
        app
          .getByText("Host")
          .closest(".form-row")
          ?.classList.contains("required-missing"),
      ).toBe(true),
    );
    expect(
      app
        .getByText("Database")
        .closest(".form-row")
        ?.classList.contains("required-missing"),
    ).toBe(false);

    fireEvent.click(app.getByRole("button", { name: "Validate" }));

    await waitFor(() =>
      expect(
        app
          .getByText("Host")
          .closest(".form-row")
          ?.classList.contains("required-missing"),
      ).toBe(true),
    );
    await waitFor(() =>
      expect(
        app
          .getByText("Host")
          .closest(".form-row")
          ?.classList.contains("required-error"),
      ).toBe(true),
    );
    expect(
      app.getByLabelText("Host").classList.contains("required-error-control"),
    ).toBe(true);
    expect(
      app
        .getByText("Database")
        .closest(".form-row")
        ?.classList.contains("required-missing"),
    ).toBe(true);
    await waitFor(() => expect(scrollIntoView).toHaveBeenCalled());
    await waitFor(() =>
      expect((document.activeElement as HTMLElement | null)?.id).toBe(
        "field---host",
      ),
    );
    expect(api.validate).not.toHaveBeenCalled();
  });

  it("sends an unrenderable configuration issue to backend validation", async () => {
    const existing = {
      ...delivery("existing", "Existing"),
      config: {
        delivery_id: "existing",
        delivery_type: "batch",
        source: { source: { unknown_option: true } },
        sink: { sink: {} },
      },
    } satisfies DeliveryRecord;
    installApiMocks([existing]);
    vi.mocked(api.catalog).mockResolvedValue({
      ...CATALOG,
      connectors: [
        {
          key: "source",
          title: "Test source",
          source: {
            schema: {
              type: "object",
              properties: {},
              additionalProperties: false,
            },
            initial: {},
            delivery_modes: ["batch"],
            record_semantics: ["append_only"],
            partitioned: false,
            connection_check: false,
            message_preview: false,
          },
        },
        {
          key: "sink",
          title: "Test destination",
          sink: {
            schema: {
              type: "object",
              properties: {},
              additionalProperties: false,
            },
            initial: {},
            delivery_modes: ["batch"],
            record_semantics: ["append_only"],
            partitioned: false,
            connection_check: false,
            message_preview: false,
          },
        },
      ],
    });
    vi.mocked(api.delivery).mockResolvedValue(existing);
    const view = render(<App />);
    const app = within(view.container as HTMLElement);
    await app.findByText("Existing");
    fireEvent.click(app.getByText("Existing").closest("button")!);
    await app.findByRole("heading", { name: "Existing" });
    fireEvent.click(app.getByRole("button", { name: "Edit" }));

    fireEvent.click(app.getByRole("button", { name: "Validate" }));

    await waitFor(() => expect(api.validate).toHaveBeenCalledOnce());
  });

  it("rejects a catalog whose initial value has an incomplete hidden subtree", async () => {
    const existing = {
      ...delivery("existing", "Existing"),
      config: {
        delivery_id: "existing",
        delivery_type: "batch",
        source: {
          source: {
            connection: "https://registry.example",
            projection: { columns: [] },
          },
        },
        sink: { sink: {} },
      },
    } satisfies DeliveryRecord;
    installApiMocks([existing]);
    vi.mocked(api.catalog).mockResolvedValue({
      ...CATALOG,
      connectors: [
        {
          key: "source",
          title: "Test source",
          source: {
            schema: {
              type: "object",
              properties: {
                connection: { type: "string", title: "Registry URL" },
                projection: {
                  type: "object",
                  "x-ui": { widget: "hidden" },
                  properties: {
                    columns: {
                      type: "array",
                      minItems: 1,
                      items: { type: "string" },
                    },
                  },
                  required: ["columns"],
                },
              },
              required: ["connection", "projection"],
            },
            initial: {
              connection: "https://registry.example",
              projection: { columns: [] },
            },
            delivery_modes: ["batch"],
            record_semantics: ["append_only"],
            partitioned: false,
            connection_check: false,
            message_preview: false,
          },
        },
        {
          key: "sink",
          title: "Test destination",
          sink: {
            schema: { type: "object", properties: {} },
            initial: {},
            delivery_modes: ["batch"],
            record_semantics: ["append_only"],
            partitioned: false,
            connection_check: false,
            message_preview: false,
          },
        },
      ],
    });
    vi.mocked(api.delivery).mockResolvedValue(existing);
    const view = render(<App />);
    const app = within(view.container as HTMLElement);
    expect(
      await app.findByText(
        "source source initial hidden field #/projection/columns is incomplete",
      ),
    ).toBeTruthy();
    expect(api.validate).not.toHaveBeenCalled();
  });

  it("does not let a save response overwrite a delivery opened meanwhile", async () => {
    const existing = delivery("existing", "Existing");
    installApiMocks([existing]);
    vi.mocked(api.delivery).mockResolvedValue(existing);
    const save = deferred<DeliveryRecord>();
    vi.mocked(api.create).mockImplementation(() => save.promise);
    const view = render(<App />);
    const app = within(view.container as HTMLElement);
    await app.findByRole("heading", { name: "Untitled delivery" });

    fireEvent.input(app.getByLabelText("Delivery name"), {
      target: { value: "Unsaved" },
    });
    fireEvent.click(app.getByRole("button", { name: "Save" }));
    await waitFor(() => expect(api.create).toHaveBeenCalledOnce());
    fireEvent.click(app.getByText("Existing").closest("button")!);
    await app.findByRole("heading", { name: "Existing" });

    await act(async () => save.resolve(delivery("created", "Unsaved")));

    expect(app.getByRole("heading", { name: "Existing" })).toBeTruthy();
    expect(app.queryByRole("heading", { name: "Unsaved" })).toBeNull();
  });

  it("publishes only the latest of two navigation requests", async () => {
    const first = delivery("first", "First");
    const second = delivery("second", "Second");
    installApiMocks([first, second]);
    const firstRequest = deferred<DeliveryRecord>();
    vi.mocked(api.delivery).mockImplementation((id) =>
      id === first.id ? firstRequest.promise : Promise.resolve(second),
    );
    const view = render(<App />);
    const app = within(view.container as HTMLElement);
    await app.findByText("First");

    fireEvent.click(app.getByText("First").closest("button")!);
    fireEvent.click(app.getByText("Second").closest("button")!);
    await app.findByRole("heading", { name: "Second" });
    await act(async () => firstRequest.resolve(first));

    expect(app.getByRole("heading", { name: "Second" })).toBeTruthy();
  });

  it("opens existing deliveries read-only until Edit is clicked", async () => {
    const existing = delivery("existing", "Existing");
    installApiMocks([existing]);
    vi.mocked(api.delivery).mockResolvedValue(existing);
    const view = render(<App />);
    const app = within(view.container as HTMLElement);
    await app.findByText("Existing");

    fireEvent.click(app.getByText("Existing").closest("button")!);
    await app.findByRole("heading", { name: "Existing" });

    const name = app.getByLabelText("Delivery name") as HTMLInputElement;
    expect(name.disabled).toBe(true);
    expect(app.queryByRole("button", { name: "Save" })).toBeNull();

    fireEvent.click(app.getByRole("button", { name: "Edit" }));

    expect(name.disabled).toBe(false);
    expect(app.getByRole("button", { name: "Save" })).toBeTruthy();
  });

  it("clones a saved delivery under the next name with independent state", async () => {
    const existing = {
      ...delivery("dttoriginal", "orders9"),
      description: "cloned description",
      config: {
        delivery_id: "dttoriginal",
        durable_storage: {
          type: "local_file",
          path: ".transferia-server/workers/original/state",
        },
        delivery_type: "batch",
        source: {},
        sink: {},
      },
    } satisfies DeliveryRecord;
    installApiMocks([existing]);
    vi.mocked(api.delivery).mockResolvedValue(existing);
    const view = render(<App />);
    const app = within(view.container as HTMLElement);
    await app.findByText("orders9");

    fireEvent.click(app.getByText("orders9").closest("button")!);
    await app.findByRole("heading", { name: "orders9" });
    fireEvent.click(app.getByRole("button", { name: "Clone" }));

    expect(app.getByRole("heading", { name: "orders10" })).toBeTruthy();
    expect(
      (app.getByLabelText("Delivery name") as HTMLInputElement).value,
    ).toBe("orders10");
    expect(
      (app.getByLabelText("Description", { exact: false }) as HTMLInputElement)
        .value,
    ).toBe("cloned description");
    expect(view.container.querySelector(".transfer-id-slot")?.textContent).toBe(
      "TRANSFER ID · assigned on save",
    );

    vi.mocked(api.create).mockResolvedValue(delivery("dttclone", "orders10"));
    fireEvent.click(app.getByRole("button", { name: "Save" }));
    await waitFor(() => expect(api.create).toHaveBeenCalledOnce());
    const cloned = vi.mocked(api.create).mock.calls[0]![2];
    expect(cloned.delivery_id).toBeUndefined();
    expect(cloned.durable_storage).not.toEqual(existing.config.durable_storage);
    expect(cloned.delivery_type).toBe("batch");
  });

  it("does not auto-open the data widget when a new delivery becomes discoverable", async () => {
    installApiMocks([]);
    vi.mocked(api.catalog).mockResolvedValue({
      ...CATALOG,
      connectors: [
        {
          key: "source",
          title: "Test source",
          source: {
            schema: { type: "object", properties: {} },
            initial: {},
            delivery_modes: ["batch"],
            record_semantics: ["append_only"],
            partitioned: false,
            connection_check: false,
            message_preview: false,
          },
        },
        {
          key: "sink",
          title: "Test sink",
          sink: {
            schema: { type: "object", properties: {} },
            initial: {},
            delivery_modes: ["batch"],
            record_semantics: ["append_only"],
            partitioned: false,
            connection_check: false,
            message_preview: false,
          },
        },
      ],
    });
    vi.mocked(api.discover).mockResolvedValue({
      ...discovery(),
      datasets: [
        {
          role: "Main",
          name: "events",
          intermediate_columns: [],
          final_columns: [],
        },
      ],
    });
    const view = render(<App />);
    const app = within(view.container as HTMLElement);
    await app.findByRole("heading", { name: "Untitled delivery" });
    chooseFromSelect(app, "Delivery type", "Batch");
    chooseFromSelect(app, "Source", "Test source");
    chooseFromSelect(app, "Destination", "Test sink");
    await waitFor(() => expect(api.discover).toHaveBeenCalled());

    expect(app.queryByRole("dialog", { name: "Final schema" })).toBeNull();
    expect(
      app
        .getByRole("button", { name: "Data widget" })
        .classList.contains("data-widget-ready"),
    ).toBe(true);
  });

  it("ignores an action response after navigating to another delivery", async () => {
    const first = delivery("first", "First", true);
    const second = delivery("second", "Second");
    installApiMocks([first, second]);
    vi.mocked(api.delivery).mockImplementation((id) =>
      Promise.resolve(id === first.id ? first : second),
    );
    const activation = deferred<DeliveryRecord>();
    vi.mocked(api.activate).mockImplementation(() => activation.promise);
    const view = render(<App />);
    const app = within(view.container as HTMLElement);
    await app.findByText("First");
    fireEvent.click(app.getByText("First").closest("button")!);
    await app.findByRole("heading", { name: "First" });

    fireEvent.click(app.getByRole("button", { name: "Activate" }));
    await waitFor(() => expect(api.activate).toHaveBeenCalledOnce());
    fireEvent.click(app.getByText("Second").closest("button")!);
    await app.findByRole("heading", { name: "Second" });
    await act(async () =>
      activation.resolve({
        ...first,
        runtime: { state: "running", run_id: "run-1", pid: 42 },
      }),
    );

    expect(app.getByRole("heading", { name: "Second" })).toBeTruthy();
    expect(app.queryByText("running")).toBeNull();
  });

  it("ignores an old delivery poll after navigation", async () => {
    vi.useFakeTimers();
    const first = delivery("first", "First");
    const second = delivery("second", "Second");
    installApiMocks([first, second]);
    vi.mocked(api.delivery).mockResolvedValue(first);
    const view = render(<App />);
    const app = within(view.container as HTMLElement);
    await flushEffects();
    fireEvent.click(app.getByText("First").closest("button")!);
    await flushEffects();
    expect(app.getByRole("heading", { name: "First" })).toBeTruthy();

    const poll = deferred<DeliveryRecord>();
    vi.mocked(api.delivery).mockImplementation((id) =>
      id === first.id ? poll.promise : Promise.resolve(second),
    );
    await act(async () => {
      await vi.advanceTimersByTimeAsync(2000);
    });
    expect(api.delivery).toHaveBeenCalledWith(first.id);
    fireEvent.click(app.getByText("Second").closest("button")!);
    await flushEffects();
    expect(app.getByRole("heading", { name: "Second" })).toBeTruthy();

    await act(async () =>
      poll.resolve({
        ...first,
        runtime: { state: "running", run_id: "run-1", pid: 7 },
      }),
    );

    expect(app.getByRole("heading", { name: "Second" })).toBeTruthy();
  });

  it("does not let an older poll revert a completed action", async () => {
    vi.useFakeTimers();
    const first = delivery("first", "First", true);
    installApiMocks([first]);
    vi.mocked(api.delivery).mockResolvedValue(first);
    const view = render(<App />);
    const app = within(view.container as HTMLElement);
    await flushEffects();
    fireEvent.click(app.getByText("First").closest("button")!);
    await flushEffects();

    const poll = deferred<DeliveryRecord>();
    vi.mocked(api.delivery).mockImplementation(() => poll.promise);
    await act(async () => {
      await vi.advanceTimersByTimeAsync(2000);
    });
    vi.mocked(api.activate).mockResolvedValue({
      ...first,
      record_version: "2",
      runtime: { state: "running", run_id: "run-1", pid: 42 },
    });
    fireEvent.click(app.getByRole("button", { name: "Activate" }));
    await flushEffects();
    expect(app.getAllByText("running").length).toBeGreaterThan(0);

    await act(async () => poll.resolve(first));

    expect(app.getAllByText("running").length).toBeGreaterThan(0);
  });

  it("switches Play and Pause immediately without moving the transport controls", async () => {
    const first = delivery("first", "First", true);
    installApiMocks([first]);
    vi.mocked(api.delivery).mockResolvedValue(first);
    const activation = deferred<DeliveryRecord>();
    const pause = deferred<DeliveryRecord>();
    vi.mocked(api.activate).mockImplementation(() => activation.promise);
    vi.mocked(api.stop).mockImplementation(() => pause.promise);
    const view = render(<App />);
    const app = within(view.container as HTMLElement);
    await app.findByText("First");
    fireEvent.click(app.getByText("First").closest("button")!);
    await app.findByRole("heading", { name: "First" });
    const controls = app.getByLabelText("Delivery controls");

    fireEvent.click(app.getByRole("button", { name: "Activate" }));

    expect(app.getByLabelText("Delivery controls")).toBe(controls);
    expect(app.getByRole("button", { name: "Pause" })).toBeTruthy();
    expect(api.activate).toHaveBeenCalledWith("first", 1, "1");

    await act(async () =>
      activation.resolve({
        ...first,
        record_version: "2",
        runtime: { state: "running", run_id: "run-1", pid: 42 },
      }),
    );
    await waitFor(() =>
      expect(
        (app.getByRole("button", { name: "Pause" }) as HTMLButtonElement)
          .disabled,
      ).toBe(false),
    );

    fireEvent.click(app.getByRole("button", { name: "Pause" }));

    expect(app.getByLabelText("Delivery controls")).toBe(controls);
    expect(app.getByRole("button", { name: "Activate" })).toBeTruthy();
    expect(api.stop).toHaveBeenCalledWith("first", 1, "2", "run-1");

    await act(async () =>
      pause.resolve({
        ...first,
        record_version: "3",
        runtime: { state: "stopped" },
      }),
    );
  });

  it("keeps YAML from the latest configuration revision", async () => {
    vi.useFakeTimers();
    installApiMocks([]);
    const oldYaml = deferred<{ yaml: string }>();
    const newYaml = deferred<{ yaml: string }>();
    vi.mocked(api.yaml)
      .mockImplementationOnce(() => oldYaml.promise)
      .mockImplementationOnce(() => newYaml.promise);
    const view = render(<App />);
    const app = within(view.container as HTMLElement);
    await flushEffects();
    await act(async () => {
      await vi.advanceTimersByTimeAsync(120);
    });
    expect(api.yaml).toHaveBeenCalledTimes(1);

    const deliveryType = within(
      app.getByText("Delivery type").closest("label")!,
    );
    fireEvent.pointerDown(
      deliveryType.getByRole("button", { name: "Delivery type" }),
      { button: 0 },
    );
    fireEvent.pointerDown(app.getByRole("option", { name: "Batch" }), {
      button: 0,
    });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(120);
    });
    expect(api.yaml).toHaveBeenCalledTimes(2);
    await act(async () => newYaml.resolve({ yaml: "delivery_type: batch" }));
    await act(async () => oldYaml.resolve({ yaml: "stale: true" }));

    fireEvent.click(app.getByRole("tab", { name: "YAML" }));
    expect(
      (app.getByLabelText("YAML configuration") as HTMLTextAreaElement).value,
    ).toBe("delivery_type: batch");
    expect(view.container.querySelector(".syntax-key")?.textContent).toContain(
      "delivery_type",
    );
  });

  it("applies the edited YAML revision before validating it", async () => {
    const catalog: UiCatalog = {
      ...CATALOG,
      connectors: [
        {
          key: "source",
          title: "Test source",
          source: {
            schema: { type: "object", properties: {} },
            initial: {},
            delivery_modes: ["batch"],
            record_semantics: ["append_only"],
            partitioned: false,
            connection_check: false,
            message_preview: false,
          },
        },
        {
          key: "sink",
          title: "Test destination",
          sink: {
            schema: { type: "object", properties: {} },
            initial: {},
            delivery_modes: ["batch"],
            record_semantics: ["append_only"],
            partitioned: false,
            connection_check: false,
            message_preview: false,
          },
        },
      ],
    };
    const previousConfig = {
      delivery_id: "existing",
      delivery_type: "batch",
      source: { source: {} },
      sink: { sink: {} },
      pipeline_memory_limit_bytes: 1,
    };
    const appliedConfig = {
      ...previousConfig,
      pipeline_memory_limit_bytes: 2,
    };
    const existing = {
      ...delivery("existing", "Existing"),
      config: previousConfig,
    } satisfies DeliveryRecord;
    const updated = {
      ...existing,
      config: appliedConfig,
      revision: 2,
      record_version: "2",
    } satisfies DeliveryRecord;
    installApiMocks([existing]);
    vi.mocked(api.catalog).mockResolvedValue(catalog);
    vi.mocked(api.delivery).mockResolvedValue(existing);
    vi.mocked(api.yaml).mockResolvedValue({ yaml: "previous: true" });
    vi.mocked(api.parseYaml).mockResolvedValue({ config: appliedConfig });
    vi.mocked(api.update).mockResolvedValue(updated);
    vi.mocked(api.validate).mockResolvedValue({
      delivery: {
        ...updated,
        record_version: "3",
        validation: { state: "ready", revision: 2 },
      },
      discovery: discovery(),
    });
    const view = render(<App />);
    const app = within(view.container as HTMLElement);
    await app.findByText("Existing");
    fireEvent.click(app.getByText("Existing").closest("button")!);
    await app.findByRole("heading", { name: "Existing" });
    fireEvent.click(app.getByRole("button", { name: "Edit" }));
    fireEvent.click(app.getByRole("tab", { name: "YAML" }));
    const yaml = await app.findByLabelText("YAML configuration");
    fireEvent.input(yaml, { target: { value: "pipeline: changed" } });

    fireEvent.click(app.getByRole("button", { name: "Validate" }));

    await waitFor(() =>
      expect(api.update).toHaveBeenCalledWith(
        "existing",
        1,
        "1",
        "Existing",
        "",
        appliedConfig,
      ),
    );
    await waitFor(() =>
      expect(api.validate).toHaveBeenCalledWith("existing", 2, "2"),
    );
    expect(vi.mocked(api.parseYaml).mock.invocationCallOrder[0]).toBeLessThan(
      vi.mocked(api.update).mock.invocationCallOrder[0]!,
    );
    expect(vi.mocked(api.update).mock.invocationCallOrder[0]).toBeLessThan(
      vi.mocked(api.validate).mock.invocationCallOrder[0]!,
    );
  });

  it("does not validate stale state after YAML parsing fails", async () => {
    const existing = delivery("existing", "Existing");
    installApiMocks([existing]);
    vi.mocked(api.delivery).mockResolvedValue(existing);
    vi.mocked(api.parseYaml).mockRejectedValue(new Error("invalid YAML"));
    const view = render(<App />);
    const app = within(view.container as HTMLElement);
    await app.findByText("Existing");
    fireEvent.click(app.getByText("Existing").closest("button")!);
    await app.findByRole("heading", { name: "Existing" });
    fireEvent.click(app.getByRole("button", { name: "Edit" }));
    fireEvent.click(app.getByRole("tab", { name: "YAML" }));
    const yaml = await app.findByLabelText("YAML configuration");
    fireEvent.input(yaml, { target: { value: "not: [valid" } });

    fireEvent.click(app.getByRole("button", { name: "Validate" }));

    expect(await app.findByText("invalid YAML")).toBeTruthy();
    expect(api.update).not.toHaveBeenCalled();
    expect(api.validate).not.toHaveBeenCalled();
  });

  it("enables Performance advice only from current successful validation", async () => {
    const existing = adviceDelivery("advice-ready", "Advice ready");
    installApiMocks([existing]);
    vi.mocked(api.catalog).mockResolvedValue(speedtestCatalog());
    vi.mocked(api.delivery).mockResolvedValue(existing);
    vi.mocked(api.discover).mockResolvedValue(discovery(3));
    vi.mocked(api.validate).mockResolvedValue({
      delivery: {
        ...existing,
        record_version: "2",
        validation: { state: "ready", revision: 1 },
      },
      discovery: discovery(3),
    });
    const view = render(<App />);
    const app = within(view.container as HTMLElement);
    await app.findByText("Advice ready");
    fireEvent.click(app.getByText("Advice ready").closest("button")!);
    await app.findByRole("heading", { name: "Advice ready" });
    await waitFor(() => expect(api.discover).toHaveBeenCalled());

    const unavailable = app.getByRole("tab", { name: "Performance advice" });
    const host = unavailable.parentElement;
    const logs = app.getByRole("tab", { name: "Logs" });
    expect(unavailable.getAttribute("aria-disabled")).toBe("true");
    expect(host?.title).toBe("Available after successful validation");
    fireEvent.click(unavailable);
    expect(
      app.queryByRole("heading", { name: "Performance advice" }),
    ).toBeNull();

    fireEvent.click(app.getByRole("button", { name: "Edit" }));
    fireEvent.click(app.getByRole("button", { name: "Validate" }));

    const available = await app.findByRole("tab", {
      name: "Performance advice (3)",
    });
    expect(available).toBe(unavailable);
    expect(available.parentElement).toBe(host);
    expect(app.getByRole("tab", { name: "Logs" })).toBe(logs);
    expect(available.getAttribute("aria-disabled")).toBe("false");
    fireEvent.click(available);
    expect(
      await app.findByRole("heading", { name: "Performance advice" }),
    ).toBeTruthy();
    expect(app.getByText("Performance recommendation 1")).toBeTruthy();

    fireEvent.click(app.getByRole("tab", { name: "UI" }));
    fireEvent.input(app.getByLabelText("Host"), {
      target: { value: "changed.example" },
    });

    const invalidated = app.getByRole("tab", { name: "Performance advice" });
    expect(invalidated).toBe(unavailable);
    expect(invalidated.getAttribute("aria-disabled")).toBe("true");
    await waitFor(() => expect(api.discover).toHaveBeenCalledTimes(2));
    expect(invalidated.getAttribute("aria-disabled")).toBe("true");
  });

  it("keeps Performance advice unavailable after a successful empty result", async () => {
    const existing = adviceDelivery("advice-empty", "Advice empty");
    installApiMocks([existing]);
    vi.mocked(api.catalog).mockResolvedValue(speedtestCatalog());
    vi.mocked(api.delivery).mockResolvedValue(existing);
    vi.mocked(api.validate).mockResolvedValue({
      delivery: {
        ...existing,
        validation: { state: "ready", revision: 1 },
      },
      discovery: discovery(),
    });
    const view = render(<App />);
    const app = within(view.container as HTMLElement);
    await app.findByText("Advice empty");
    fireEvent.click(app.getByText("Advice empty").closest("button")!);
    await app.findByRole("heading", { name: "Advice empty" });
    fireEvent.click(app.getByRole("button", { name: "Edit" }));
    fireEvent.click(app.getByRole("button", { name: "Validate" }));
    await waitFor(() => expect(api.validate).toHaveBeenCalledOnce());

    const tab = app.getByRole("tab", { name: "Performance advice" });
    expect(tab.getAttribute("aria-disabled")).toBe("true");
    expect(tab.parentElement?.title).toBe(
      "No performance advice for this validated configuration",
    );
  });

  it("clears validated advice when validation returns invalid", async () => {
    const existing = adviceDelivery("advice-invalid", "Advice invalid");
    installApiMocks([existing]);
    vi.mocked(api.catalog).mockResolvedValue(speedtestCatalog());
    vi.mocked(api.delivery).mockResolvedValue(existing);
    vi.mocked(api.validate)
      .mockResolvedValueOnce({
        delivery: {
          ...existing,
          record_version: "2",
          validation: { state: "ready", revision: 1 },
        },
        discovery: discovery(2),
      })
      .mockResolvedValueOnce({
        delivery: {
          ...existing,
          record_version: "3",
          validation: {
            state: "invalid",
            revision: 1,
            message: "source schema changed",
          },
        },
      });
    const view = render(<App />);
    const app = within(view.container as HTMLElement);
    await app.findByText("Advice invalid");
    fireEvent.click(app.getByText("Advice invalid").closest("button")!);
    await app.findByRole("heading", { name: "Advice invalid" });
    fireEvent.click(app.getByRole("button", { name: "Edit" }));

    fireEvent.click(app.getByRole("button", { name: "Validate" }));
    await app.findByRole("tab", { name: "Performance advice (2)" });
    await app.findByText("Configuration is valid.");
    await waitFor(() =>
      expect(
        (app.getByRole("button", { name: "Validate" }) as HTMLButtonElement)
          .disabled,
      ).toBe(false),
    );
    fireEvent.click(app.getByRole("button", { name: "Validate" }));

    await app.findByText("Validation failed: source schema changed");
    const tab = app.getByRole("tab", { name: "Performance advice" });
    expect(tab.getAttribute("aria-disabled")).toBe("true");
    expect(tab.parentElement?.title).toBe(
      "Available after successful validation",
    );
    expect(app.queryByText("Configuration is valid.")).toBeNull();
  });

  it("enables Speedtest from endpoint readiness alone and sends the current config", async () => {
    const existing = {
      ...delivery("speedtest-ready", "Speedtest ready"),
      config: {
        delivery_id: "speedtest-ready",
        delivery_type: null,
        source: { source: { host: "source.example" } },
        sink: { sink: { database: "benchmark" } },
      },
    } satisfies DeliveryRecord;
    installApiMocks([existing]);
    vi.mocked(api.catalog).mockResolvedValue(speedtestCatalog());
    vi.mocked(api.delivery).mockResolvedValue(existing);
    vi.mocked(api.speedtestEstimate).mockResolvedValue({
      logical_streams: 1,
      source: speedtestMeasurement(1_000),
      destination: speedtestMeasurement(900),
      profile: {
        sampled_rows: 1,
        sampled_arrow_bytes: 8,
        sampled_deliveries: 1,
        sample_limit_bytes: 16_777_216,
        truncated: false,
        datasets: [],
      },
    });
    const view = render(<App />);
    const app = within(view.container as HTMLElement);
    await app.findByText("Speedtest ready");
    fireEvent.click(app.getByText("Speedtest ready").closest("button")!);
    await app.findByRole("heading", { name: "Speedtest ready" });

    const tab = app.getByRole("tab", { name: "Speedtest" });
    expect(tab.getAttribute("aria-disabled")).toBe("false");
    fireEvent.click(tab);
    fireEvent.click(await app.findByRole("button", { name: "Test" }));

    await waitFor(() => expect(api.speedtestEstimate).toHaveBeenCalledOnce());
    expect(api.speedtestEstimate).toHaveBeenCalledWith(
      {
        config: existing.config,
        duration_seconds: 10,
        cleanup_timeout_seconds: 60,
      },
      expect.any(AbortSignal),
    );
  });

  it("guides an unavailable Speedtest only to missing endpoint fields", async () => {
    const existing = {
      ...delivery("speedtest-incomplete", "Speedtest incomplete"),
      config: {
        delivery_id: "speedtest-incomplete",
        delivery_type: "batch",
        source: { source: { host: "source.example" } },
        sink: { sink: { database: "" } },
      },
    } satisfies DeliveryRecord;
    installApiMocks([existing]);
    vi.mocked(api.catalog).mockResolvedValue(speedtestCatalog());
    vi.mocked(api.delivery).mockResolvedValue(existing);
    const scrollIntoView = vi.fn();
    Object.defineProperty(HTMLElement.prototype, "scrollIntoView", {
      configurable: true,
      value: scrollIntoView,
    });
    const view = render(<App />);
    const app = within(view.container as HTMLElement);
    await app.findByText("Speedtest incomplete");
    fireEvent.click(app.getByText("Speedtest incomplete").closest("button")!);
    await app.findByRole("heading", { name: "Speedtest incomplete" });
    fireEvent.click(app.getByRole("button", { name: "Edit" }));

    fireEvent.click(app.getByRole("tab", { name: "Speedtest" }));

    await waitFor(() =>
      expect(
        app
          .getByText("Database")
          .closest(".form-row")
          ?.classList.contains("required-missing"),
      ).toBe(true),
    );
    expect(
      app
        .getByText("Delivery type")
        .closest("label")
        ?.classList.contains("required-missing"),
    ).toBe(false);
    expect(
      app
        .getByText("Host")
        .closest(".form-row")
        ?.classList.contains("required-missing"),
    ).toBe(false);
    await waitFor(() =>
      expect(document.activeElement).toBe(app.getByLabelText("Database")),
    );
    expect(scrollIntoView).toHaveBeenCalledOnce();
    expect(api.speedtestEstimate).not.toHaveBeenCalled();
  });

  it("applies edited YAML before deciding whether Speedtest is available", async () => {
    const incomplete = {
      ...delivery("speedtest-yaml", "Speedtest YAML"),
      config: {
        delivery_id: "speedtest-yaml",
        delivery_type: "batch",
        source: { source: { host: "" } },
        sink: { sink: { database: "benchmark" } },
      },
    } satisfies DeliveryRecord;
    const complete = {
      ...incomplete.config,
      source: { source: { host: "source.example" } },
    };
    installApiMocks([incomplete]);
    vi.mocked(api.catalog).mockResolvedValue(speedtestCatalog());
    vi.mocked(api.delivery).mockResolvedValue(incomplete);
    vi.mocked(api.yaml).mockResolvedValue({ yaml: "source: incomplete" });
    vi.mocked(api.parseYaml).mockResolvedValue({ config: complete });
    const view = render(<App />);
    const app = within(view.container as HTMLElement);
    await app.findByText("Speedtest YAML");
    fireEvent.click(app.getByText("Speedtest YAML").closest("button")!);
    await app.findByRole("heading", { name: "Speedtest YAML" });
    fireEvent.click(app.getByRole("button", { name: "Edit" }));
    fireEvent.click(app.getByRole("tab", { name: "YAML" }));
    fireEvent.input(await app.findByLabelText("YAML configuration"), {
      target: { value: "source: complete" },
    });

    fireEvent.click(app.getByRole("tab", { name: "Speedtest" }));

    expect(await app.findByRole("heading", { name: "Speedtest" })).toBeTruthy();
    expect(api.parseYaml).toHaveBeenCalledWith("source: complete");
    expect(api.speedtestEstimate).not.toHaveBeenCalled();
  });

  it("validates the committed save even when sidebar refresh fails", async () => {
    installApiMocks([]);
    vi.mocked(api.catalog).mockResolvedValue({
      ...CATALOG,
      connectors: [
        {
          key: "source",
          title: "Test source",
          source: {
            schema: { type: "object", properties: {} },
            initial: {},
            delivery_modes: ["batch"],
            record_semantics: ["append_only"],
            partitioned: false,
            connection_check: false,
            message_preview: false,
          },
        },
        {
          key: "sink",
          title: "Test destination",
          sink: {
            schema: { type: "object", properties: {} },
            initial: {},
            delivery_modes: ["batch"],
            record_semantics: ["append_only"],
            partitioned: false,
            connection_check: false,
            message_preview: false,
          },
        },
      ],
    });
    const created = delivery("created", "Created");
    vi.mocked(api.create).mockResolvedValue(created);
    vi.mocked(api.deliveries)
      .mockResolvedValueOnce([])
      .mockRejectedValueOnce(new Error("list unavailable"));
    vi.mocked(api.validate).mockResolvedValue({
      delivery: {
        ...created,
        record_version: "2",
        validation: { state: "ready", revision: 1 },
      },
      discovery: discovery(),
    });
    const view = render(<App />);
    const app = within(view.container as HTMLElement);
    await app.findByRole("heading", { name: "Untitled delivery" });

    fireEvent.input(app.getByLabelText("Delivery name"), {
      target: { value: "Created" },
    });
    chooseFromSelect(app, "Delivery type", "Batch");
    chooseFromSelect(app, "Source", "Test source");
    chooseFromSelect(app, "Destination", "Test destination");
    fireEvent.click(app.getByRole("button", { name: "Validate" }));

    await waitFor(() =>
      expect(api.validate).toHaveBeenCalledWith("created", 1, "1"),
    );
    expect(api.delivery).not.toHaveBeenCalled();
    expect(await app.findByText(/Delivery list refresh failed/)).toBeTruthy();
  });
});

function chooseFromSelect(
  app: ReturnType<typeof within>,
  label: string,
  option: string,
) {
  const field = within(app.getByText(label).closest("label, article")!);
  fireEvent.pointerDown(field.getAllByRole("button")[0]!, {
    button: 0,
  });
  fireEvent.pointerDown(app.getByRole("option", { name: option }), {
    button: 0,
  });
}

function selectOptions(
  app: ReturnType<typeof within>,
  label: string,
): string[] {
  const field = within(app.getByText(label).closest("label, article")!);
  fireEvent.pointerDown(field.getAllByRole("button")[0]!, { button: 0 });
  const options = app
    .getAllByRole("option")
    .map((option: HTMLElement) => option.textContent ?? "");
  fireEvent.pointerDown(field.getAllByRole("button")[0]!, { button: 0 });
  return options;
}

function endpoint(
  deliveryModes: ("batch" | "stream")[],
  recordSemantics: ("append_only" | "changelog")[],
) {
  return {
    schema: { type: "object" as const, properties: {} },
    initial: {},
    delivery_modes: deliveryModes,
    record_semantics: recordSemantics,
    partitioned: false,
    connection_check: false,
    message_preview: false,
  };
}

function connector(
  key: string,
  title: string,
  endpointDefinition: Pick<UiCatalog["connectors"][number], "source" | "sink">,
): UiCatalog["connectors"][number] {
  return { key, title, ...endpointDefinition };
}

function conditionalReplicationSource(): NonNullable<
  UiCatalog["connectors"][number]["source"]
> {
  return {
    schema: {
      type: "object",
      properties: {
        replication: {
          anyOf: [
            {
              type: "object",
              properties: {
                server_id: { type: "integer", minimum: 1 },
              },
              required: ["server_id"],
              "x-ui": {
                capabilities: {
                  component: "source",
                  key: "replication",
                  delivery_modes: ["stream", "batch_and_stream"],
                  record_semantics: ["changelog"],
                },
              },
            },
            { type: "null" },
          ],
        },
      },
      "x-ui": {
        capabilities: {
          component: "source",
          key: "snapshot",
          delivery_modes: ["batch"],
          record_semantics: ["append_only"],
        },
      },
    },
    initial: { replication: null },
    delivery_modes: ["batch", "stream", "batch_and_stream"],
    record_semantics: ["append_only", "changelog"],
    partitioned: false,
    connection_check: false,
    message_preview: false,
  };
}

function installApiMocks(deliveries: DeliverySummary[]) {
  vi.spyOn(api, "catalog").mockResolvedValue(CATALOG);
  vi.spyOn(api, "deliveries").mockResolvedValue(deliveries);
  vi.spyOn(api, "delivery").mockRejectedValue(new Error("unexpected delivery"));
  vi.spyOn(api, "create").mockRejectedValue(new Error("unexpected create"));
  vi.spyOn(api, "update").mockRejectedValue(new Error("unexpected update"));
  vi.spyOn(api, "yaml").mockResolvedValue({ yaml: "{}" });
  vi.spyOn(api, "parseYaml").mockResolvedValue({ config: {} });
  vi.spyOn(api, "discover").mockResolvedValue(discovery());
  vi.spyOn(api, "speedtestEstimate").mockRejectedValue(
    new Error("unexpected speedtest estimate"),
  );
  vi.spyOn(api, "speedtestTune").mockRejectedValue(
    new Error("unexpected speedtest tune"),
  );
  vi.spyOn(api, "validate").mockResolvedValue({
    delivery: delivery("validated", "Validated", true),
    discovery: discovery(),
  });
  vi.spyOn(api, "activate").mockRejectedValue(new Error("unexpected activate"));
  vi.spyOn(api, "stop").mockRejectedValue(new Error("unexpected stop"));
  vi.spyOn(api, "options").mockResolvedValue({ options: [] });
}

function delivery(id: string, name: string, ready = false): DeliveryRecord {
  return {
    id,
    name,
    description: "",
    revision: 1,
    validation: ready ? { state: "ready", revision: 1 } : { state: "draft" },
    runtime: { state: "stopped" },
    record_version: "1",
    config: {
      delivery_id: id,
      delivery_type: null,
      source: {},
      sink: {},
    },
    created_at_ms: 1,
    updated_at_ms: 1,
  };
}

function adviceDelivery(id: string, name: string): DeliveryRecord {
  return {
    ...delivery(id, name),
    config: {
      delivery_id: id,
      delivery_type: "batch",
      source: { source: { host: "source.example" } },
      sink: { sink: { database: "benchmark" } },
    },
  };
}

function discovery(performanceAdviceCount = 0): DiscoveryResult {
  return {
    source: "source",
    sink: "sink",
    pipeline_count: 1,
    performance_advice: Array.from(
      { length: performanceAdviceCount },
      (_, index) => ({
        code: `advice-${index + 1}`,
        severity: "warning" as const,
        summary: `Performance recommendation ${index + 1}`,
        explanation: "The discovered source layout can be improved.",
        remediation: "Apply the recommended source setting.",
        config_paths: ["#/source/source"],
      }),
    ),
    datasets: [],
    sink_limits: {
      sink: "sink",
      supported_arrow_types: [],
    },
  };
}

function speedtestCatalog(): UiCatalog {
  return {
    ...CATALOG,
    connectors: [
      {
        key: "source",
        title: "Test source",
        source: {
          ...endpoint(["batch"], ["append_only"]),
          schema: {
            type: "object",
            properties: { host: { type: "string", title: "Host" } },
            required: ["host"],
          },
          initial: { host: "" },
        },
      },
      {
        key: "sink",
        title: "Test destination",
        sink: {
          ...endpoint([], ["append_only"]),
          schema: {
            type: "object",
            properties: {
              database: { type: "string", title: "Database" },
            },
            required: ["database"],
          },
          initial: { database: "" },
        },
      },
    ],
  };
}

function speedtestMeasurement(rowsPerSecond: number) {
  return {
    rows: "1000",
    arrow_bytes: "8000",
    duration_ms: 1_000,
    rows_per_second: rowsPerSecond,
    bytes_per_second: rowsPerSecond * 8,
    completed: false,
  };
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((next) => {
    resolve = next;
  });
  return { promise, resolve };
}

async function flushEffects() {
  await act(async () => {
    await Promise.resolve();
    await Promise.resolve();
  });
}
