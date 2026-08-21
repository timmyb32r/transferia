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
    expect(app.queryByRole("heading", { name: "Source" })).toBeNull();
    expect(app.queryByRole("heading", { name: "Destination" })).toBeNull();
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
    chooseFromSelect(app, "Destination", "Test destination");
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

  it("does not auto-open the data widget for an existing delivery", async () => {
    const existing = {
      ...delivery("existing", "Existing"),
      config: {
        delivery_id: "existing",
        delivery_type: "batch",
        source: { source: {} },
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
            schema: { type: "object", properties: {} },
            initial: {},
            delivery_modes: ["batch"],
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
            partitioned: false,
            connection_check: false,
            message_preview: false,
          },
        },
      ],
    });
    vi.mocked(api.delivery).mockResolvedValue(existing);
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
    await app.findByText("Existing");

    fireEvent.click(app.getByText("Existing").closest("button")!);
    await app.findByRole("heading", { name: "Existing" });
    await waitFor(() => expect(api.discover).toHaveBeenCalled());

    expect(app.queryByRole("dialog", { name: "Final schema" })).toBeNull();
    expect(app.getByRole("button", { name: "Data widget" })).toBeTruthy();
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

function installApiMocks(deliveries: DeliverySummary[]) {
  vi.spyOn(api, "catalog").mockResolvedValue(CATALOG);
  vi.spyOn(api, "deliveries").mockResolvedValue(deliveries);
  vi.spyOn(api, "delivery").mockRejectedValue(new Error("unexpected delivery"));
  vi.spyOn(api, "create").mockRejectedValue(new Error("unexpected create"));
  vi.spyOn(api, "update").mockRejectedValue(new Error("unexpected update"));
  vi.spyOn(api, "yaml").mockResolvedValue({ yaml: "{}" });
  vi.spyOn(api, "parseYaml").mockResolvedValue({ config: {} });
  vi.spyOn(api, "discover").mockResolvedValue(discovery());
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

function discovery(): DiscoveryResult {
  return {
    source: "source",
    sink: "sink",
    pipeline_count: 1,
    datasets: [],
    sink_limits: {
      sink: "sink",
      supported_arrow_types: [],
    },
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
