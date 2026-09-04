// @vitest-environment jsdom

import { cleanup, fireEvent, render } from "@testing-library/preact";
import { afterEach, describe, expect, it, vi } from "vitest";

import {
  DeliverySidebar,
  EditorActions,
  EditorTabs,
  OperationNotices,
} from "../src/delivery/EditorChrome";
import type { EditorState } from "../src/state";

const editor = (runtime: EditorState["runtime"]): EditorState => ({
  sessionId: "session",
  editing: true,
  id: "delivery",
  persistedRevision: 1,
  recordVersion: "2",
  localRevision: 0,
  savedLocalRevision: 0,
  name: "Delivery",
  description: "",
  config: {},
  validation: { state: "ready", revision: 1 },
  runtime,
});

describe("editor chrome", () => {
  afterEach(cleanup);

  it("keeps asynchronous feedback dismissible from the whole notice", () => {
    const dismiss = vi.fn();
    const view = render(
      <OperationNotices
        operations={{
          save: { requestId: 1, label: "Saving…" },
          validate: {
            requestId: 2,
            error: `Validation failed: ${"unbroken".repeat(100)}`,
          },
          action: { requestId: 3, success: "Configuration is valid." },
        }}
        onDismiss={dismiss}
      />,
    );

    const overlay = view.getByLabelText("Operation status");
    expect(overlay.classList.contains("operation-notices")).toBe(true);
    expect(view.getAllByRole("status")).toHaveLength(1);
    const error = view.getByRole("alert");
    expect(overlay.contains(error)).toBe(true);
    expect(error.tabIndex).toBe(0);
    expect(view.getByText("Configuration is valid.")).toBeTruthy();
    fireEvent.click(view.getByText("Configuration is valid."));
    expect(dismiss).toHaveBeenCalledWith("action", 3);
    fireEvent.click(error);
    expect(dismiss).toHaveBeenCalledWith("validate", 2);
  });

  it("exposes Data schema as a peer configuration view", () => {
    const onDataSchema = vi.fn();
    const onSpeedtest = vi.fn();
    const onPerformanceAdvice = vi.fn();
    const onLogs = vi.fn();
    const view = render(
      <EditorTabs
        active="ui"
        disabled={false}
        dataSchemaAvailable
        speedtestAvailable
        performanceAdviceCount={3}
        onUi={() => undefined}
        onYaml={() => undefined}
        onDataSchema={onDataSchema}
        onSpeedtest={onSpeedtest}
        onPerformanceAdvice={onPerformanceAdvice}
        onLogs={onLogs}
      />,
    );

    fireEvent.click(view.getByRole("tab", { name: "Data schema" }));

    expect(onDataSchema).toHaveBeenCalledOnce();
    fireEvent.click(view.getByRole("tab", { name: "Speedtest" }));
    expect(onSpeedtest).toHaveBeenCalledOnce();
    fireEvent.click(
      view.getByRole("tab", { name: "Performance advice (3)" }),
    );
    expect(onPerformanceAdvice).toHaveBeenCalledOnce();
    fireEvent.click(view.getByRole("tab", { name: "Logs" }));
    expect(onLogs).toHaveBeenCalledOnce();
  });

  it("keeps the Performance advice tab stable across validation states", () => {
    const onPerformanceAdvice = vi.fn();
    const renderTabs = (count: number | undefined, disabled = false) => (
      <EditorTabs
        active="ui"
        disabled={disabled}
        dataSchemaAvailable={false}
        performanceAdviceCount={count}
        onUi={() => undefined}
        onYaml={() => undefined}
        onDataSchema={() => undefined}
        onPerformanceAdvice={onPerformanceAdvice}
        onLogs={() => undefined}
      />
    );
    const view = render(renderTabs(undefined));
    const host = view.container.querySelector(
      ".performance-advice-tab-tooltip",
    );
    const tab = view.getByRole("tab", { name: "Performance advice" });
    const logs = view.getByRole("tab", { name: "Logs" });

    expect(host?.classList.contains("performance-advice-tab-tooltip")).toBe(
      true,
    );
    expect(host?.getAttribute("title")).toBe(
      "Available after successful validation",
    );
    expect(tab.getAttribute("aria-disabled")).toBe("true");
    fireEvent.click(tab);
    expect(onPerformanceAdvice).not.toHaveBeenCalled();

    view.rerender(renderTabs(3));
    expect(view.container.querySelector(".performance-advice-tab-tooltip")).toBe(
      host,
    );
    expect(view.getByRole("tab", { name: "Logs" })).toBe(logs);
    const available = view.getByRole("tab", {
      name: "Performance advice (3)",
    });
    expect(available).toBe(tab);
    expect(available.getAttribute("aria-disabled")).toBe("false");
    expect(
      available.querySelector(".performance-advice-tab-count")?.textContent,
    ).toBe("(3)");
    fireEvent.click(available);
    expect(onPerformanceAdvice).toHaveBeenCalledOnce();

    view.rerender(renderTabs(0));
    expect(view.container.querySelector(".performance-advice-tab-tooltip")).toBe(
      host,
    );
    expect(host?.getAttribute("title")).toBe(
      "No performance advice for this validated configuration",
    );
    expect(tab.getAttribute("aria-disabled")).toBe("true");

    view.rerender(renderTabs(3, true));
    expect((tab as HTMLButtonElement).disabled).toBe(true);
    expect(tab.getAttribute("aria-label")).toBe("Performance advice (3)");
    expect(view.getByRole("tab", { name: "Logs" })).toBe(logs);
  });

  it("keeps Speedtest diagnostic until both endpoint configurations are complete", () => {
    const onSpeedtestUnavailable = vi.fn();
    const view = render(
      <EditorTabs
        active="ui"
        disabled={false}
        dataSchemaAvailable={false}
        speedtestAvailable={false}
        speedtestUnavailableReason="Fill required destination field: Database"
        onUi={() => undefined}
        onYaml={() => undefined}
        onDataSchema={() => undefined}
        onSpeedtest={() => undefined}
        onSpeedtestUnavailable={onSpeedtestUnavailable}
        onPerformanceAdvice={() => undefined}
      />,
    );
    const tab = view.getByRole("tab", { name: "Speedtest" });

    expect(tab.getAttribute("aria-disabled")).toBe("true");
    expect(tab.parentElement?.title).toBe(
      "Fill required destination field: Database",
    );
    fireEvent.click(tab);
    expect(onSpeedtestUnavailable).toHaveBeenCalledOnce();
  });

  it("lets unavailable Data schema reveal missing source fields", () => {
    const onDataSchemaUnavailable = vi.fn();
    const view = render(
      <EditorTabs
        active="ui"
        disabled={false}
        dataSchemaAvailable={false}
        dataSchemaUnavailableReason="Complete the required parser settings"
        onUi={() => undefined}
        onYaml={() => undefined}
        onDataSchema={() => undefined}
        onDataSchemaUnavailable={onDataSchemaUnavailable}
        onPerformanceAdvice={() => undefined}
      />,
    );
    const tab = view.getByRole("tab", {
      name: "Data schema",
    }) as HTMLButtonElement;
    expect(tab.disabled).toBe(false);
    expect(tab.getAttribute("aria-disabled")).toBe("true");
    expect(tab.parentElement?.title).toBe(
      "Complete the required parser settings",
    );
    fireEvent.click(tab);
    expect(onDataSchemaUnavailable).toHaveBeenCalledOnce();
  });

  it("keeps Deactivate left of Pause and passes the exact run generation", () => {
    const onStop = vi.fn();
    const onPause = vi.fn();
    const view = render(
      <EditorActions
        editor={editor({ state: "running", run_id: "run-17", pid: 42 })}
        blocked={false}
        requiredFieldsComplete
        onMissingRequired={() => undefined}
        onEdit={() => undefined}
        onClone={() => undefined}
        onDelete={() => undefined}
        onSave={() => undefined}
        onValidate={() => undefined}
        onActivate={() => undefined}
        onPause={onPause}
        onStop={onStop}
      />,
    );

    const controls = view.getByLabelText("Delivery controls");
    const deactivate = view.getByRole("button", { name: "Deactivate" });
    const pause = view.getByRole("button", { name: "Pause" });
    expect(Array.from(controls.querySelectorAll("button"))).toEqual([
      deactivate,
      pause,
    ]);
    expect(deactivate.querySelector(".stop-icon")).toBeTruthy();
    expect(pause.querySelector(".pause-icon")).toBeTruthy();

    fireEvent.click(deactivate);
    fireEvent.click(pause);

    expect(onStop).toHaveBeenCalledWith("run-17");
    expect(onPause).toHaveBeenCalledWith("run-17");
  });

  it("keeps the transport control footprint stable while Play becomes Pause", () => {
    const inactive = editor({ state: "stopped" });
    const renderActions = (
      runtimeEditor: EditorState,
      runtimeActionIntent?: "activate" | "pause",
    ) => (
      <EditorActions
        editor={runtimeEditor}
        blocked={runtimeActionIntent !== undefined}
        activatePending={runtimeActionIntent !== undefined}
        runtimeActionIntent={runtimeActionIntent}
        requiredFieldsComplete
        onMissingRequired={() => undefined}
        onEdit={() => undefined}
        onClone={() => undefined}
        onDelete={() => undefined}
        onSave={() => undefined}
        onValidate={() => undefined}
        onActivate={() => undefined}
        onStop={() => undefined}
      />
    );
    const view = render(renderActions(inactive));
    const controls = view.getByLabelText("Delivery controls");
    const deactivate = view.getByRole("button", { name: "Deactivate" });
    const activate = view.getByRole("button", { name: "Activate" });
    expect((deactivate as HTMLButtonElement).disabled).toBe(true);
    expect(activate.querySelector(".play-icon")).toBeTruthy();

    view.rerender(renderActions(inactive, "activate"));

    expect(view.getByLabelText("Delivery controls")).toBe(controls);
    expect(view.getByRole("button", { name: "Deactivate" })).toBe(deactivate);
    expect(view.getByRole("button", { name: "Pause" })).toBeTruthy();
    expect(view.queryByRole("button", { name: "Activate" })).toBeNull();

    view.rerender(
      renderActions(
        editor({ state: "running", run_id: "run-17", pid: 42 }),
        "pause",
      ),
    );

    expect(view.getByLabelText("Delivery controls")).toBe(controls);
    expect(view.getByRole("button", { name: "Activate" })).toBeTruthy();
    expect(view.queryByRole("button", { name: "Pause" })).toBeNull();
  });

  it("keeps delivery actions disabled while a blocking command is pending", () => {
    const view = render(
      <EditorActions
        editor={editor({ state: "stopped" })}
        blocked
        requiredFieldsComplete
        onMissingRequired={() => undefined}
        onEdit={() => undefined}
        onClone={() => undefined}
        onDelete={() => undefined}
        onSave={() => undefined}
        onValidate={() => undefined}
        onActivate={() => undefined}
        onStop={() => undefined}
      />,
    );

    expect(
      (view.getByRole("button", { name: "Save" }) as HTMLButtonElement)
        .disabled,
    ).toBe(true);
    expect(
      (view.getByRole("button", { name: "Validate" }) as HTMLButtonElement)
        .disabled,
    ).toBe(true);
    expect(
      (view.getByRole("button", { name: "Activate" }) as HTMLButtonElement)
        .disabled,
    ).toBe(true);
  });

  it("shows Edit instead of Save for an existing delivery in view mode", () => {
    const onEdit = vi.fn();
    const onDelete = vi.fn();
    const view = render(
      <EditorActions
        editor={{ ...editor({ state: "stopped" }), editing: false }}
        blocked={false}
        requiredFieldsComplete
        onMissingRequired={() => undefined}
        onEdit={onEdit}
        onClone={() => undefined}
        onDelete={onDelete}
        onSave={() => undefined}
        onValidate={() => undefined}
        onActivate={() => undefined}
        onStop={() => undefined}
      />,
    );

    expect(view.queryByRole("button", { name: "Save" })).toBeNull();
    fireEvent.click(view.getByRole("button", { name: "Edit" }));
    expect(onEdit).toHaveBeenCalledOnce();
    fireEvent.click(view.getByRole("button", { name: "Delete" }));
    expect(onDelete).toHaveBeenCalledOnce();
  });

  it("allows a newly created delivery to enter edit mode", () => {
    const onEdit = vi.fn();
    const view = render(
      <EditorActions
        editor={{ ...editor({ state: "created" }), editing: false }}
        blocked={false}
        requiredFieldsComplete
        onMissingRequired={() => undefined}
        onEdit={onEdit}
        onClone={() => undefined}
        onDelete={() => undefined}
        onSave={() => undefined}
        onValidate={() => undefined}
        onActivate={() => undefined}
        onStop={() => undefined}
      />,
    );

    fireEvent.click(view.getByRole("button", { name: "Edit" }));

    expect(onEdit).toHaveBeenCalledOnce();
  });

  it("lets an inactive Activate reveal missing required fields", () => {
    const onMissingRequired = vi.fn();
    const onActivate = vi.fn();
    const view = render(
      <EditorActions
        editor={{
          sessionId: "new-session",
          editing: true,
          localRevision: 0,
          name: "",
          description: "",
          config: {},
          validation: { state: "draft" },
          runtime: { state: "stopped" },
        }}
        blocked={false}
        requiredFieldsComplete={false}
        onMissingRequired={onMissingRequired}
        onEdit={() => undefined}
        onClone={() => undefined}
        onDelete={() => undefined}
        onSave={() => undefined}
        onValidate={() => undefined}
        onActivate={onActivate}
        onStop={() => undefined}
      />,
    );

    const activate = view.getByRole("button", { name: "Activate" });
    expect((activate as HTMLButtonElement).disabled).toBe(false);
    expect(activate.getAttribute("aria-disabled")).toBe("true");
    fireEvent.click(activate);

    expect(onMissingRequired).toHaveBeenCalledOnce();
    expect(onActivate).not.toHaveBeenCalled();
    expect(view.getByRole("tooltip").textContent).toBe(
      "Complete the required delivery, source, and destination fields",
    );
  });

  it("explains an inactive Activate immediately", () => {
    const view = render(
      <EditorActions
        editor={{
          ...editor({ state: "stopped" }),
          validation: { state: "draft" },
        }}
        blocked={false}
        requiredFieldsComplete
        onMissingRequired={() => undefined}
        onEdit={() => undefined}
        onClone={() => undefined}
        onDelete={() => undefined}
        onSave={() => undefined}
        onValidate={() => undefined}
        onActivate={() => undefined}
        onStop={() => undefined}
      />,
    );

    expect(
      (view.getByRole("button", { name: "Activate" }) as HTMLButtonElement)
        .disabled,
    ).toBe(true);
    expect(view.getByRole("tooltip").textContent).toBe(
      "Validate the current revision first",
    );
  });

  it("shows Validate pending feedback without waiting for a progress notice", () => {
    const view = render(
      <EditorActions
        editor={editor({ state: "stopped" })}
        blocked
        validatePending
        requiredFieldsComplete
        onMissingRequired={() => undefined}
        onEdit={() => undefined}
        onClone={() => undefined}
        onDelete={() => undefined}
        onSave={() => undefined}
        onValidate={() => undefined}
        onActivate={() => undefined}
        onStop={() => undefined}
      />,
    );
    const validate = view.getByRole("button", { name: "Validate" });

    expect(validate.getAttribute("aria-busy")).toBe("true");
    expect(validate.classList.contains("interaction-pending")).toBe(true);
  });

  it("lets Validate reveal missing required fields without submitting", () => {
    const onMissingRequired = vi.fn();
    const onValidate = vi.fn();
    const view = render(
      <EditorActions
        editor={{
          sessionId: "new-session",
          editing: true,
          localRevision: 0,
          name: "",
          description: "",
          config: {},
          validation: { state: "draft" },
          runtime: { state: "stopped" },
        }}
        blocked={false}
        requiredFieldsComplete={false}
        onMissingRequired={onMissingRequired}
        onEdit={() => undefined}
        onClone={() => undefined}
        onDelete={() => undefined}
        onSave={() => undefined}
        onValidate={onValidate}
        onActivate={() => undefined}
        onStop={() => undefined}
      />,
    );

    const validate = view.getByRole("button", { name: "Validate" });
    expect((validate as HTMLButtonElement).disabled).toBe(false);
    expect(validate.classList.contains("diagnostic-disabled")).toBe(false);
    expect(validate.getAttribute("aria-disabled")).toBeNull();
    fireEvent.click(validate);

    expect(onMissingRequired).toHaveBeenCalledOnce();
    expect(onValidate).not.toHaveBeenCalled();
  });

  it("reports sidebar navigation without owning request state", () => {
    const onNew = vi.fn();
    const onOpen = vi.fn();
    const onToggleDataWidget = vi.fn();
    const view = render(
      <DeliverySidebar
        catalog={{ common_schema: {}, initial: {}, connectors: [] }}
        deliveries={[
          {
            id: "delivery-1",
            name: "First",
            description: "",
            revision: 1,
            validation: { state: "draft" },
            runtime: { state: "stopped" },
            updated_at_ms: 0,
          },
        ]}
        selectedId={undefined}
        appearance={{
          design: "classic",
          theme: "dark",
        }}
        onAppearance={() => undefined}
        dataWidgetAvailable
        dataWidgetVisible={false}
        onToggleDataWidget={onToggleDataWidget}
        onNew={onNew}
        onOpen={onOpen}
      />,
    );

    fireEvent.click(view.getByRole("button", { name: "+ New delivery" }));
    fireEvent.click(view.getByRole("button", { name: "Open Transferia home" }));
    const deliveryItem = view.getByRole("button", { name: /First/ });
    fireEvent.click(deliveryItem.querySelector(".delivery-item-name")!);
    fireEvent.click(deliveryItem.querySelector(".status")!);
    fireEvent.click(view.getByRole("button", { name: "Data widget" }));

    expect(
      view.getByRole("button", { name: "Data widget" }).classList,
    ).toContain("primary");
    expect(
      view.getByRole("button", { name: "Data widget" }).classList,
    ).toContain("data-widget-ready");

    expect(onNew).toHaveBeenCalledTimes(2);
    expect(onOpen).toHaveBeenCalledTimes(2);
    expect(onOpen).toHaveBeenNthCalledWith(1, "delivery-1");
    expect(onOpen).toHaveBeenNthCalledWith(2, "delivery-1");
    expect(onToggleDataWidget).toHaveBeenCalledOnce();
    const sidebarButtons = view.getAllByRole("button");
    expect(
      sidebarButtons.indexOf(view.getByRole("button", { name: "Data widget" })),
    ).toBeLessThan(
      sidebarButtons.indexOf(
        view.getByRole("button", { name: "Matrix" }),
      ),
    );
    expect(
      sidebarButtons.indexOf(
        view.getByRole("button", { name: "Matrix" }),
      ),
    ).toBeLessThan(
      sidebarButtons.indexOf(view.getByRole("button", { name: /Settings/ })),
    );
  });

  it("enables and highlights the data widget without remounting nearby controls", () => {
    const props = {
      catalog: { common_schema: {}, initial: {}, connectors: [] },
      deliveries: [],
      selectedId: undefined,
      appearance: { design: "classic" as const, theme: "dark" as const },
      onAppearance: () => undefined,
      dataWidgetUnavailableReason: "Discover data first",
      dataWidgetVisible: false,
      onToggleDataWidget: () => undefined,
      onNew: () => undefined,
      onOpen: () => undefined,
    };
    const view = render(
      <DeliverySidebar {...props} dataWidgetAvailable={false} />,
    );
    const unavailable = view.getByRole("button", {
      name: "Data widget",
    }) as HTMLButtonElement;
    const matrix = view.getByRole("button", { name: "Matrix" });
    const settings = view.getByRole("button", { name: /Settings/ });
    expect(unavailable.disabled).toBe(true);
    expect(unavailable.classList).not.toContain("data-widget-ready");

    view.rerender(<DeliverySidebar {...props} dataWidgetAvailable />);
    const available = view.getByRole("button", {
      name: "Data widget",
    }) as HTMLButtonElement;
    expect(available).toBe(unavailable);
    expect(available.disabled).toBe(false);
    expect(available.classList).toContain("primary");
    expect(available.classList).toContain("data-widget-ready");
    expect(view.getByRole("button", { name: "Matrix" })).toBe(
      matrix,
    );
    expect(view.getByRole("button", { name: /Settings/ })).toBe(settings);
  });
});
