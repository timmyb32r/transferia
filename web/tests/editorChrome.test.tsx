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
    const onLogs = vi.fn();
    const view = render(
      <EditorTabs
        active="ui"
        disabled={false}
        dataSchemaAvailable
        onUi={() => undefined}
        onYaml={() => undefined}
        onDataSchema={onDataSchema}
        onLogs={onLogs}
      />,
    );

    fireEvent.click(view.getByRole("tab", { name: "Data schema" }));

    expect(onDataSchema).toHaveBeenCalledOnce();
    fireEvent.click(view.getByRole("tab", { name: "Logs" }));
    expect(onLogs).toHaveBeenCalledOnce();
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

  it("passes the exact running generation to Stop", () => {
    const onStop = vi.fn();
    const view = render(
      <EditorActions
        editor={editor({ state: "running", run_id: "run-17", pid: 42 })}
        blocked={false}
        requiredFieldsComplete
        onMissingRequired={() => undefined}
        onEdit={() => undefined}
        onDelete={() => undefined}
        onSave={() => undefined}
        onValidate={() => undefined}
        onActivate={() => undefined}
        onStop={onStop}
      />,
    );

    fireEvent.click(view.getByRole("button", { name: "Stop" }));

    expect(onStop).toHaveBeenCalledWith("run-17");
  });

  it("keeps delivery actions disabled while a blocking command is pending", () => {
    const view = render(
      <EditorActions
        editor={editor({ state: "stopped" })}
        blocked
        requiredFieldsComplete
        onMissingRequired={() => undefined}
        onEdit={() => undefined}
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
    expect(
      view.getByRole("tooltip").textContent,
    ).toBe("Complete the required delivery, source, and destination fields");
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
          design: "yandex-cloud",
          theme: "dark",
          autoShowSchemaWidget: true,
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
    fireEvent.click(view.getByRole("button", { name: /First/ }));
    fireEvent.click(view.getByRole("button", { name: "Data widget" }));

    expect(onNew).toHaveBeenCalledTimes(2);
    expect(onOpen).toHaveBeenCalledWith("delivery-1");
    expect(onToggleDataWidget).toHaveBeenCalledOnce();
  });
});
