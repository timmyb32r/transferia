// @vitest-environment jsdom

import { cleanup, fireEvent, render } from "@testing-library/preact";
import { afterEach, describe, expect, it, vi } from "vitest";

import { DeliverySidebar, EditorActions } from "../src/delivery/EditorChrome";
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

  it("passes the exact running generation to Stop", () => {
    const onStop = vi.fn();
    const view = render(
      <EditorActions
        editor={editor({ state: "running", run_id: "run-17", pid: 42 })}
        blocked={false}
        requiredFieldsComplete
        onMissingRequired={() => undefined}
        onEdit={() => undefined}
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
    const view = render(
      <EditorActions
        editor={{ ...editor({ state: "stopped" }), editing: false }}
        blocked={false}
        requiredFieldsComplete
        onMissingRequired={() => undefined}
        onEdit={onEdit}
        onSave={() => undefined}
        onValidate={() => undefined}
        onActivate={() => undefined}
        onStop={() => undefined}
      />,
    );

    expect(view.queryByRole("button", { name: "Save" })).toBeNull();
    fireEvent.click(view.getByRole("button", { name: "Edit" }));
    expect(onEdit).toHaveBeenCalledOnce();
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
  });

  it("reports sidebar navigation without owning request state", () => {
    const onNew = vi.fn();
    const onOpen = vi.fn();
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
        appearance={{ design: "yandex-cloud", theme: "dark" }}
        onAppearance={() => undefined}
        onNew={onNew}
        onOpen={onOpen}
      />,
    );

    fireEvent.click(view.getByRole("button", { name: "+ New delivery" }));
    fireEvent.click(view.getByRole("button", { name: /First/ }));

    expect(onNew).toHaveBeenCalledOnce();
    expect(onOpen).toHaveBeenCalledWith("delivery-1");
  });
});
