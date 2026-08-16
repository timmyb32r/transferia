// @vitest-environment jsdom

import { cleanup, fireEvent, render } from "@testing-library/preact";
import { afterEach, describe, expect, it, vi } from "vitest";

import { DeliverySidebar, EditorActions } from "../src/delivery/EditorChrome";
import type { EditorState } from "../src/state";

const editor = (runtime: EditorState["runtime"]): EditorState => ({
  sessionId: "session",
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
        onSave={() => undefined}
        onValidate={() => undefined}
        onActivate={() => undefined}
        onStop={() => undefined}
      />,
    );

    expect(
      (view.getByRole("button", { name: "Save draft" }) as HTMLButtonElement)
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
