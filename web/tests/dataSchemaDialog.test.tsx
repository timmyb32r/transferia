// @vitest-environment jsdom
import { cleanup, fireEvent, render } from "@testing-library/preact";
import { useState } from "preact/hooks";
import { afterEach, expect, it, vi } from "vitest";
import { DataSchemaDialog } from "../src/delivery/DataSchemaDialog";
import type { DiscoveryResult } from "../src/types";

afterEach(cleanup);
const result: DiscoveryResult = { source: "postgres", sink: "clickhouse", pipeline_count: 1,
  performance_advice: [], sink_limits: { sink: "clickhouse", supported_arrow_types: [] },
  datasets: ["events", "users"].map(name => ({ name, role: "Main", intermediate_columns: [], final_columns: [] })) };

it("opens over the editor, focuses Close and restores the launcher and scrolling on close", () => {
  function Fixture() {
    const [open, setOpen] = useState(false);
    return <><button onClick={() => setOpen(true)}>Schema viewer</button><p>Editor remains mounted</p>
      {open && <DataSchemaDialog result={result} onClose={() => setOpen(false)} />}</>;
  }
  const view = render(<Fixture />);
  const launcher = view.getByRole("button", { name: "Schema viewer" });
  const editor = view.getByText("Editor remains mounted");
  const overflow = document.documentElement.style.overflow;
  launcher.focus(); fireEvent.click(launcher);
  const dialog = view.getByRole("dialog", { name: "Schema viewer" });
  expect(dialog.getAttribute("aria-modal")).toBe("true");
  expect(document.activeElement).toBe(view.getByRole("button", { name: "Close Schema viewer" }));
  expect(document.documentElement.style.overflow).toBe("hidden");
  expect(view.getByText("Editor remains mounted")).toBe(editor);
  fireEvent.click(view.getByRole("button", { name: "Close Schema viewer" }));
  expect(view.queryByRole("dialog")).toBeNull();
  expect(document.activeElement).toBe(launcher);
  expect(document.documentElement.style.overflow).toBe(overflow);
});

it("dismisses the table picker with Escape before closing the popup", () => {
  const close = vi.fn();
  const view = render(<DataSchemaDialog result={result} onClose={close} />);
  fireEvent.click(view.getByRole("button", { name: "events" }));
  fireEvent.keyDown(view.getByRole("searchbox"), { key: "Escape" });
  expect(view.queryByRole("listbox")).toBeNull();
  expect(close).not.toHaveBeenCalled();
  fireEvent.keyDown(view.getByRole("dialog"), { key: "Escape" });
  expect(close).toHaveBeenCalledOnce();
});

it("keeps the popup and close control mounted while loading, showing results or errors", () => {
  const close = vi.fn();
  const view = render(<DataSchemaDialog onClose={close} />);
  const dialog = view.getByRole("dialog");
  const button = view.getByRole("button", { name: "Close Schema viewer" });
  expect(view.getByRole("status").textContent).toContain("Discovering");
  view.rerender(<DataSchemaDialog result={result} onClose={close} />);
  expect(view.getByRole("button", { name: "events" })).toBeTruthy();
  view.rerender(<DataSchemaDialog error="Schema discovery failed" onClose={close} />);
  expect(view.getByRole("status").textContent).toBe("Schema discovery failed");
  expect(view.getByRole("dialog")).toBe(dialog);
  expect(view.getByRole("button", { name: "Close Schema viewer" })).toBe(button);
  fireEvent.mouseDown(dialog.parentElement!);
  expect(close).toHaveBeenCalledOnce();
});
