// @vitest-environment jsdom

import { cleanup, fireEvent, render } from "@testing-library/preact";
import { afterEach, describe, expect, it, vi } from "vitest";

import { MiddlewareEditor } from "../src/schema/MiddlewareEditor";

afterEach(cleanup);

describe("DataFusion middleware editor", () => {
  it("adds an immediately runnable SQL transform without raw YAML editing", () => {
    const onChange = vi.fn();
    const view = render(
      <MiddlewareEditor value={[]} disabled={false} onChange={onChange} />,
    );

    fireEvent.click(view.getByRole("button", { name: "+ Add SQL transform" }));

    expect(onChange).toHaveBeenCalledWith([
      { datafusion: { sql: "SELECT * FROM input" } },
    ]);
  });

  it("renders SQL and sample editors only for a DataFusion transform", () => {
    const view = render(
      <MiddlewareEditor
        value={[{ datafusion: { sql: "SELECT id FROM input" } }]}
        disabled={false}
        onChange={() => undefined}
      />,
    );

    expect(view.getByText("SQL over table")).toBeTruthy();
    expect(view.getByText("Playground")).toBeTruthy();
    expect(view.getByDisplayValue("SELECT id FROM input")).toBeTruthy();
  });
});
