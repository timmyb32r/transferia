// @vitest-environment jsdom

import { cleanup, fireEvent, waitFor } from "@testing-library/preact";
import { afterEach, describe, expect, it, vi } from "vitest";

import { MiddlewareEditor } from "../src/features/middleware/MiddlewareEditor";
import { SourceSampleProvider } from "../src/features/middleware/SourceSampleContext";
import { render } from "./support/render";

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
    expect(view.getByRole("region", { name: "Playground" })).toBeTruthy();
    expect(view.getByRole("tab", { name: "Input" })).toBeTruthy();
    expect(view.getByRole("tab", { name: "Output" })).toBeTruthy();
    expect(view.getByDisplayValue("SELECT id FROM input")).toBeTruthy();
  });

  it("loads playground rows from the source instead of inventing sample data", async () => {
    const load = vi.fn().mockResolvedValue([{ id: 17, name: "source" }]);
    const view = render(
      <SourceSampleProvider loader={load}>
        <MiddlewareEditor
          value={[{ datafusion: { sql: "SELECT * FROM input" } }]}
          disabled={false}
          onChange={() => undefined}
        />
      </SourceSampleProvider>,
    );

    fireEvent.click(view.getByRole("button", { name: "Run sample" }));
    await waitFor(() => expect(load).toHaveBeenCalledOnce());

    expect(
      view.getByRole("tab", { name: "Output" }).getAttribute("aria-selected"),
    ).toBe("true");
  });
});
