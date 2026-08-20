// @vitest-environment jsdom

import { cleanup, fireEvent, render, waitFor } from "@testing-library/preact";
import { useRef, useState } from "preact/hooks";
import { afterEach, describe, expect, it } from "vitest";

import { RequiredFieldGuide } from "../src/delivery/RequiredFieldGuide";
import { requestRequiredGuidance } from "../src/ui/requiredGuidance";

afterEach(cleanup);

describe("required field guide", () => {
  it("highlights every incomplete branch on the path to the next leaf", async () => {
    const view = render(<Harness />);
    const parent = view.getByTestId("parent");
    const leaf = view.getByTestId("leaf");
    const sibling = view.getByTestId("sibling");

    await waitFor(() =>
      expect(leaf.classList.contains("required-next")).toBe(true),
    );
    expect(parent.classList.contains("required-next")).toBe(true);
    expect(sibling.classList.contains("required-next")).toBe(false);
    expect(
      view
        .getByRole("button", { name: "Choose auth type" })
        .classList.contains("required-next-control"),
    ).toBe(true);
    expect(
      view.getByLabelText("Token").classList.contains("required-next-control"),
    ).toBe(true);
    expect(
      view
        .getByLabelText("Unrelated")
        .classList.contains("required-next-control"),
    ).toBe(false);
  });

  it("prioritizes the first incomplete field inside an explicitly revealed scope", async () => {
    const view = render(<ScopedHarness />);
    const earlier = view.getByLabelText("Earlier required field");
    const parserField = view.getByLabelText("Parser required field");

    await waitFor(() =>
      expect(earlier.classList.contains("required-next-control")).toBe(true),
    );

    requestRequiredGuidance(view.getByTestId("parser-settings"));

    await waitFor(() =>
      expect(parserField.classList.contains("required-next-control")).toBe(
        true,
      ),
    );
    expect(earlier.classList.contains("required-next-control")).toBe(false);
  });

  it("advances from a completed parser field to the incomplete column row after blur", async () => {
    const view = render(<ParserHarness />);
    const tableName = view.getByLabelText("Table name");
    const row = view.getByTestId("column-row");

    await waitFor(() =>
      expect(tableName.classList.contains("required-next-control")).toBe(true),
    );
    tableName.focus();
    fireEvent.input(tableName, { target: { value: "events" } });
    expect(row.classList.contains("required-next")).toBe(false);

    tableName.blur();

    await waitFor(() =>
      expect(row.classList.contains("required-next")).toBe(true),
    );
  });

  it("does not guide unrelated controls owned by a structural required ancestor", async () => {
    const view = render(<StructuralHarness />);

    await waitFor(() =>
      expect(
        view.getByTestId("column-row").classList.contains("required-next"),
      ).toBe(true),
    );
    expect(
      view
        .getByLabelText("Column name")
        .classList.contains("required-next-control"),
    ).toBe(true);
    expect(
      view
        .getByRole("button", { name: "Keys" })
        .classList.contains("required-next-control"),
    ).toBe(false);
    expect(
      view
        .getByLabelText("Destination")
        .classList.contains("required-next-control"),
    ).toBe(false);

    view.getByLabelText("Destination").focus();
    requestRequiredGuidance(view.container.firstElementChild as HTMLElement);
    await waitFor(() =>
      expect(
        view.getByTestId("column-row").classList.contains("required-next"),
      ).toBe(true),
    );
    expect(
      view
        .getByLabelText("Destination")
        .classList.contains("required-next-control"),
    ).toBe(false);
  });

  it("uses the same row and control target for validation with an error tone", async () => {
    const view = render(<StructuralHarness tone="error" />);
    const row = view.getByTestId("column-row");
    const columnName = view.getByLabelText("Column name");

    await waitFor(() =>
      expect(row.classList.contains("required-error")).toBe(true),
    );
    expect(row.classList.contains("required-next")).toBe(true);
    expect(columnName.classList.contains("required-next-control")).toBe(true);
    expect(columnName.classList.contains("required-error-control")).toBe(true);
    expect(
      view
        .getByRole("button", { name: "Keys" })
        .classList.contains("required-error-control"),
    ).toBe(false);
  });
});

function Harness() {
  const root = useRef<HTMLDivElement>(null);
  return (
    <div ref={root}>
      <RequiredFieldGuide root={root} enabled revision={0} />
      <div data-testid="parent" class="required-incomplete">
        <button type="button" class="select-trigger">
          Choose auth type
        </button>
        <div data-testid="leaf" class="required-incomplete">
          <label>
            Token
            <input aria-label="Token" />
          </label>
        </div>
        <div data-testid="sibling" class="required-incomplete">
          <label>
            Unrelated
            <input aria-label="Unrelated" />
          </label>
        </div>
      </div>
    </div>
  );
}

function ScopedHarness() {
  const root = useRef<HTMLDivElement>(null);
  return (
    <div ref={root}>
      <RequiredFieldGuide root={root} enabled revision={0} />
      <div class="required-incomplete">
        <input aria-label="Earlier required field" />
      </div>
      <section data-testid="parser-settings">
        <div class="required-incomplete">
          <input aria-label="Parser required field" />
        </div>
      </section>
    </div>
  );
}

function ParserHarness() {
  const root = useRef<HTMLDivElement>(null);
  const [tableName, setTableName] = useState("");
  return (
    <div ref={root}>
      <RequiredFieldGuide root={root} enabled revision={tableName} />
      <div class={tableName === "" ? "required-incomplete" : ""}>
        <input
          aria-label="Table name"
          value={tableName}
          onInput={(event) => setTableName(event.currentTarget.value)}
        />
      </div>
      <table>
        <tbody>
          <tr data-testid="column-row" class="required-incomplete">
            <td>
              <input aria-label="Column name" />
            </td>
            <td>
              <input aria-label="JSON path" />
            </td>
          </tr>
        </tbody>
      </table>
    </div>
  );
}

function StructuralHarness({ tone = "guided" }: { tone?: "guided" | "error" }) {
  const root = useRef<HTMLDivElement>(null);
  return (
    <div ref={root}>
      <RequiredFieldGuide root={root} enabled revision={0} tone={tone} />
      <div class="required-incomplete" data-required-guidance="structural">
        <table>
          <tbody>
            <tr data-testid="column-row" class="required-incomplete">
              <td>
                <input aria-label="Column name" />
              </td>
            </tr>
          </tbody>
        </table>
        <button type="button" class="select-trigger">
          Keys
        </button>
      </div>
      <div class="required-incomplete required-missing">
        <input aria-label="Destination" />
      </div>
    </div>
  );
}
