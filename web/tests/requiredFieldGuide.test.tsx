// @vitest-environment jsdom

import { render, waitFor } from "@testing-library/preact";
import { useRef } from "preact/hooks";
import { describe, expect, it } from "vitest";

import { RequiredFieldGuide } from "../src/delivery/RequiredFieldGuide";

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
