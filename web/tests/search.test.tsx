// @vitest-environment jsdom

import { render } from "@testing-library/preact";
import { describe, expect, it } from "vitest";

import { SearchHighlight } from "../src/ui/SearchHighlight";
import { searchMatchIndices } from "../src/ui/search";

describe("search match highlighting", () => {
  it("highlights the first subsequence match", () => {
    expect(searchMatchIndices("cdc", "dc")).toEqual([1, 2]);
    expect(searchMatchIndices("adbc", "dc")).toEqual([1, 3]);
    const view = render(<SearchHighlight text="adbc" query="dc" />);
    expect([...view.container.querySelectorAll("strong")].map((node) => node.textContent)).toEqual([
      "d",
      "c",
    ]);
  });

  it("highlights matches typed with the Russian keyboard layout", () => {
    expect(searchMatchIndices("cdc", "вс")).toEqual([1, 2]);
  });
});
