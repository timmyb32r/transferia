// @vitest-environment jsdom
import { cleanup, fireEvent } from "@testing-library/preact";
import { afterEach, expect, it, vi } from "vitest";
import { MatchedTablesDisclosure } from "../src/features/tableSelection/MatchedTablesDisclosure";
import { render } from "./support/render";

afterEach(() => { cleanup(); vi.restoreAllMocks(); });

function geometry() {
  vi.spyOn(HTMLElement.prototype, "offsetHeight", "get").mockImplementation(function (this: HTMLElement) {
    return this.classList.contains("table-rule-matches")
      ? Number.parseFloat(this.style.height) || Math.min(140, this.children.length * 26 + 2) : 0;
  });
  vi.spyOn(HTMLElement.prototype, "clientHeight", "get").mockImplementation(function (this: HTMLElement) { return this.offsetHeight - 2; });
  vi.spyOn(HTMLElement.prototype, "scrollHeight", "get").mockImplementation(function (this: HTMLElement) {
    return Math.max(this.clientHeight, this.children.length * 26);
  });
}

function disclosure(count: number | undefined, open = true) {
  return <><MatchedTablesDisclosure id="matches" label="Matched tables" headerClass="table-rule-result"
    open={open} onToggle={() => {}} tables={count === undefined ? undefined
      : Array.from({ length: count }, (_, index) => ({ namespace: "db", name: `t${index}` }))} />
    <button type="button">Following control</button></>;
}

it.each([0, 1, 3])("fits %s results on opening and hides the unnecessary height action", count => {
  geometry();
  const view = render(disclosure(count));
  const region = view.getByRole("region");
  expect(region.style.height).toBe(`${Math.max(1, count) * 26 + 2}px`);
  expect(view.queryByRole("button", { name: "Show all" })).toBeNull();
  const action = view.container.querySelector<HTMLButtonElement>(".table-matches-height-toggle")!;
  expect(action.style.visibility).toBe("hidden");
  expect(action.disabled).toBe(true);
});

it("keeps a short open list and following controls fixed through pending and longer results", () => {
  geometry();
  const view = render(disclosure(1));
  const region = view.getByRole("region");
  const following = view.getByRole("button", { name: "Following control" });
  const action = view.container.querySelector(".table-matches-height-toggle");
  following.focus();
  for (const count of [undefined, 40, 0]) {
    view.rerender(disclosure(count));
    expect(view.getByRole("region")).toBe(region);
    expect(region.style.height).toBe("28px");
    expect(view.getByRole("button", { name: "Following control" })).toBe(following);
    expect(document.activeElement).toBe(following);
    expect(view.container.querySelector(".table-matches-height-toggle")).toBe(action);
  }
});

it("caps long lists, fits on request, restores the compact height and remeasures only on reopening", () => {
  geometry();
  const view = render(disclosure(40));
  const region = view.getByRole("region");
  expect(region.style.height).toBe("140px");
  const action = view.getByRole("button", { name: "Show all" });
  fireEvent.click(action);
  expect(region.style.height).toBe("1042px");
  expect(view.getByRole("button", { name: "Restore height" })).toBe(action);
  view.rerender(disclosure(1));
  expect(region.style.height).toBe("1042px");
  action.focus();
  fireEvent.click(action);
  expect(region.style.height).toBe("140px");
  expect(view.queryByRole("button", { name: "Show all" })).toBeNull();
  expect(document.activeElement).toBe(view.getByRole("button", { name: "Matched tables 1" }));
  view.rerender(disclosure(1, false));
  view.rerender(disclosure(1));
  expect(view.getByRole("region").style.height).toBe("28px");
});
