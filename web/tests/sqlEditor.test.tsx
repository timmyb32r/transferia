// @vitest-environment jsdom
import { cleanup, fireEvent, render } from "@testing-library/preact";
import { useState } from "preact/hooks";
import { afterEach, expect, it, vi } from "vitest";
import { SqlEditor } from "../src/ui/SqlEditor";
import { SyntaxHighlight } from "../src/ui/SyntaxHighlight";

afterEach(cleanup);

it("colors SQL keywords, DataFusion function calls, literals and comments without changing text", () => {
  const sql = `WITH t AS (SELECT date_bin(INTERVAL '1 hour', ts), arrow_cast(x, 'Int64'), 1.5e-3 FROM input)
SELECT "select", 'it''s SELECT', $$FROM$$, true, NULL FROM t -- WHERE
/* SELECT */ WHERE x IS NOT NULL`;
  const view = render(<SyntaxHighlight language="sql" value={sql} />);
  expect(view.container.textContent).toBe(sql);
  const tokens = (kind: string) => Array.from(view.container.querySelectorAll(`.syntax-${kind}`), token => token.textContent);
  expect(tokens("keyword")).toContain("WITH");
  expect(tokens("function")).toEqual(["date_bin", "arrow_cast"]);
  expect(tokens("string")).toContain("'it''s SELECT'");
  expect(tokens("string")).toContain("$$FROM$$");
  expect(tokens("identifier")).toContain('"select"');
  expect(tokens("number")).toContain("1.5e-3");
  expect(tokens("comment")).toEqual(["-- WHERE", "/* SELECT */"]);
});

it.each(["", "SELECT\n", "SELECT 'unfinished", 'SELECT "unfinished', "/* unfinished", "SELECT '<img src=x>'", "SELECT колонка FROM input"])(
  "preserves incomplete and arbitrary SQL verbatim: %s", value => {
    const view = render(<SyntaxHighlight language="sql" value={value} />);
    expect(view.container.textContent).toBe(value);
    expect(view.container.querySelector("img")).toBeNull();
  });

it("keeps native input, focus, selection and synchronized scroll through highlighting updates", () => {
  const changed = vi.fn();
  function Editor() {
    const [value, setValue] = useState("SELECT * FROM input");
    return <><SqlEditor value={value} disabled={false} onChange={next => { changed(next); setValue(next); }} />
      <button>Following control</button></>;
  }
  const view = render(<Editor />);
  const input = view.getByRole("textbox", { name: "SQL over table input" }) as HTMLTextAreaElement;
  const highlight = view.container.querySelector("pre")!;
  const following = view.getByRole("button");
  expect(highlight.getAttribute("aria-hidden")).toBe("true");
  expect(input.getAttribute("autocomplete")).toBe("none");
  input.focus();
  input.scrollTop = 48;
  input.scrollLeft = 140;
  fireEvent.scroll(input);
  expect(highlight.scrollTop).toBe(48);
  expect(highlight.scrollLeft).toBe(140);
  input.value = "SELECT\t42\nFROM input\n";
  input.setSelectionRange(7, 9);
  fireEvent.input(input);
  expect(changed).toHaveBeenCalledWith("SELECT\t42\nFROM input\n");
  expect(highlight.textContent).toBe(`${input.value}\n`);
  expect(view.getByRole("textbox")).toBe(input);
  expect(document.activeElement).toBe(input);
  expect(input.selectionStart).toBe(7);
  expect(input.selectionEnd).toBe(9);
  expect(view.getByRole("button")).toBe(following);
});

it("keeps highlighting visible in a disabled SQL editor", () => {
  const view = render(<SqlEditor value="SELECT 42" disabled onChange={vi.fn()} />);
  expect((view.getByRole("textbox") as HTMLTextAreaElement).disabled).toBe(true);
  expect(view.container.querySelector(".syntax-keyword")?.textContent).toBe("SELECT");
});
