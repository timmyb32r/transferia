// @vitest-environment jsdom

import { cleanup, fireEvent, render } from "@testing-library/preact";
import { useState } from "preact/hooks";
import { afterEach, describe, expect, it } from "vitest";

import {
  AutofillResistantInput,
  AutofillResistantSelect,
  AutofillResistantTextarea,
  useOpaqueFieldName,
} from "../src/ui/AutofillResistantField";

afterEach(cleanup);

describe("autofill-resistant fields", () => {
  it.each(["text", "search", "password", "number", "email", "url", "tel", "textarea", "select"])(
    "preserves the %s field DOM identity and focus across controlled edits",
    (type) => {
      function Editor({ revision }: { revision: number }) {
        const [value, setValue] = useState("");
        const props = {
          "aria-label": "Field",
          value,
          onInput: (event: Event) => setValue((event.currentTarget as HTMLInputElement).value),
        };
        return <div data-revision={revision}>
          {type === "textarea" ? <AutofillResistantTextarea {...props} />
            : type === "select" ? <AutofillResistantSelect {...props}>
              <option value="">None</option><option value="1">One</option><option value="12">Twelve</option>
            </AutofillResistantSelect>
            : <AutofillResistantInput {...props} type={type} />}
        </div>;
      }
      const view = render(<Editor revision={0} />);
      const field = view.getByLabelText("Field");
      field.focus();
      for (const [revision, value] of ["1", "12"].entries()) {
        fireEvent.input(field, { target: { value } });
        view.rerender(<Editor revision={revision + 1} />);
        expect(view.getByLabelText("Field")).toBe(field);
        expect(document.activeElement).toBe(field);
        expect((field as HTMLInputElement).value).toBe(value);
      }
    },
  );

  it.each(["text", "search", "password", "number", "checkbox"])(
    "protects a native %s input",
    (type) => {
      const attemptedOverrides: Record<string, string> = {
        autoComplete: "email",
        "data-form-type": "username",
      };
      const view = render(
        <AutofillResistantInput
          aria-label={`${type} field`}
          type={type}
          {...attemptedOverrides}
        />,
      );

      expectProtected(view.getByLabelText(`${type} field`));
    },
  );

  it("protects textareas and native selects", () => {
    const view = render(
      <div>
        <AutofillResistantTextarea aria-label="Notes" />
        <AutofillResistantSelect aria-label="Choice">
          <option value="one">One</option>
        </AutofillResistantSelect>
      </div>,
    );

    expectProtected(view.getByLabelText("Notes"));
    expectProtected(view.getByLabelText("Choice"), false);
  });

  it("keeps an opaque field name stable only for the current mount", () => {
    const first = render(<AutofillResistantInput aria-label="Field" />);
    const initialName = (first.getByLabelText("Field") as HTMLInputElement)
      .name;

    first.rerender(<AutofillResistantInput aria-label="Field" value="next" />);
    expect((first.getByLabelText("Field") as HTMLInputElement).name).toBe(
      initialName,
    );
    first.unmount();

    const second = render(<AutofillResistantInput aria-label="Field" />);
    expect((second.getByLabelText("Field") as HTMLInputElement).name).not.toBe(
      initialName,
    );
  });

  it("shares one opaque name inside an explicit radio group", () => {
    function RadioGroup() {
      const groupName = useOpaqueFieldName();
      return (
        <div>
          <AutofillResistantInput
            aria-label="First"
            type="radio"
            opaqueGroupName={groupName}
          />
          <AutofillResistantInput
            aria-label="Second"
            type="radio"
            opaqueGroupName={groupName}
          />
        </div>
      );
    }

    const view = render(<RadioGroup />);
    const first = view.getByLabelText("First") as HTMLInputElement;
    const second = view.getByLabelText("Second") as HTMLInputElement;
    expect(first.name).toBe(second.name);
    expect(first.name).toMatch(/^tf-/);
  });
});

function expectProtected(element: HTMLElement, textEntry = true) {
  expect(element.getAttribute("autocomplete")).toBe("none");
  expect(element.getAttribute("data-1p-ignore")).toBe("true");
  expect(element.getAttribute("data-lpignore")).toBe("true");
  expect(element.getAttribute("data-form-type")).toBe("other");
  expect(element.getAttribute("name")).toMatch(/^tf-/);
  if (!textEntry) return;
  expect(element.getAttribute("autocapitalize")).toBe("off");
  expect(element.getAttribute("autocorrect")).toBe("off");
  expect(element.getAttribute("spellcheck")).toBe("false");
}
