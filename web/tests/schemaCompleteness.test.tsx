// @vitest-environment jsdom

import { cleanup } from "@testing-library/preact";
import { afterEach, describe, expect, it } from "vitest";

import { productionWidgetRegistry } from "../src/features/formWidgetRegistry";
import { compileSchema } from "../src/schema/compiler";
import { SchemaForm } from "../src/schema/SchemaForm";
import { render } from "./support/render";

afterEach(cleanup);

describe("schema completeness feedback", () => {
  it("highlights an invalid optional value that blocks delivery actions", () => {
    const node = compileSchema({
      type: "object",
      properties: {
        database: { type: "string" },
        optional_port: { type: "integer", minimum: 1 },
      },
      required: ["database"],
    });
    const view = render(
      <SchemaForm
        node={node}
        value={{ database: "db1", optional_port: 0 }}
        showRequiredErrors
        onChange={() => undefined}
      />,
    );

    const control = view.container.querySelector("#field---optional_port");
    const field = control?.closest(".form-row");
    expect(field?.classList.contains("required-incomplete")).toBe(true);
    expect(field?.classList.contains("required-missing")).toBe(true);
  });

  it("does not blame a visible ancestor for an invalid hidden subtree", () => {
    const node = compileSchema(
      {
        type: "object",
        properties: {
          connection: { type: "string", title: "Registry URL" },
          projection: {
            type: "object",
            "x-ui": { widget: "hidden" },
            properties: {
              columns: {
                type: "array",
                minItems: 1,
                items: { type: "string" },
              },
            },
            required: ["columns"],
          },
        },
        required: ["connection", "projection"],
      },
      productionWidgetRegistry,
    );
    const view = render(
      <SchemaForm
        node={node}
        value={{
          connection: "https://registry.example",
          projection: { columns: [] },
        }}
        showRequiredErrors
        onChange={() => undefined}
      />,
    );

    expect(view.container.querySelector(".required-incomplete")).toBeNull();
    expect(
      view.getByLabelText("Registry URL").classList.contains("required-error-control"),
    ).toBe(false);
  });
});
