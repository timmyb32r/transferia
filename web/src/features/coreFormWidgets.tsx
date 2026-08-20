import { CompactArrayEditor } from "../schema/CompactArrayEditor";
import {
  ByteSizeInput,
  PasswordInput,
  SystemColumnsEditor,
} from "../schema/controls";
import type { WidgetPlugin } from "../schema/widgetPlugin";
import { Disclosure } from "../ui/Disclosure";
import { FormField } from "../ui/FormField";
import { humanize } from "../schema/compiler";

export const coreFormWidgets: readonly WidgetPlugin[] = [
  {
    name: "byte_size",
    kinds: ["number", "string"],
    renderer: "node",
    node: (context) => {
      if (context.node.kind === "string")
        return (
          <input
            id={context.controlId}
            type="text"
            autoComplete="off"
            value={typeof context.value === "string" ? context.value : ""}
            disabled={context.disabled}
            onInput={(event) => context.onChange(event.currentTarget.value)}
          />
        );
      if (context.node.kind !== "number") return null;
      return (
        <ByteSizeInput
          id={context.controlId}
          value={typeof context.value === "number" ? context.value : null}
          disabled={context.disabled}
          onChange={context.onChange}
        />
      );
    },
  },
  {
    name: "compact_array",
    kinds: ["array"],
    renderer: "property",
    property: (context, services) => {
      if (context.node.kind !== "array") return null;
      return (
        <FormField
          label={context.node.title ?? humanize(context.name)}
          optional={!context.required}
          class="form-row-wide compact-array-field"
        >
          <CompactArrayEditor
            node={context.node}
            value={Array.isArray(context.value) ? context.value : []}
            disabled={context.disabled}
            showPartitionRanges={context.showPartitionRanges ?? true}
            onChange={context.onChange}
            NodeEditor={services.NodeEditor}
          />
        </FormField>
      );
    },
  },
  {
    name: "password",
    kinds: ["string"],
    renderer: "node",
    connectionActionAnchor: "after",
    node: (context) =>
      context.node.kind === "string" ? (
        <PasswordInput
          id={context.controlId}
          value={typeof context.value === "string" ? context.value : ""}
          disabled={context.disabled}
          onChange={context.onChange}
        />
      ) : null,
  },
  {
    name: "sql",
    kinds: ["string"],
    renderer: "node",
    node: (context) =>
      context.node.kind === "string" ? (
        <textarea
          id={context.controlId}
          autoComplete="off"
          class="sql-input"
          spellcheck={false}
          value={typeof context.value === "string" ? context.value : ""}
          disabled={context.disabled}
          onInput={(event) => context.onChange(event.currentTarget.value)}
        />
      ) : null,
  },
  {
    name: "system_columns",
    kinds: ["object"],
    renderer: "both",
    node: (context) =>
      context.node.kind === "object" ? (
        <SystemColumnsEditor
          node={context.node}
          value={context.value}
          disabled={context.disabled}
          onChange={context.onChange}
        />
      ) : null,
    property: (context, services) => (
      <Disclosure
        label="Add system columns"
        class="system-columns unified-system-columns"
      >
        <services.NodeEditor
          node={context.node}
          value={context.effectiveValue}
          disabled={context.disabled}
          onChange={context.onChange}
        />
      </Disclosure>
    ),
  },
  ...genericWidgets(),
];

function genericWidgets(): WidgetPlugin[] {
  return [
    {
      name: "column_keys",
      kinds: ["array"],
      renderer: "generic",
      hidden: true,
    },
    { name: "duration", kinds: ["string"], renderer: "generic" },
    {
      name: "hidden",
      kinds: [
        "string",
        "number",
        "boolean",
        "object",
        "array",
        "union",
        "nullable",
      ],
      renderer: "generic",
      hidden: true,
    },
    {
      name: "parser",
      kinds: ["object", "union"],
      renderer: "generic",
      wide: true,
      hideDescription: true,
      controlWidth: "parser",
      connectionActionAnchor: "before",
    },
    { name: "select", kinds: ["string"], renderer: "generic" },
    {
      name: "serializer",
      kinds: ["object", "union"],
      renderer: "generic",
      wide: true,
      controlWidth: "parser",
    },
  ];
}
