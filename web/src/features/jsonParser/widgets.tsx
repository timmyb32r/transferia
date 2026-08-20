import type { WidgetPlugin } from "../../schema/widgetPlugin";
import { stringArray } from "../../schema/value";
import { ColumnMappingsEditor } from "./ColumnMappingsEditor";
import { JsonParserEditor } from "./JsonParserEditor";
import { isComplete } from "../../schema/compiler";

export const jsonParserWidgets: readonly WidgetPlugin[] = [
  {
    name: "json_parser",
    kinds: ["object"],
    renderer: "node",
    node: (context, services) =>
      context.node.kind === "object" ? (
        <JsonParserEditor
          {...context}
          node={context.node}
          NodeEditor={services.NodeEditor}
          PropertyEditor={services.PropertyEditor}
        />
      ) : null,
  },
  {
    name: "parser_common",
    kinds: ["object"],
    renderer: "property",
    property: (context, services) => (
      <section class="parser-common-section">
        <h3>{context.node.title ?? "Parser settings"}</h3>
        <services.NodeEditor
          node={context.node}
          value={context.effectiveValue}
          disabled={context.disabled}
          onChange={context.onChange}
        />
      </section>
    ),
  },
  {
    name: "column_mappings",
    kinds: ["array"],
    renderer: "property",
    property: (context, services) => {
      if (
        context.node.kind !== "array" ||
        context.parentValue === undefined ||
        context.onParentChange === undefined
      )
        return null;
      return (
        <ColumnMappingsEditor
          node={context.node.item}
          value={Array.isArray(context.value) ? context.value : []}
          incomplete={
            context.required && !isComplete(context.node, context.value)
          }
          keys={stringArray(context.parentValue.keys)}
          additionalKeyOptions={[]}
          disabled={context.disabled}
          NodeEditor={services.NodeEditor}
          PropertyEditor={services.PropertyEditor}
          onChange={(columns, keys) =>
            context.onParentChange?.({
              ...context.parentValue,
              [context.name]: columns,
              keys,
            })
          }
        />
      );
    },
  },
];
