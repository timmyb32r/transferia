import type { JsonValue } from "../json";
import { branchMatches, type CompiledNode } from "./compiler";
import { draftValue } from "./draft";
import type { NodeEditorComponent } from "./editorTypes";
import { hasEditableContent, type WidgetRegistry } from "./widgetRegistry";
import { isObject } from "./value";

export function VariantDetailsCard({
  node,
  value,
  disabled,
  widget,
  bridgeClass,
  cardClass,
  widgets,
  NodeEditor,
  onChange,
}: {
  node: CompiledNode;
  value: JsonValue;
  disabled: boolean;
  widget: "parser" | "serializer";
  bridgeClass: string;
  cardClass: string;
  widgets: WidgetRegistry;
  NodeEditor: NodeEditorComponent;
  onChange: (value: JsonValue) => void;
}) {
  if (node.kind !== "object") return null;
  const variantEntry = Object.entries(node.properties).find(
    ([, child]) => child.xUi.widget === widget,
  );
  if (variantEntry === undefined) return null;
  const [name, variantNode] = variantEntry;
  if (variantNode.kind !== "union") return null;
  const object = isObject(value) ? value : {};
  const variantValue = object[name];
  const selected =
    variantValue === undefined
      ? undefined
      : variantNode.branches.find((branch) =>
          branchMatches(branch, variantValue),
        );
  if (
    selected === undefined ||
    selected.constant !== undefined ||
    !hasEditableContent(selected.node, widgets)
  )
    return null;
  return (
    <>
      <div class={bridgeClass} aria-hidden="true" />
      <section class={cardClass} tabindex={-1}>
        <div class="section-heading">
          <h2>{selected.label} settings</h2>
        </div>
        <NodeEditor
          node={selected.node}
          value={draftValue(selected.node, variantValue)}
          disabled={disabled}
          onChange={(next) => onChange({ ...object, [name]: next })}
        />
      </section>
    </>
  );
}
