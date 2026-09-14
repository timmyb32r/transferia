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
  cardClass,
  contentClass,
  widgets,
  NodeEditor,
  onChange,
}: {
  node: CompiledNode;
  value: JsonValue;
  disabled: boolean;
  widget: string;
  cardClass: string;
  contentClass: (node: CompiledNode) => string;
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
  const detailsNode = selected?.node.kind === "object"
    ? { ...selected.node, properties: Object.fromEntries(Object.entries(selected.node.properties)
        .filter(([, child]) => child.xUi.section !== "performance")) }
    : selected?.node;
  if (
    selected === undefined ||
    selected.constant !== undefined ||
    detailsNode === undefined ||
    !hasEditableContent(detailsNode, widgets)
  )
    return null;
  return (
    <>
      <section class={cardClass} tabindex={-1}>
        <div class={contentClass(selected.node)}>
          <div class="section-heading">
            <h2>{selected.label} settings</h2>
          </div>
          <NodeEditor
            node={detailsNode}
            value={draftValue(selected.node, variantValue)}
            disabled={disabled}
            onChange={(next) => onChange({ ...object, [name]: next })}
          />
        </div>
      </section>
    </>
  );
}
