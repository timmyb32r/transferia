import type { JsonValue } from "../json";
import { SelectControl } from "../ui/SelectControl";
import { branchMatches, createValue, type CompiledNode } from "./compiler";
import type { NodeEditorComponent } from "./editorTypes";
import { hasEditableContent, type WidgetRegistry } from "./widgetRegistry";
import type { VariantUi } from "./SchemaForm";

export function UnionNodeEditor({
  node,
  value,
  disabled,
  path,
  controlId,
  variantUi,
  widgets,
  NodeEditor,
  onChange,
}: {
  node: Extract<CompiledNode, { kind: "union" }>;
  value: JsonValue;
  disabled: boolean;
  path: string;
  controlId?: string | undefined;
  variantUi: VariantUi;
  widgets: WidgetRegistry;
  NodeEditor: NodeEditorComponent;
  onChange: (value: JsonValue) => void;
}) {
  const selected = node.branches.findIndex((branch) =>
    branchMatches(branch, value),
  );
  const widget = node.xUi.widget;
  const action = widget === undefined ? undefined : variantUi.actions?.[widget];
  const selectionOnly =
    node.xUi.defer_variant_details === true ||
    (widget !== undefined && variantUi.selectionOnly?.includes(widget) === true);
  return (
    <div class="union-editor">
      <div class={action !== undefined ? "parser-selector-row" : undefined}>
        <SelectControl
          id={controlId}
          value={selected < 0 ? "" : String(selected)}
          disabled={disabled}
          placeholder="Not selected"
          options={node.branches.map((branch, index) => ({
            value: String(index),
            label: branch.label,
          }))}
          onChange={(raw) => {
            if (raw === "") {
              onChange(null);
              return;
            }
            const branch = node.branches[Number(raw)];
            if (branch === undefined) return;
            const created = branch.constant ?? createValue(branch.node);
            onChange(
              branch.discriminator !== undefined &&
                typeof created === "object" &&
                created !== null &&
                !Array.isArray(created)
                ? {
                    ...created,
                    [branch.discriminator.key]: branch.discriminator.value,
                  }
                : created,
            );
            if (widget !== undefined) variantUi.onSelected?.(widget);
          }}
        />
        {action}
      </div>
      {!selectionOnly &&
        selected >= 0 &&
        node.branches[selected]!.constant === undefined &&
        hasEditableContent(node.branches[selected]!.node, widgets) && (
          <div class="nested-section">
            <NodeEditor
              node={node.branches[selected]!.node}
              value={value}
              disabled={disabled}
              path={`${path}/branch-${selected}`}
              onChange={onChange}
            />
          </div>
        )}
    </div>
  );
}
