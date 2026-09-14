import type { JsonValue } from "../json";
import { SelectControl } from "../ui/SelectControl";
import {
  branchMatches,
  materializeBranch,
  type CompiledNode,
} from "./compiler";
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
  const options = node.branches.map((branch, index) => ({ branch, index }));
  const branch = node.branches[selected];
  const details = !selectionOnly && branch !== undefined && branch.constant === undefined &&
    hasEditableContent(branch.node, widgets) ? (
      <NodeEditor node={branch.node} value={value} disabled={disabled}
        path={`${path}/branch-${selected}`} onChange={onChange} />
    ) : null;
  const performanceOnly = branch?.node.kind === "object" && Object.values(branch.node.properties)
    .filter(child => !widgets.isHidden(child)).every(child => child.xUi.section === "performance");
  return (
    <div class="union-editor">
      <div class={action !== undefined ? "parser-selector-row" : undefined}>
        <SelectControl
          id={controlId}
          value={selected < 0 ? "" : String(selected)}
          disabled={disabled}
          placeholder="Not selected"
          options={options.map(({ branch, index }) => ({
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
            onChange(materializeBranch(branch));
            if (widget !== undefined) variantUi.onSelected?.(widget);
          }}
        />
        {action}
      </div>
      {performanceOnly ? details : details && <div class="nested-section">{details}</div>}
    </div>
  );
}
