import type { ComponentChildren } from "preact";

import type { JsonValue } from "../json";
import { SelectControl } from "../ui/SelectControl";
import { branchMatches, createValue, type CompiledNode } from "./compiler";
import type { NodeEditorComponent } from "./editorTypes";
import { revealDetails } from "./revealDetails";
import { hasEditableContent, type WidgetRegistry } from "./widgetRegistry";

export function UnionNodeEditor({
  node,
  value,
  disabled,
  path,
  controlId,
  parserSelectionOnly,
  serializerSelectionOnly,
  parserAction,
  widgets,
  NodeEditor,
  onChange,
}: {
  node: Extract<CompiledNode, { kind: "union" }>;
  value: JsonValue;
  disabled: boolean;
  path: string;
  controlId?: string | undefined;
  parserSelectionOnly: boolean;
  serializerSelectionOnly: boolean;
  parserAction?: ComponentChildren;
  widgets: WidgetRegistry;
  NodeEditor: NodeEditorComponent;
  onChange: (value: JsonValue) => void;
}) {
  const selected = node.branches.findIndex((branch) =>
    branchMatches(branch, value),
  );
  return (
    <div class="union-editor">
      <div
        class={
          node.xUi.widget === "parser" && parserAction
            ? "parser-selector-row"
            : undefined
        }
      >
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
            onChange(branch.constant ?? createValue(branch.node));
            if (node.xUi.widget === "parser")
              revealDetails(".parser-details-card");
            if (node.xUi.widget === "serializer")
              revealDetails(".serializer-details-card");
          }}
        />
        {node.xUi.widget === "parser" && parserAction}
      </div>
      {(!parserSelectionOnly || node.xUi.widget !== "parser") &&
        (!serializerSelectionOnly || node.xUi.widget !== "serializer") &&
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
