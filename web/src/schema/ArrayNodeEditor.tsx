import type { JsonValue } from "../json";
import { Button } from "../ui/Button";
import { TrashIcon } from "../ui/icons";
import { createValue, type CompiledNode } from "./compiler";
import type { NodeEditorComponent } from "./editorTypes";

export function ArrayNodeEditor({
  node,
  value,
  disabled,
  path,
  NodeEditor,
  onChange,
}: {
  node: Extract<CompiledNode, { kind: "array" }>;
  value: JsonValue;
  disabled: boolean;
  path: string;
  NodeEditor: NodeEditorComponent;
  onChange: (value: JsonValue) => void;
}) {
  const items = Array.isArray(value) ? value : [];
  return (
    <div class="array-editor">
      {items.map((item, index) => (
        <div class="array-row" key={index}>
          <span class="array-index">{index + 1}</span>
          <div class="array-value">
            <NodeEditor
              node={node.item}
              value={item}
              disabled={disabled}
              path={`${path}/${index}`}
              onChange={(next) => {
                const copy = [...items];
                copy[index] = next;
                onChange(copy);
              }}
            />
          </div>
          <Button variant="plain"
            shape="icon"
            class="danger"
            title="Remove"
            disabled={disabled}
            onClick={() =>
              onChange(items.filter((_, itemIndex) => itemIndex !== index))
            }
          >
            <TrashIcon />
          </Button>
        </div>
      ))}
      <Button
        shape="icon"
        class="add"
        title="Add"
        disabled={disabled}
        onClick={() => onChange([...items, createValue(node.item)])}
      >
        +
      </Button>
    </div>
  );
}
