import type { JsonValue } from "../json";
import { Button } from "../ui/Button";
import { TrashIcon } from "../ui/icons";
import { createValue, humanize, type CompiledNode } from "./compiler";
import { useStableRowIds } from "./controls";
import { draftValue } from "./draft";
import type { NodeEditorComponent } from "./editorTypes";
import { isObject } from "./value";
import { useWidgetRegistry } from "./widgetRegistry";

export function CompactArrayEditor({
  node,
  value,
  disabled,
  showPartitionRanges,
  onChange,
  NodeEditor,
}: {
  node: Extract<CompiledNode, { kind: "array" }>;
  value: JsonValue[];
  disabled: boolean;
  showPartitionRanges: boolean;
  onChange: (value: JsonValue) => void;
  NodeEditor: NodeEditorComponent;
}) {
  const widgets = useWidgetRegistry();
  const rowIds = useStableRowIds(value.length);
  const fields =
    node.item.kind === "object"
      ? Object.entries(node.item.properties).filter(
          ([, child]) =>
            !widgets.isHidden(child) &&
            (showPartitionRanges || child.xUi.widget !== "partition_ranges"),
        )
      : [];
  const singular =
    typeof node.xUi.item_label === "string" ? node.xUi.item_label : "item";
  const updateItem = (index: number, next: JsonValue) => {
    const items = [...value];
    items[index] = next;
    onChange(items);
  };
  return (
    <div class="compact-array-editor">
      <div class="table-shell">
        <table class="config-table compact-array-table">
          <thead>
            <tr>
              <th class="row-number" aria-label="Row" />
              {fields.length > 0 ? (
                fields.map(([field, child]) => (
                  <th key={field}>
                    {child.title ?? humanize(field)}
                    {child.xUi.widget === "partition_ranges" && (
                      <small class="optional">(optional)</small>
                    )}
                  </th>
                ))
              ) : (
                <th>{humanize(singular)}</th>
              )}
              <th class="actions-column">Actions</th>
            </tr>
          </thead>
          <tbody>
            {value.map((item, index) => {
              const object = isObject(item) ? item : {};
              return (
                <tr class="config-table-row" key={rowIds.values[index]}>
                  <td class="row-number">{index + 1}</td>
                  {fields.length > 0 ? (
                    fields.map(([field, child]) => (
                      <td key={field}>
                        <label
                          class="visually-hidden"
                          for={`compact-${rowIds.values[index]}-${field}`}
                        >
                          {child.title ?? humanize(field)} row {index + 1}
                        </label>
                        <NodeEditor
                          node={child}
                          value={draftValue(child, object[field])}
                          disabled={disabled}
                          path={`#/compact/${rowIds.values[index]}/${field}`}
                          controlId={`compact-${rowIds.values[index]}-${field}`}
                          onChange={(next) =>
                            updateItem(index, { ...object, [field]: next })
                          }
                        />
                      </td>
                    ))
                  ) : (
                    <td>
                      <label
                        class="visually-hidden"
                        for={`compact-${rowIds.values[index]}-value`}
                      >
                        {humanize(singular)} row {index + 1}
                      </label>
                      <NodeEditor
                        node={node.item}
                        value={item}
                        disabled={disabled}
                        path={`#/compact/${rowIds.values[index]}/value`}
                        controlId={`compact-${rowIds.values[index]}-value`}
                        onChange={(next) => updateItem(index, next)}
                      />
                    </td>
                  )}
                  <td class="actions-column">
                    <Button
                      shape="row"
                      class="danger"
                      title={`Remove ${singular}`}
                      aria-label={`Remove ${singular} ${index + 1}`}
                      disabled={disabled}
                      onClick={() => {
                        rowIds.remove(index);
                        onChange(
                          value.filter((_, itemIndex) => itemIndex !== index),
                        );
                      }}
                    >
                      <TrashIcon />
                    </Button>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      <Button
        shape="add-row"
        disabled={disabled}
        onClick={() => {
          rowIds.insert(value.length);
          onChange([...value, createValue(node.item)]);
        }}
      >
        + Add {singular}
      </Button>
    </div>
  );
}
