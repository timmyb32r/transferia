import { Fragment, type ComponentType } from "preact";
import { useEffect, useRef, useState } from "preact/hooks";

import type { JsonObject, JsonValue } from "../json";
import { MultiSelectControl } from "../ui/SelectControl";
import { ColumnActions } from "./ColumnActions";
import { createValue, type CompiledNode } from "./compiler";
import {
  DragHandleIcon,
  IndeterminateCheckbox,
  TrashIcon,
  useStableRowIds,
} from "./controls";
import { closestArrowType, isStringArrowType } from "./formLogic";
import {
  isObject,
  jsonValuesEqual,
  uniqueStrings,
} from "./value";

interface NodeEditorProps {
  node: CompiledNode;
  value: JsonValue;
  disabled?: boolean;
  onChange: (value: JsonValue) => void;
  path?: string;
  controlId?: string;
}

interface PropertyEditorProps {
  name: string;
  node: CompiledNode;
  required: boolean;
  value: JsonValue | undefined;
  disabled: boolean;
  showPartitionRanges?: boolean;
  onChange: (value: JsonValue) => void;
  path?: string;
}

function createColumnDragPreview(
  row: HTMLTableRowElement,
  dataTransfer: DataTransfer,
  clientX: number,
  clientY: number,
): HTMLTableElement {
  const bounds = row.getBoundingClientRect();
  const table = document.createElement("table");
  const body = document.createElement("tbody");
  const clone = row.cloneNode(true) as HTMLTableRowElement;
  const sourceInputs = row.querySelectorAll<HTMLInputElement>("input");
  const clonedInputs = clone.querySelectorAll<HTMLInputElement>("input");

  sourceInputs.forEach((input, index) => {
    const cloned = clonedInputs[index];
    if (cloned === undefined) return;
    cloned.value = input.value;
    cloned.checked = input.checked;
  });
  clone.classList.remove("dragged", "drag-before", "drag-after");
  table.className = "config-table column-table column-drag-preview";
  table.style.width = `${bounds.width}px`;
  body.append(clone);
  table.append(body);
  document.body.append(table);
  dataTransfer.setDragImage(
    table,
    Math.max(0, clientX - bounds.left),
    Math.max(0, clientY - bounds.top),
  );
  return table;
}

export function ColumnMappingsEditor({
  node,
  value,
  keys,
  additionalKeyOptions,
  systemColumns,
  disabled,
  onChange,
  NodeEditor,
  PropertyEditor,
}: {
  node: CompiledNode;
  value: JsonValue[];
  keys: string[];
  additionalKeyOptions: string[];
  systemColumns?: {
    node: CompiledNode;
    value: JsonValue | undefined;
    onChange: (value: JsonValue) => void;
  };
  disabled: boolean;
  onChange: (columns: JsonValue[], keys: string[]) => void;
  NodeEditor: ComponentType<NodeEditorProps>;
  PropertyEditor: ComponentType<PropertyEditorProps>;
}) {
  const [expandedSettings, setExpandedSettings] = useState<Set<number>>(
    () => new Set(),
  );
  const [selectedRows, setSelectedRows] = useState<Set<number>>(
    () => new Set(),
  );
  const [systemColumnsOpen, setSystemColumnsOpen] = useState(false);
  const [draggedRow, setDraggedRow] = useState<number>();
  const [dragTargetSlot, setDragTargetSlot] = useState<number>();
  const dragPreview = useRef<HTMLTableElement | null>(null);
  const rowIds = useStableRowIds(value.length);
  const removeDragPreview = () => {
    dragPreview.current?.remove();
    dragPreview.current = null;
  };
  useEffect(() => removeDragPreview, []);
  useEffect(() => {
    setSelectedRows((current) => {
      const next = new Set(
        [...current].filter((index) => index < value.length),
      );
      return next.size === current.size ? current : next;
    });
  }, [value.length]);
  if (node.kind !== "object")
    return (
      <NodeEditor
        node={{ kind: "array", item: node, xUi: {} }}
        value={value}
        disabled={disabled}
        onChange={(next) => onChange(Array.isArray(next) ? next : [], keys)}
      />
    );
  const updateColumn = (index: number, next: JsonObject) => {
    const previous = isObject(value[index]) ? value[index] : {};
    const oldName =
      typeof previous.column_name === "string" ? previous.column_name : "";
    const newName =
      typeof next.column_name === "string" ? next.column_name : "";
    const oldJsonType = previous.json_data_type;
    if (
      typeof next.json_data_type === "string" &&
      next.json_data_type !== oldJsonType
    ) {
      next = {
        ...next,
        arrow_type: closestArrowType(next.json_data_type),
      };
    }
    if (!isStringArrowType(next.arrow_type))
      next = { ...next, low_cardinality: false };
    if (
      newName !== oldName &&
      (previous.jsonpath === "" || previous.jsonpath === `$.${oldName}`)
    )
      next = { ...next, jsonpath: newName === "" ? "" : `$.${newName}` };
    const columns = [...value];
    columns[index] = next;
    const nextKeys =
      newName === oldName
        ? keys
        : keys.map((key) => (key === oldName ? newName : key)).filter(Boolean);
    onChange(columns, nextKeys);
  };
  const toggleSettings = (index: number) =>
    setExpandedSettings((current) => {
      const next = new Set(current);
      if (next.has(index)) next.delete(index);
      else next.add(index);
      return next;
    });
  const duplicateColumn = (index: number) => {
    const columns = [...value];
    columns.splice(index + 1, 0, structuredClone(value[index]!));
    rowIds.insert(index + 1);
    setExpandedSettings(new Set());
    setSelectedRows(new Set());
    onChange(columns, keys);
  };
  const deleteColumn = (index: number, name: string) => {
    rowIds.remove(index);
    setExpandedSettings(new Set());
    setSelectedRows(new Set());
    onChange(
      value.filter((_, itemIndex) => itemIndex !== index),
      keys.filter((key) => key !== name),
    );
  };
  const toggleRowSelection = (index: number) =>
    setSelectedRows((current) => {
      const next = new Set(current);
      if (next.has(index)) next.delete(index);
      else next.add(index);
      return next;
    });
  const selectAllRows = (selected: boolean) =>
    setSelectedRows(
      selected ? new Set(value.map((_, index) => index)) : new Set(),
    );
  const deleteSelectedRows = () => {
    const deletedNames = new Set(
      [...selectedRows].flatMap((index) => {
        const column = isObject(value[index]) ? value[index] : {};
        return typeof column.column_name === "string" &&
          column.column_name !== ""
          ? [column.column_name]
          : [];
      }),
    );
    setExpandedSettings(new Set());
    setSelectedRows(new Set());
    rowIds.retain((_, index) => !selectedRows.has(index));
    onChange(
      value.filter((_, index) => !selectedRows.has(index)),
      keys.filter((key) => !deletedNames.has(key)),
    );
  };
  const moveColumn = (from: number, to: number) => {
    setDraggedRow(undefined);
    setDragTargetSlot(undefined);
    if (from === to || value[from] === undefined || value[to] === undefined)
      return;
    const columns = [...value];
    const [column] = columns.splice(from, 1);
    columns.splice(to, 0, column!);
    rowIds.move(from, to);
    setExpandedSettings(new Set());
    setSelectedRows(new Set());
    onChange(columns, keys);
  };
  const moveColumnToSlot = (from: number, slot: number) => {
    setDraggedRow(undefined);
    setDragTargetSlot(undefined);
    if (value[from] === undefined || slot < 0 || slot > value.length) return;
    const target = slot > from ? slot - 1 : slot;
    if (target === from) return;
    const columns = [...value];
    const [column] = columns.splice(from, 1);
    columns.splice(target, 0, column!);
    rowIds.move(from, target);
    setExpandedSettings(new Set());
    setSelectedRows(new Set());
    onChange(columns, keys);
  };
  const insertionSlot = (event: DragEvent, index: number) => {
    const bounds = (
      event.currentTarget as HTMLTableRowElement
    ).getBoundingClientRect();
    if (bounds.height === 0) return index + 1;
    return event.clientY > bounds.top + bounds.height / 2 ? index + 1 : index;
  };
  const showLowCardinality = node.properties.low_cardinality !== undefined;
  const allRowsSelected =
    value.length > 0 && selectedRows.size === value.length;
  const someRowsSelected = selectedRows.size > 0;
  return (
    <div class="column-editor">
      <div class="column-editor-heading">
        <div>
          <small>DATA SCHEMA</small>
          <h3>Output columns</h3>
        </div>
        <div class="column-editor-actions">
          {someRowsSelected && (
            <div class="bulk-selection-toolbar" role="status">
              <span>{selectedRows.size} selected</span>
              <button
                type="button"
                class="bulk-delete"
                aria-label={`Delete ${selectedRows.size} selected ${selectedRows.size === 1 ? "column" : "columns"}`}
                title="Delete selected columns"
                disabled={disabled}
                onClick={deleteSelectedRows}
              >
                <TrashIcon />
              </button>
            </div>
          )}
          {systemColumns && (
            <button
              type="button"
              class="inline-disclosure"
              aria-expanded={systemColumnsOpen}
              disabled={disabled}
              onClick={(event) => {
                setSystemColumnsOpen((current) => !current);
                if (event.detail > 0) {
                  const button = event.currentTarget;
                  queueMicrotask(() => button.blur());
                }
              }}
            >
              Add system columns
            </button>
          )}
          <button
            class="add-row-button"
            type="button"
            disabled={disabled}
            onClick={() => {
              rowIds.insert(value.length);
              onChange([...value, createValue(node)], keys);
            }}
          >
            + Add column
          </button>
        </div>
      </div>
      {systemColumns && systemColumnsOpen && (
        <section class="schema-system-columns-panel">
          <div class="subsection-heading">
            <h4>System columns</h4>
            <button
              type="button"
              aria-label="Close system columns"
              onClick={() => setSystemColumnsOpen(false)}
            >
              ×
            </button>
          </div>
          <NodeEditor
            node={systemColumns.node}
            value={systemColumns.value ?? createValue(systemColumns.node)}
            disabled={disabled}
            onChange={systemColumns.onChange}
          />
        </section>
      )}
      <div class="table-shell">
        <table class="config-table column-table">
          <thead>
            <tr>
              <th class="drag-column" aria-label="Reorder" />
              <th class="selection-column">
                <IndeterminateCheckbox
                  ariaLabel="Select all output columns"
                  checked={allRowsSelected}
                  indeterminate={someRowsSelected && !allRowsSelected}
                  disabled={disabled || value.length === 0}
                  onChange={selectAllRows}
                />
              </th>
              <th>Column</th>
              <th>JSON path</th>
              <th>JSON type</th>
              <th>Arrow type</th>
              <th class="flag-column">Not null</th>
              {showLowCardinality && (
                <th class="flag-column">Low cardinality</th>
              )}
              <th class="actions-column">Actions</th>
            </tr>
          </thead>
          <tbody>
            {value.map((raw, index) => {
              const column = isObject(raw) ? raw : {};
              const name =
                typeof column.column_name === "string"
                  ? column.column_name
                  : "";
              const mainFields = [
                "column_name",
                "jsonpath",
                "json_data_type",
                "arrow_type",
              ];
              const extraFields = Object.entries(node.properties).filter(
                ([field]) =>
                  ![...mainFields, "nullable", "low_cardinality"].includes(
                    field,
                  ),
              );
              const hasCustomSettings = extraFields.some(
                ([field, child]) =>
                  column[field] !== undefined &&
                  !jsonValuesEqual(column[field]!, createValue(child)),
              );
              const settingsExpanded = expandedSettings.has(index);
              const selected = selectedRows.has(index);
              return (
                <Fragment key={rowIds.values[index]}>
                  <tr
                    class={`config-table-row ${selected ? "selected" : ""} ${draggedRow === index ? "dragged" : ""} ${dragTargetSlot === index && draggedRow !== index ? "drag-before" : ""} ${dragTargetSlot === value.length && index === value.length - 1 && draggedRow !== index ? "drag-after" : ""}`}
                    onDragOver={(event) => {
                      if (draggedRow === undefined) return;
                      event.preventDefault();
                      if (event.dataTransfer)
                        event.dataTransfer.dropEffect = "move";
                      setDragTargetSlot(insertionSlot(event, index));
                    }}
                    onDrop={(event) => {
                      event.preventDefault();
                      removeDragPreview();
                      if (draggedRow !== undefined)
                        moveColumnToSlot(
                          draggedRow,
                          insertionSlot(event, index),
                        );
                    }}
                  >
                    <td class="drag-column">
                      <button
                        type="button"
                        class="drag-handle"
                        draggable={!disabled}
                        disabled={disabled}
                        aria-label={`Move output column ${index + 1}`}
                        title="Drag to reorder; use arrow keys for keyboard control"
                        onDragStart={(event) => {
                          if (event.dataTransfer) {
                            removeDragPreview();
                            event.dataTransfer.effectAllowed = "move";
                            event.dataTransfer.setData(
                              "text/plain",
                              String(index),
                            );
                            const row = event.currentTarget.closest("tr");
                            if (row instanceof HTMLTableRowElement)
                              dragPreview.current = createColumnDragPreview(
                                row,
                                event.dataTransfer,
                                event.clientX,
                                event.clientY,
                              );
                          }
                          setDraggedRow(index);
                          setDragTargetSlot(index);
                        }}
                        onDragEnd={() => {
                          removeDragPreview();
                          setDraggedRow(undefined);
                          setDragTargetSlot(undefined);
                        }}
                        onKeyDown={(event) => {
                          if (event.key === "ArrowUp" && index > 0) {
                            event.preventDefault();
                            moveColumn(index, index - 1);
                          }
                          if (
                            event.key === "ArrowDown" &&
                            index < value.length - 1
                          ) {
                            event.preventDefault();
                            moveColumn(index, index + 1);
                          }
                        }}
                      >
                        <DragHandleIcon />
                      </button>
                    </td>
                    <td class="selection-column">
                      <input
                        type="checkbox"
                        aria-label={`Select output column ${index + 1}`}
                        checked={selected}
                        disabled={disabled}
                        onChange={() => toggleRowSelection(index)}
                      />
                    </td>
                    {mainFields.map((field) => (
                      <td key={field}>
                        {node.properties[field] && (
                          <NodeEditor
                            node={node.properties[field]!}
                            value={
                              column[field] ??
                              createValue(node.properties[field]!)
                            }
                            disabled={disabled}
                            onChange={(next) =>
                              updateColumn(index, {
                                ...column,
                                [field]: next,
                              })
                            }
                          />
                        )}
                      </td>
                    ))}
                    <td class="flag-column">
                      <input
                        type="checkbox"
                        aria-label={`Column ${index + 1} not null`}
                        disabled={disabled}
                        checked={column.nullable !== true}
                        onChange={(event) =>
                          updateColumn(index, {
                            ...column,
                            nullable: !event.currentTarget.checked,
                          })
                        }
                      />
                    </td>
                    {showLowCardinality && (
                      <td
                        class={`flag-column tooltip-host ${isStringArrowType(column.arrow_type) ? "" : "disabled"}`}
                        data-tooltip="Low cardinality is meaningful only for string values"
                      >
                        <input
                          type="checkbox"
                          aria-label={`Column ${index + 1} low cardinality`}
                          disabled={
                            disabled || !isStringArrowType(column.arrow_type)
                          }
                          checked={column.low_cardinality === true}
                          onChange={(event) =>
                            updateColumn(index, {
                              ...column,
                              low_cardinality: event.currentTarget.checked,
                            })
                          }
                        />
                      </td>
                    )}
                    <td class="actions-column">
                      <ColumnActions
                        row={index + 1}
                        disabled={disabled}
                        hasSettings={extraFields.length > 0}
                        hasCustomSettings={hasCustomSettings}
                        settingsExpanded={settingsExpanded}
                        onSettings={() => toggleSettings(index)}
                        onMoveUp={
                          index === 0
                            ? undefined
                            : () => moveColumn(index, index - 1)
                        }
                        onMoveDown={
                          index === value.length - 1
                            ? undefined
                            : () => moveColumn(index, index + 1)
                        }
                        onDuplicate={() => duplicateColumn(index)}
                        onDelete={() => deleteColumn(index, name)}
                      />
                    </td>
                  </tr>
                  {extraFields.length > 0 && settingsExpanded && (
                    <tr class="table-details-row">
                      <td />
                      <td />
                      <td colSpan={showLowCardinality ? 7 : 6}>
                        <section class="column-details">
                          <div class="column-details-heading">
                            <strong>Advanced column settings</strong>
                            <button
                              type="button"
                              aria-label={`Close column ${index + 1} settings`}
                              onClick={() => toggleSettings(index)}
                            >
                              ×
                            </button>
                          </div>
                          <div class="schema-object">
                            {extraFields.map(([field, child]) => (
                              <PropertyEditor
                                name={field}
                                node={child}
                                required={node.required.has(field)}
                                value={column[field]}
                                disabled={disabled}
                                onChange={(next) =>
                                  updateColumn(index, {
                                    ...column,
                                    [field]: next,
                                  })
                                }
                              />
                            ))}
                          </div>
                        </section>
                      </td>
                    </tr>
                  )}
                </Fragment>
              );
            })}
          </tbody>
        </table>
      </div>
      {value.length === 0 && (
        <p class="empty-columns">
          Add the first output column to define the parsed data schema.
        </p>
      )}
      <div class="column-keys">
        <span class="field-label">
          Keys <small class="optional">(optional)</small>
        </span>
        <MultiSelectControl
          values={keys}
          disabled={disabled}
          placeholder="Not selected"
          options={uniqueStrings([
            ...value.flatMap((raw) => {
              const column = isObject(raw) ? raw : {};
              return typeof column.column_name === "string" &&
                column.column_name !== ""
                ? [column.column_name]
                : [];
            }),
            ...additionalKeyOptions,
            ...keys,
          ]).map((name) => ({ value: name, label: name }))}
          onChange={(next) => onChange(value, next)}
        />
      </div>
    </div>
  );
}

