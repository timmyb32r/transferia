import { Fragment } from "preact";
import { useEffect, useRef, useState } from "preact/hooks";

import type { JsonValue } from "../../json";
import { Button } from "../../ui/Button";
import { DragHandleIcon, TrashIcon } from "../../ui/icons";
import { MultiSelectControl } from "../../ui/SelectControl";
import { ColumnActions } from "./ColumnActions";
import {
  createColumnDragPreview,
  insertionSlot,
} from "../../schema/columnDrag";
import { createValue, type CompiledNode } from "../../schema/compiler";
import { IndeterminateCheckbox } from "../../schema/controls";
import type {
  NodeEditorComponent,
  PropertyEditorComponent,
} from "../../schema/editorTypes";
import { isStringArrowType } from "./model";
import { useColumnMappings } from "./useColumnMappings";
import { isObject, jsonValuesEqual, uniqueStrings } from "../../schema/value";

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
  NodeEditor: NodeEditorComponent;
  PropertyEditor: PropertyEditorComponent;
}) {
  const [systemColumnsOpen, setSystemColumnsOpen] = useState(false);
  const [draggedRow, setDraggedRow] = useState<number>();
  const [dragTargetSlot, setDragTargetSlot] = useState<number>();
  const dragPreview = useRef<HTMLTableElement | null>(null);
  const mappings = useColumnMappings({ value, keys, onChange });
  const {
    expandedSettings,
    selectedRows,
    rowIds,
    updateColumn,
    toggleSettings,
    duplicateColumn,
    deleteColumn,
    toggleRowSelection,
    selectAllRows,
    deleteSelectedRows,
    moveColumn: moveColumnModel,
    moveColumnToSlot: moveColumnToSlotModel,
  } = mappings;
  const moveColumn = (from: number, to: number) => {
    setDraggedRow(undefined);
    setDragTargetSlot(undefined);
    moveColumnModel(from, to);
  };
  const moveColumnToSlot = (from: number, slot: number) => {
    setDraggedRow(undefined);
    setDragTargetSlot(undefined);
    moveColumnToSlotModel(from, slot);
  };
  const removeDragPreview = () => {
    dragPreview.current?.remove();
    dragPreview.current = null;
  };
  useEffect(() => removeDragPreview, []);
  if (node.kind !== "object")
    return (
      <NodeEditor
        node={{ kind: "array", item: node, xUi: {} }}
        value={value}
        disabled={disabled}
        onChange={(next) => onChange(Array.isArray(next) ? next : [], keys)}
      />
    );
  const showLowCardinality = node.properties.low_cardinality !== undefined;
  const allRowsSelected =
    value.length > 0 && selectedRows.size === value.length;
  const someRowsSelected = selectedRows.size > 0;
  const notNullCount = value.filter(
    (raw) => isObject(raw) && raw.nullable !== true,
  ).length;
  const allNotNull = value.length > 0 && notNullCount === value.length;
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
              <Button
                class="bulk-delete"
                aria-label={`Delete ${selectedRows.size} selected ${selectedRows.size === 1 ? "column" : "columns"}`}
                title="Delete selected columns"
                disabled={disabled}
                onClick={deleteSelectedRows}
              >
                <TrashIcon />
              </Button>
            </div>
          )}
          {systemColumns && (
            <Button
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
            </Button>
          )}
          <Button
            shape="add-row"
            disabled={disabled}
            onClick={() => {
              rowIds.insert(value.length);
              const created = createValue(node);
              onChange(
                [
                  ...value,
                  isObject(created)
                    ? {
                        ...created,
                        json_data_type: "string",
                        arrow_type: "Utf8",
                      }
                    : created,
                ],
                keys,
              );
            }}
          >
            + Add column
          </Button>
        </div>
      </div>
      {systemColumns && systemColumnsOpen && (
        <section class="schema-system-columns-panel">
          <div class="subsection-heading">
            <h4>System columns</h4>
            <Button
              aria-label="Close system columns"
              onClick={() => setSystemColumnsOpen(false)}
            >
              ×
            </Button>
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
              <th class="flag-column bulk-flag-column">
                <span>Not null</span>
                <IndeterminateCheckbox
                  ariaLabel="Set not null for all output columns"
                  checked={allNotNull}
                  indeterminate={notNullCount > 0 && !allNotNull}
                  disabled={disabled || value.length === 0}
                  onChange={() => {
                    const nullable = allNotNull;
                    onChange(
                      value.map((raw) => ({
                        ...(isObject(raw) ? raw : {}),
                        nullable,
                      })),
                      keys,
                    );
                  }}
                />
              </th>
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
                      <Button
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
                      </Button>
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
                            <Button
                              aria-label={`Close column ${index + 1} settings`}
                              onClick={() => toggleSettings(index)}
                            >
                              ×
                            </Button>
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
