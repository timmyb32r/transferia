import { Fragment } from "preact";
import { useEffect, useRef, useState } from "preact/hooks";

import type { JsonValue } from "../../json";
import { AutofillResistantInput } from "../../ui/AutofillResistantField";
import { Button } from "../../ui/Button";
import { DragHandleIcon, TrashIcon } from "../../ui/icons";
import { ColumnActions } from "./ColumnActions";
import {
  createColumnDragPreview,
  insertionSlot,
} from "../../schema/columnDrag";
import {
  createValue,
  isComplete,
  type CompiledNode,
} from "../../schema/compiler";
import { IndeterminateCheckbox } from "../../schema/controls";
import { draftValue } from "../../schema/draft";
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
  incomplete,
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
  incomplete: boolean;
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
  const keyCheckbox = (name: string) => (
    <AutofillResistantInput
      type="checkbox"
      aria-label={`Key ${name || "unnamed column"}`}
      disabled={disabled || name === ""}
      checked={keys.includes(name)}
      onChange={(event) => onChange(value, event.currentTarget.checked
        ? uniqueStrings([...keys, name])
        : keys.filter((key) => key !== name))}
    />
  );
  const allRowsSelected =
    value.length > 0 && selectedRows.size === value.length;
  const someRowsSelected = selectedRows.size > 0;
  const notNullCount = value.filter(
    (raw) => isObject(raw) && raw.nullable !== true,
  ).length;
  const allNotNull = value.length > 0 && notNullCount === value.length;
  const mainFields = [
    "column_name",
    "jsonpath",
    "json_data_type",
    "arrow_type",
  ].filter((field) => node.properties[field] !== undefined);
  const mainFieldLabels: Record<string, string> = {
    column_name: "Column",
    jsonpath: "JSON path",
    json_data_type: "JSON type",
    arrow_type: "Arrow type",
  };
  const rowIsIncomplete = (raw: JsonValue) => {
    const column = isObject(raw) ? raw : {};
    return mainFields.some((field) => {
      const child = node.properties[field];
      return (
        child !== undefined &&
        node.required.has(field) &&
        !isComplete(child, column[field])
      );
    });
  };
  const addColumn = () => {
    rowIds.insert(value.length);
    const created = createValue(node);
    const column = isObject(created) ? { ...created } : created;
    if (isObject(column) && node.properties.json_data_type !== undefined)
      column.json_data_type = "string";
    if (isObject(column) && node.properties.arrow_type !== undefined)
      column.arrow_type = "Utf8";
    onChange([...value, column], keys);
  };
  return (
    <div
      data-required-guidance="structural"
      class={[
        "column-editor",
        !disabled && incomplete ? "required-incomplete" : "",
      ]
        .filter(Boolean)
        .join(" ")}
    >
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
            value={draftValue(systemColumns.node, systemColumns.value)}
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
              {mainFields.map((field) => (
                <th key={field} class={field === "json_data_type" ? "json-type-column" : undefined}>{mainFieldLabels[field]}</th>
              ))}
              <th class="flag-column">Key</th>
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
              const incompleteRequiredMainField = rowIsIncomplete(raw);
              return (
                <Fragment key={rowIds.values[index]}>
                  <tr
                    class={`config-table-row ${!disabled && incompleteRequiredMainField ? "required-incomplete" : ""} ${selected ? "selected" : ""} ${draggedRow === index ? "dragged" : ""} ${dragTargetSlot === index && draggedRow !== index ? "drag-before" : ""} ${dragTargetSlot === value.length && index === value.length - 1 && draggedRow !== index ? "drag-after" : ""}`}
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
                      <AutofillResistantInput
                        type="checkbox"
                        aria-label={`Select output column ${index + 1}`}
                        checked={selected}
                        disabled={disabled}
                        onChange={() => toggleRowSelection(index)}
                      />
                    </td>
                    {mainFields.map((field) => {
                      const original = node.properties[field];
                      const child = field === "arrow_type" && original?.kind === "string" && original.enumValues !== undefined
                        ? { ...original, xUi: { ...original.xUi, labels: {
                            ...original.xUi.labels,
                            ...Object.fromEntries(original.enumValues.filter((type): type is string => typeof type === "string" && type.startsWith("Timestamp(")).map((type) => [type, type.replace("Timestamp(", "Timestamp\n(")])),
                          } } }
                        : field === "json_data_type" && original?.kind === "string" && original.enumValues !== undefined
                        ? { ...original, enumValues: original.enumValues.filter((type) => type !== "decimal") }
                        : field === "json_data_type" && original?.kind === "union"
                          ? { ...original, branches: original.branches.filter((branch) => branch.constant !== "decimal") }
                          : original;
                      return (
                        <td key={field} class={field === "arrow_type" ? "arrow-type-cell" : undefined}>
                          {child && (
                            <NodeEditor
                              node={child}
                              value={column[field] ?? createValue(child)}
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
                      );
                    })}
                    <td class="flag-column">{keyCheckbox(typeof column.column_name === "string" ? column.column_name : "")}</td>
                    <td class="flag-column">
                      <AutofillResistantInput
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
                        class={`flag-column ${isStringArrowType(column.arrow_type) ? "" : "disabled"}`}
                        title={isStringArrowType(column.arrow_type) ? undefined : "Low cardinality is meaningful only for string values"}
                      >
                        <AutofillResistantInput
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
                      <td
                        colSpan={
                          mainFields.length + 3 + (showLowCardinality ? 1 : 0)
                        }
                      >
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
      <div class="column-editor-footer">
        <Button
          shape="add-row"
          data-required-control={value.length === 0 ? "true" : undefined}
          disabled={disabled}
          onClick={addColumn}
        >
          + Add column
        </Button>
      </div>
      {uniqueStrings([...additionalKeyOptions, ...keys]).filter((name) =>
        !value.some((raw) => isObject(raw) && raw.column_name === name),
      ).map((name) => (
        <label class="system-column-key" key={name}>
          {keyCheckbox(name)} {name} — Key
        </label>
      ))}
    </div>
  );
}
