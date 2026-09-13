import { useEffect, useState } from "preact/hooks";

import type { JsonObject, JsonValue } from "../../json";
import { useStableRowIds } from "../../schema/controls";
import { closestArrowType, isStringArrowType } from "./model";
import { isObject, uniqueStrings } from "../../schema/value";

export function useColumnMappings({
  value,
  keys,
  onChange,
}: {
  value: JsonValue[];
  keys: string[];
  onChange: (columns: JsonValue[], keys: string[]) => void;
}) {
  const [expandedSettings, setExpandedSettings] = useState<Set<number>>(
    () => new Set(),
  );
  const [selectedRows, setSelectedRows] = useState<Set<number>>(
    () => new Set(),
  );
  const rowIds = useStableRowIds(value.length);
  // Unnamed draft columns have no serializable key identity yet. Keep their
  // selection by row ID until a name is entered; never put an empty name in keys.
  const [pendingKeys, setPendingKeys] = useState<Set<string>>(() => new Set());
  const columnName = (index: number) => {
    const column = value[index];
    return isObject(column) ? stringProperty(column, "column_name") : "";
  };
  const isColumnKey = (index: number) => {
    const name = columnName(index);
    return name === "" ? pendingKeys.has(rowIds.values[index]!) : keys.includes(name);
  };
  const setPendingKey = (id: string, checked: boolean) => {
    setPendingKeys((current) => {
      const next = new Set(current);
      if (checked) next.add(id);
      else next.delete(id);
      return next;
    });
  };
  const setColumnKey = (index: number, checked: boolean) => {
    const name = columnName(index);
    if (name === "") setPendingKey(rowIds.values[index]!, checked);
    else onChange(value, checked ? uniqueStrings([...keys, name]) : keys.filter((key) => key !== name));
  };

  useEffect(() => {
    setSelectedRows((current) => {
      const next = new Set(
        [...current].filter((index) => index < value.length),
      );
      return next.size === current.size ? current : next;
    });
  }, [value.length]);

  const resetTransientRows = () => {
    setExpandedSettings(new Set());
    setSelectedRows(new Set());
  };
  const updateColumn = (index: number, candidate: JsonObject) => {
    const previous = isObject(value[index]) ? value[index] : {};
    const oldName = stringProperty(previous, "column_name");
    const newName = stringProperty(candidate, "column_name");
    let next = candidate;
    if (
      typeof next.json_data_type === "string" &&
      next.json_data_type !== previous.json_data_type
    ) {
      next = {
        ...next,
        arrow_type: closestArrowType(next.json_data_type),
      };
    }
    if (!isStringArrowType(next.arrow_type)) {
      next = { ...next, low_cardinality: false };
    }
    if (
      newName !== oldName &&
      (previous.jsonpath === "" || previous.jsonpath === `$.${oldName}`)
    ) {
      next = { ...next, jsonpath: newName === "" ? "" : `$.${newName}` };
    }
    const columns = [...value];
    columns[index] = next;
    let nextKeys =
      newName === oldName
        ? keys
        : keys.map((key) => (key === oldName ? newName : key)).filter(Boolean);
    if (newName !== oldName) {
      const wasKey = isColumnKey(index);
      setPendingKey(rowIds.values[index]!, wasKey && newName === "");
      if (wasKey && newName !== "") nextKeys = uniqueStrings([...nextKeys, newName]);
    }
    onChange(columns, nextKeys);
  };
  const toggleSettings = (index: number) =>
    setExpandedSettings((current) => toggled(current, index));
  const duplicateColumn = (index: number) => {
    const columns = [...value];
    columns.splice(index + 1, 0, structuredClone(value[index]!));
    rowIds.insert(index + 1);
    if (columnName(index) === "" && isColumnKey(index)) {
      setPendingKey(rowIds.values[index + 1]!, true);
    }
    resetTransientRows();
    onChange(columns, keys);
  };
  const deleteColumn = (index: number, name: string) => {
    setPendingKey(rowIds.values[index]!, false);
    rowIds.remove(index);
    resetTransientRows();
    onChange(
      value.filter((_, itemIndex) => itemIndex !== index),
      keys.filter((key) => key !== name),
    );
  };
  const toggleRowSelection = (index: number) =>
    setSelectedRows((current) => toggled(current, index));
  const selectAllRows = (selected: boolean) =>
    setSelectedRows(
      selected ? new Set(value.map((_, index) => index)) : new Set(),
    );
  const deleteSelectedRows = () => {
    const deletedNames = new Set(
      [...selectedRows].flatMap((index) => {
        const column = isObject(value[index]) ? value[index] : {};
        const name = stringProperty(column, "column_name");
        return name === "" ? [] : [name];
      }),
    );
    resetTransientRows();
    const deletedIds = new Set([...selectedRows].map((index) => rowIds.values[index]));
    setPendingKeys((current) => new Set([...current].filter((id) => !deletedIds.has(id))));
    rowIds.retain((_, index) => !selectedRows.has(index));
    onChange(
      value.filter((_, index) => !selectedRows.has(index)),
      keys.filter((key) => !deletedNames.has(key)),
    );
  };
  const moveColumn = (from: number, to: number) => {
    if (from === to || value[from] === undefined || value[to] === undefined)
      return;
    const columns = [...value];
    const [column] = columns.splice(from, 1);
    columns.splice(to, 0, column!);
    rowIds.move(from, to);
    resetTransientRows();
    onChange(columns, keys);
  };

  return {
    expandedSettings,
    selectedRows,
    rowIds,
    isColumnKey,
    setColumnKey,
    updateColumn,
    toggleSettings,
    duplicateColumn,
    deleteColumn,
    toggleRowSelection,
    selectAllRows,
    deleteSelectedRows,
    moveColumn,
  };
}

function toggled(values: Set<number>, index: number): Set<number> {
  const next = new Set(values);
  if (next.has(index)) next.delete(index);
  else next.add(index);
  return next;
}

function stringProperty(object: JsonObject, name: string): string {
  const value = object[name];
  return typeof value === "string" ? value : "";
}
