import { useEffect, useState } from "preact/hooks";

import type { JsonObject, JsonValue } from "../json";
import { useStableRowIds } from "./controls";
import { closestArrowType, isStringArrowType } from "./formLogic";
import { isObject } from "./value";

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
    const nextKeys =
      newName === oldName
        ? keys
        : keys.map((key) => (key === oldName ? newName : key)).filter(Boolean);
    onChange(columns, nextKeys);
  };
  const toggleSettings = (index: number) =>
    setExpandedSettings((current) => toggled(current, index));
  const duplicateColumn = (index: number) => {
    const columns = [...value];
    columns.splice(index + 1, 0, structuredClone(value[index]!));
    rowIds.insert(index + 1);
    resetTransientRows();
    onChange(columns, keys);
  };
  const deleteColumn = (index: number, name: string) => {
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
  const moveColumnToSlot = (from: number, slot: number) => {
    if (value[from] === undefined || slot < 0 || slot > value.length) return;
    const target = slot > from ? slot - 1 : slot;
    if (target === from) return;
    const columns = [...value];
    const [column] = columns.splice(from, 1);
    columns.splice(target, 0, column!);
    rowIds.move(from, target);
    resetTransientRows();
    onChange(columns, keys);
  };

  return {
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
    moveColumn,
    moveColumnToSlot,
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
