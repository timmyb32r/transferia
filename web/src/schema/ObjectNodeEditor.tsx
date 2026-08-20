import { Fragment, type ComponentChildren } from "preact";
import { useEffect, useRef, useState } from "preact/hooks";

import type { JsonObject } from "../json";
import { Disclosure } from "../ui/Disclosure";
import type { CompiledNode } from "./compiler";
import type { PropertyEditorComponent } from "./editorTypes";
import {
  clearConfiguredPartitionRanges,
  hasConfiguredPartitionRanges,
  partitionRangesProperty,
} from "./partitionRanges";
import type { WidgetRegistry } from "./widgetRegistry";

type ObjectNode = Extract<CompiledNode, { kind: "object" }>;
type PropertyEntry = [string, CompiledNode];

export function ObjectNodeEditor({
  node,
  value,
  disabled,
  path,
  connectionAction,
  widgets,
  PropertyEditor,
  onChange,
}: {
  node: ObjectNode;
  value: JsonObject;
  disabled: boolean;
  path: string;
  connectionAction?: ComponentChildren;
  widgets: WidgetRegistry;
  PropertyEditor: PropertyEditorComponent;
  onChange: (value: JsonObject) => void;
}) {
  const partitionRanges = partitionRangesProperty(node);
  const configuredPartitionRanges = hasConfiguredPartitionRanges(
    value,
    partitionRanges,
  );
  const [partitionRangesVisible, setPartitionRangesVisible] = useState(
    () => configuredPartitionRanges,
  );
  const previouslyConfiguredPartitionRanges = useRef(configuredPartitionRanges);
  useEffect(() => {
    if (partitionRanges === undefined) setPartitionRangesVisible(false);
    else if (configuredPartitionRanges) setPartitionRangesVisible(true);
    else if (previouslyConfiguredPartitionRanges.current)
      setPartitionRangesVisible(false);
    previouslyConfiguredPartitionRanges.current = configuredPartitionRanges;
  }, [
    configuredPartitionRanges,
    partitionRanges?.arrayName,
    partitionRanges?.fieldName,
  ]);

  const visible = Object.entries(node.properties).filter(
    ([, child]) => !widgets.isHidden(child),
  );
  const regular = visible.filter(
    ([, child]) => child.xUi.section === undefined,
  );
  const advanced = section(visible, "advanced");
  const systemColumns = section(visible, "system_columns");
  const shardGroup = section(visible, "shard_group");
  const connectionAnchor =
    regular.find(
      ([, child]) =>
        widgets.presentation(child)?.connectionActionAnchor === "after",
    ) ??
    regular.find(
      ([, child]) =>
        widgets.presentation(child)?.connectionActionAnchor === "before",
    );
  const connectionAnchorPosition = connectionAnchor
    ? widgets.presentation(connectionAnchor[1])?.connectionActionAnchor
    : undefined;
  const property = ([name, child]: PropertyEntry) => (
    <PropertyEditor
      key={name}
      name={name}
      node={child}
      required={node.required.has(name)}
      value={value[name]}
      disabled={disabled}
      path={`${path}/${name}`}
      onChange={(next) => onChange({ ...value, [name]: next })}
    />
  );

  return (
    <div class="schema-object">
      {regular.map(([name, child]) => (
        <Fragment key={name}>
          {connectionAnchorPosition === "before" &&
            connectionAnchor?.[0] === name &&
            connectionAction}
          <PropertyEditor
            name={name}
            node={child}
            required={node.required.has(name)}
            value={value[name]}
            disabled={disabled}
            showPartitionRanges={partitionRangesVisible}
            parentValue={value}
            onParentChange={onChange}
            path={`${path}/${name}`}
            onChange={(next) => onChange({ ...value, [name]: next })}
          />
          {connectionAnchorPosition === "after" &&
            connectionAnchor?.[0] === name &&
            connectionAction}
        </Fragment>
      ))}
      {connectionAnchor === undefined && connectionAction}
      {shardGroup.length > 0 && (
        <Disclosure label="Shard group" class="shard-group-settings">
          {shardGroup.map(property)}
        </Disclosure>
      )}
      {systemColumns.length > 0 && (
        <Disclosure label="Add system columns" class="system-columns">
          {systemColumns.map(property)}
        </Disclosure>
      )}
      {(advanced.length > 0 || partitionRanges !== undefined) && (
        <Disclosure label="Advanced settings">
          {partitionRanges !== undefined && (
            <div class="form-row partition-mode-control">
              <span class="field-label">Specify partitions</span>
              <label class="toggle">
                <input
                  type="checkbox"
                  aria-label="Specify partitions"
                  checked={partitionRangesVisible}
                  disabled={disabled}
                  onChange={(event) => {
                    const visible = event.currentTarget.checked;
                    setPartitionRangesVisible(visible);
                    if (!visible)
                      onChange(
                        clearConfiguredPartitionRanges(value, partitionRanges),
                      );
                  }}
                />
              </label>
            </div>
          )}
          {advanced.map(property)}
        </Disclosure>
      )}
    </div>
  );
}

function section(
  entries: PropertyEntry[],
  name: NonNullable<CompiledNode["xUi"]["section"]>,
): PropertyEntry[] {
  return entries.filter(([, child]) => child.xUi.section === name);
}
