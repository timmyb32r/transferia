import { Fragment, type ComponentChildren } from "preact";
import { useEffect, useId, useRef, useState } from "preact/hooks";

import type { JsonObject } from "../json";
import { AutofillResistantInput } from "../ui/AutofillResistantField";
import { Disclosure } from "../ui/Disclosure";
import { branchMatches, type CompiledNode } from "./compiler";
import type {
  NodeEditorComponent,
  PropertyEditorComponent,
} from "./editorTypes";
import {
  clearConfiguredPartitionRanges,
  hasConfiguredPartitionRanges,
  partitionRangesProperty,
} from "./partitionRanges";
import { hasEditableContent, type WidgetRegistry } from "./widgetRegistry";

type ObjectNode = Extract<CompiledNode, { kind: "object" }>;
type PropertyEntry = [string, CompiledNode];

export interface ConnectionFieldGroup {
  names: readonly string[];
  label: string;
  disabled: boolean;
  status?: ComponentChildren;
  renderField?: (name: string, field: ComponentChildren) => ComponentChildren;
  renderGroup?: (group: ComponentChildren) => ComponentChildren;
}

export function ObjectNodeEditor({
  node,
  value,
  disabled,
  path,
  connectionAction,
  connectionFields,
  widgets,
  NodeEditor,
  PropertyEditor,
  isVisible,
  onChange,
}: {
  node: ObjectNode;
  value: JsonObject;
  disabled: boolean;
  path: string;
  connectionAction?: ComponentChildren;
  connectionFields?: ConnectionFieldGroup | undefined;
  widgets: WidgetRegistry;
  NodeEditor: NodeEditorComponent;
  PropertyEditor: PropertyEditorComponent;
  isVisible: (node: CompiledNode) => boolean;
  onChange: (value: JsonObject) => void;
}) {
  const connectionStatusId = useId();
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

  const visible = Object.entries(node.properties)
    .filter(([, child]) => !widgets.isHidden(child) && isVisible(child))
    .map((entry, index) => ({ entry, index }))
    .sort(
      (left, right) =>
        (left.entry[1].xUi.order ?? 0) - (right.entry[1].xUi.order ?? 0) ||
        left.index - right.index,
    )
    .map(({ entry }) => entry);
  const regular = visible.filter(
    ([, child]) => child.xUi.section === undefined,
  );
  const advanced = section(visible, "advanced");
  const advancedParquet = section(visible, "advanced_parquet");
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
  const revealGate = regular.find(
    ([, child]) => child.xUi.reveal_rest_on_selection === true,
  );
  const gateSelected =
    revealGate === undefined ||
    (revealGate[1].kind === "union" &&
      revealGate[1].branches.some((branch) =>
        branchMatches(branch, value[revealGate[0]] ?? null),
      ));

  if (!gateSelected && revealGate !== undefined)
    return <div class="schema-object">{property(revealGate)}</div>;

  const deferredVariants = regular.flatMap(([name, child]) => {
    if (child.kind !== "union" || child.xUi.defer_variant_details !== true)
      return [];
    const branchIndex = child.branches.findIndex((branch) =>
      branchMatches(branch, value[name] ?? null),
    );
    const branch = child.branches[branchIndex];
    return branch === undefined ||
      branch.constant !== undefined ||
      !hasEditableContent(branch.node, widgets)
      ? []
      : [{ name, branchIndex, branch }];
  });

  const grouped = regular.filter(([name]) => connectionFields?.names.includes(name));
  const renderConnectionGroup = (group: ComponentChildren) => connectionFields?.renderGroup ? connectionFields.renderGroup(group) : group;
  const regularProperty = ([name, child]: PropertyEntry, fieldDisabled = disabled) => (
        <Fragment key={name}>
          {name === "unknown_fields" &&
          node.properties.conversion_error !== undefined
            ? null
            : name === "conversion_error" &&
                regular.some(([candidate]) => candidate === "unknown_fields")
              ? (
                  <div class="parse-policy-row">
                    {property([name, child])}
                    {property([
                      "unknown_fields",
                      node.properties.unknown_fields!,
                    ])}
                  </div>
                )
              : (
                  <>
                    {connectionAnchorPosition === "before" &&
                      connectionAnchor?.[0] === name &&
                      connectionAction}
                    {node.properties.common?.xUi.widget === "parser_common" && name === "json_parser" ? (
                      <NodeEditor
                        node={child}
                        value={value[name] ?? {}}
                        disabled={fieldDisabled}
                        path={`${path}/${name}`}
                        onChange={(next) => onChange({ ...value, [name]: next })}
                      />
                    ) : <PropertyEditor
                      name={name}
                      node={child}
                      required={node.required.has(name)}
                      value={value[name]}
                      disabled={fieldDisabled}
                      showPartitionRanges={partitionRangesVisible}
                      parentValue={value}
                      onParentChange={onChange}
                      path={`${path}/${name}`}
                      onChange={(next) => onChange({ ...value, [name]: next })}
                    />}
                    {connectionAnchorPosition === "after" &&
                      connectionAnchor?.[0] === name &&
                      connectionAction}
                  </>
                )}
        </Fragment>
  );

  return (
    <div class="schema-object">
      {regular.map(entry => !connectionFields || !connectionFields.names.includes(entry[0])
        ? regularProperty(entry)
        : entry[0] !== grouped[0]?.[0] ? null
        : <Fragment key="connection-dependent-settings">{renderConnectionGroup(<section class="connection-dependent-settings">
            {connectionFields.status && <div class="connection-dependent-status" id={connectionStatusId}>{connectionFields.status}</div>}
            <fieldset class="connection-dependent-fields" aria-label={connectionFields.label}
              aria-describedby={connectionFields.status ? connectionStatusId : undefined} disabled={disabled || connectionFields.disabled}>
              {grouped.map(entry => {
                const field = regularProperty(entry, disabled || connectionFields.disabled);
                return connectionFields.renderField ? connectionFields.renderField(entry[0], field) : field;
              })}
            </fieldset>
          </section>)}</Fragment>)}
      {deferredVariants.map(({ name, branchIndex, branch }) => (
        <div
          class={[
            "deferred-variant-details",
            node.properties[name]?.xUi.indent_variant_details === false
              ? ""
              : "nested-section",
          ]
            .filter(Boolean)
            .join(" ")}
          key={name}
        >
          <NodeEditor
            node={branch.node}
            value={value[name] ?? {}}
            disabled={disabled}
            path={`${path}/${name}/branch-${branchIndex}`}
            onChange={(next) => onChange({ ...value, [name]: next })}
          />
        </div>
      ))}
      {connectionAnchor === undefined && connectionAction}
      {shardGroup.length > 0 && (
        <Disclosure label="Shard group" class="shard-group-settings">
          {shardGroup.map(property)}
        </Disclosure>
      )}
      {advancedParquet.length > 0 && (
        <Disclosure
          label="Advanced Parquet settings"
          class="advanced-parquet-settings"
        >
          {advancedParquet.map(property)}
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
                <AutofillResistantInput
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
