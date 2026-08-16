import type { JsonValue } from "../json";
import { ColumnMappingsEditor } from "./ColumnMappingsEditor";
import type { CompiledNode } from "./compiler";
import type {
  NodeEditorComponent,
  PropertyEditorComponent,
} from "./editorTypes";
import { reconcileSystemColumnKeys } from "./formLogic";
import { isObject, stringArray } from "./value";

export function isJsonParserContainer(
  node: Extract<CompiledNode, { kind: "object" }>,
): boolean {
  const common = Object.values(node.properties).find(
    (child) => child.xUi.widget === "parser_common",
  );
  const parser = Object.values(node.properties).find(
    (child) =>
      child.kind === "object" &&
      Object.values(child.properties).some(
        (property) => property.xUi.widget === "column_mappings",
      ),
  );
  return (
    common?.kind === "object" &&
    parser?.kind === "object" &&
    Object.values(parser.properties).some(
      (property) =>
        property.kind === "array" && property.xUi.widget === "column_mappings",
    )
  );
}

export function JsonParserEditor({
  node,
  value,
  disabled,
  onChange,
  NodeEditor,
  PropertyEditor,
}: {
  node: Extract<CompiledNode, { kind: "object" }>;
  value: JsonValue;
  disabled: boolean;
  onChange: (value: JsonValue) => void;
  NodeEditor: NodeEditorComponent;
  PropertyEditor: PropertyEditorComponent;
}) {
  const object = isObject(value) ? value : {};
  const commonNode = node.properties.common;
  const parserNode = node.properties.json_parser;
  if (commonNode?.kind !== "object" || parserNode?.kind !== "object")
    return null;
  const common = isObject(object.common) ? object.common : {};
  const parser = isObject(object.json_parser) ? object.json_parser : {};
  const columnsNode = parserNode.properties.columns;
  if (columnsNode?.kind !== "array") return null;
  const updateCommon = (name: string, next: JsonValue) =>
    onChange({ ...object, common: { ...common, [name]: next } });
  const updateParser = (name: string, next: JsonValue) =>
    onChange({ ...object, json_parser: { ...parser, [name]: next } });
  const systemColumns = isObject(common.system_columns)
    ? Object.values(common.system_columns).filter(
        (name): name is string => typeof name === "string" && name !== "",
      )
    : [];
  const commonFields = Object.entries(commonNode.properties).filter(
    ([name]) => name !== "table_naming" && name !== "system_columns",
  );
  const parserFields = Object.entries(parserNode.properties).filter(
    ([name]) => !["json_framing", "columns", "keys"].includes(name),
  );
  return (
    <div class="schema-object json-parser-editor">
      <section class="parser-common-section">
        <h3>Parser settings</h3>
        {commonNode.properties.table_naming && (
          <PropertyEditor
            name="table_naming"
            node={commonNode.properties.table_naming}
            required={commonNode.required.has("table_naming")}
            value={common.table_naming}
            disabled={disabled}
            onChange={(next) => updateCommon("table_naming", next)}
          />
        )}
        {parserNode.properties.json_framing && (
          <PropertyEditor
            name="json_framing"
            node={parserNode.properties.json_framing}
            required={parserNode.required.has("json_framing")}
            value={parser.json_framing}
            disabled={disabled}
            onChange={(next) => updateParser("json_framing", next)}
          />
        )}
        {commonFields.map(([name, child]) => (
          <PropertyEditor
            key={name}
            name={name}
            node={child}
            required={commonNode.required.has(name)}
            value={common[name]}
            disabled={disabled}
            onChange={(next) => updateCommon(name, next)}
          />
        ))}
      </section>
      <ColumnMappingsEditor
        node={columnsNode.item}
        value={Array.isArray(parser.columns) ? parser.columns : []}
        keys={stringArray(parser.keys)}
        additionalKeyOptions={systemColumns}
        {...(commonNode.properties.system_columns === undefined
          ? {}
          : {
              systemColumns: {
                node: commonNode.properties.system_columns,
                value: common.system_columns,
                onChange: (next: JsonValue) =>
                  onChange({
                    ...object,
                    common: { ...common, system_columns: next },
                    json_parser: {
                      ...parser,
                      keys: reconcileSystemColumnKeys(
                        common.system_columns,
                        next,
                        stringArray(parser.keys),
                      ),
                    },
                  }),
              },
            })}
        disabled={disabled}
        NodeEditor={NodeEditor}
        PropertyEditor={PropertyEditor}
        onChange={(columns, keys) =>
          onChange({
            ...object,
            json_parser: { ...parser, columns, keys },
          })
        }
      />
      {parserFields.length > 0 && (
        <section class="parser-secondary-section">
          <h3>Parsing behavior</h3>
          <div class="schema-object">
            {parserFields.map(([name, child]) => (
              <PropertyEditor
                key={name}
                name={name}
                node={child}
                required={parserNode.required.has(name)}
                value={parser[name]}
                disabled={disabled}
                onChange={(next) => updateParser(name, next)}
              />
            ))}
          </div>
        </section>
      )}
    </div>
  );
}
