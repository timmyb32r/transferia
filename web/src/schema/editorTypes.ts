import type { ComponentType } from "preact";

import type { JsonObject, JsonValue } from "../json";
import type { CompiledNode } from "./compiler";

export interface NodeEditorProps {
  node: CompiledNode;
  value: JsonValue;
  disabled?: boolean | undefined;
  onChange: (value: JsonValue) => void;
  path?: string | undefined;
  controlId?: string | undefined;
}

export interface PropertyEditorProps {
  name: string;
  node: CompiledNode;
  required: boolean;
  value: JsonValue | undefined;
  disabled: boolean;
  showPartitionRanges?: boolean | undefined;
  parentValue?: JsonObject | undefined;
  onParentChange?: ((value: JsonObject) => void) | undefined;
  onChange: (value: JsonValue) => void;
  path?: string | undefined;
}

export type NodeEditorComponent = ComponentType<NodeEditorProps>;
export type PropertyEditorComponent = ComponentType<PropertyEditorProps>;
