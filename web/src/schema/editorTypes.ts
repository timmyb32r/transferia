import type { ComponentType } from "preact";

import type { JsonValue } from "../json";
import type { CompiledNode } from "./compiler";

export interface NodeEditorProps {
  node: CompiledNode;
  value: JsonValue;
  disabled?: boolean;
  onChange: (value: JsonValue) => void;
  path?: string;
  controlId?: string;
}

export interface PropertyEditorProps {
  name: string;
  node: CompiledNode;
  required: boolean;
  value: JsonValue | undefined;
  disabled: boolean;
  showPartitionRanges?: boolean;
  onChange: (value: JsonValue) => void;
  path?: string;
}

export type NodeEditorComponent = ComponentType<NodeEditorProps>;
export type PropertyEditorComponent = ComponentType<PropertyEditorProps>;
