import {
  VariantDetailsForm,
  type SchemaFormProps,
} from "../../schema/SchemaForm";
import type { CompiledNode } from "../../schema/compiler";

function parserContentClass(node: CompiledNode): string {
  const capability = node.xUi.capabilities;
  const json = capability?.component === "parser"
    && (capability.key === "json_parser" || capability.key === "s3_json");
  if (json) return "island-form island-form-wide parser-form json-parser-form";
  return capability?.component === "parser" && capability.key === "tskv"
    ? "island-form island-form-wide parser-form" : "island-form parser-form";
}

export function ParserDetailsForm(props: SchemaFormProps) {
  return (
    <VariantDetailsForm
      {...props}
      widget="parser"
      cardClass="card parser-details-card"
      contentClass={parserContentClass}
    />
  );
}
