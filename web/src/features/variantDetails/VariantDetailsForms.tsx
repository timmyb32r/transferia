import {
  VariantDetailsForm,
  type SchemaFormProps,
} from "../../schema/SchemaForm";
import type { CompiledNode } from "../../schema/compiler";

function parserContentClass(node: CompiledNode): string {
  const capability = node.xUi.capabilities;
  const wide = capability?.component === "parser"
    && (capability.key === "json_parser" || capability.key === "s3_json" || capability.key === "tskv");
  return wide ? "island-form island-form-wide" : "island-form";
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
