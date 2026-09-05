import {
  VariantDetailsForm,
  type SchemaFormProps,
} from "../../schema/SchemaForm";

export function ParserDetailsForm(props: SchemaFormProps) {
  return (
    <VariantDetailsForm
      {...props}
      widget="parser"
      bridgeClass="source-parser-bridge"
      cardClass="parser-details-card"
    />
  );
}
