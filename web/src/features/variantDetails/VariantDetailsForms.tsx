import {
  VariantDetailsForm,
  type SchemaFormProps,
} from "../../schema/SchemaForm";

export function ParserDetailsForm(props: SchemaFormProps) {
  return (
    <VariantDetailsForm
      {...props}
      widget="parser"
      bridgeClass="source-details-bridge"
      cardClass="source-details-card parser-details-card"
    />
  );
}
