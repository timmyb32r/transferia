import {
  VariantDetailsForm,
  type SchemaFormProps,
} from "../../schema/SchemaForm";

export function ParserDetailsForm(props: SchemaFormProps) {
  return (
    <VariantDetailsForm
      {...props}
      widget="parser"
      cardClass="card parser-details-card"
    />
  );
}
