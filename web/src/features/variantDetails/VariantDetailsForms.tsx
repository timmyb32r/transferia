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

export function SerializerDetailsForm(props: SchemaFormProps) {
  return (
    <VariantDetailsForm
      {...props}
      widget="serializer"
      bridgeClass="sink-serializer-bridge"
      cardClass="serializer-details-card"
    />
  );
}
