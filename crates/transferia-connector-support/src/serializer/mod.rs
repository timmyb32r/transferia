mod debezium;
pub mod json_serializer;
mod schema_registry;

pub use debezium::{QueueMessageMode, SerializedBatch, SerializedDelivery, SerializedMessage};
pub use json_serializer::JsonBatchEncoder;
pub use schema_registry::{DeliverySerializer, SerializerConfig};

pub fn json_type_mapping() -> transferia_registry::type_mapping::TypeMapping {
    transferia_registry::type_mapping::destination_mapping(
        "JSON serializer examples, evaluated by the production serializer. This transport has no native column types. Selecting Avro, Protobuf, Debezium or another format changes the output contract; inspect that format's configuration and schema.",
        |column| SerializerConfig::Json.destination_type(&column.data_type),
    )
}
