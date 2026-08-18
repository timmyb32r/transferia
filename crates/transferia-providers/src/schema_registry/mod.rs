mod avro;
mod client;
mod config;
mod protobuf;
mod wire;

pub use client::{RegistryClient, RegistrySchema};
pub use config::{SchemaFormat, SchemaRegistryAuth, SchemaRegistryConnection};
pub use wire::{decode_message_indexes, encode_message_indexes, ConfluentEnvelope};

pub(crate) use avro::{avro_to_json, json_to_avro};
pub(crate) use protobuf::protobuf_descriptor_pool;

#[cfg(test)]
mod tests;
