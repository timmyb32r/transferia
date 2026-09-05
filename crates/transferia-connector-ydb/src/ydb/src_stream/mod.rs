mod config;
mod decoder;
mod setup;
mod source;
mod topic;

pub use config::YdbReplicationConfig;
pub(super) use setup::{
    discover_replication_resources, prepare_replication, replication_contract_violation,
    PreparedReplication, decode_topic_operation,
};
pub(super) use source::{replication_discovery, YdbReplicationSource, build_table_schema, schema_materialization_admission_bytes};
