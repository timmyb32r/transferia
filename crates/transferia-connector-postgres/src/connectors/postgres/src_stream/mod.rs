mod config;
mod event;
mod identity;
mod pgoutput;
mod publication;
mod reader;
mod relation_identity;
mod slot_recovery;
mod wal2json;

pub use config::{LogicalDecoder, PostgresReplicationConfig};
pub(crate) use identity::{
    authoritative_table_identities, AuthoritativeTableIdentity, PostgresSourceIdentity,
    PostgresSystemIdentity,
};
pub(crate) use publication::{is_replication_contract_violation, validate_pgoutput_publication};
pub(crate) use reader::PostgresReplicationSource;
pub(crate) use slot_recovery::{is_replication_safety_violation, replication_safety_violation};

#[cfg(test)]
mod tests;
