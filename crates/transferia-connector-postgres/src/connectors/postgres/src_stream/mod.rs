mod config;
mod event;
mod pgoutput;
mod reader;
mod slot_recovery;
mod wal2json;

pub use config::{LogicalDecoder, PostgresReplicationConfig};
pub(crate) use reader::PostgresReplicationSource;

#[cfg(test)]
mod tests;
