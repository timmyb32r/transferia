mod bootstrap;
mod phase;

pub use bootstrap::{
    acquire_execution_lock, begin_locked_snapshot, inspect_mysql8_gtid_source,
    replication_lock_name, LockedSnapshotBootstrap, MySqlExecutionLock,
    MySqlGtidState, MySqlReplicationPreflight, MySqlSnapshotSession, PreparedMySqlSnapshot,
};
pub use phase::{
    SnapshotStreamPreparation, SnapshotStreamTracker, SNAPSHOT_STREAM_STATE_KEY,
};

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MySqlSourceIdentity {
    pub server_uuid: String,

    pub database: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MySqlBinlogBoundary {
    pub filename: String,

    pub position: u64,

    pub gtid_executed: String,

    /// UTC time reported by the source while writes were held by FTWRL.
    pub source_timestamp_micros: i64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AuthoritativeTableIdentity {
    pub database: String,

    pub table: String,

    /// Storage engine affects snapshot consistency and row-event semantics.
    pub engine: String,

    /// Ordered physical row layout and exact primary-key membership.
    ///
    /// This deliberately does not persist `SHOW CREATE TABLE`: MySQL embeds
    /// volatile table state such as the next `AUTO_INCREMENT` value in that
    /// statement, so ordinary inserts would otherwise look like schema drift.
    pub columns: Vec<AuthoritativeColumnIdentity>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AuthoritativeColumnIdentity {
    pub name: String,

    /// Exact `information_schema.COLUMNS.COLUMN_TYPE`, including width,
    /// unsigned/zerofill modifiers, and enum/set members.
    pub column_type: String,

    pub nullable: bool,

    pub character_set: Option<String>,

    pub collation: Option<String>,

    /// Numeric MySQL collation id carried by binlog table-map metadata.
    pub collation_id: Option<u16>,

    /// Stable column modifiers such as `auto_increment`, generated-column
    /// storage, and visibility.
    pub extra: String,

    /// Exact nullable `information_schema.COLUMNS.GENERATION_EXPRESSION`.
    /// MySQL reports an empty string for ordinary columns while MariaDB may report NULL.
    pub generation_expression: Option<String>,

    /// One-based position in the PRIMARY index, or `None` for non-key columns.
    pub primary_key_ordinal: Option<u64>,

    pub primary_key_prefix_length: Option<u64>,

    /// Exact PRIMARY-index direction (`A`/`D`) when reported by MySQL.
    pub primary_key_direction: Option<String>,
}

#[derive(Debug)]
pub struct MySqlReplicationSafetyViolation {
    source: anyhow::Error,
}

impl core::fmt::Display for MySqlReplicationSafetyViolation {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            formatter,
            "MySQL replication safety contract violated: {}",
            self.source
        )
    }
}

impl std::error::Error for MySqlReplicationSafetyViolation {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.source.as_ref())
    }
}

#[must_use]
pub fn replication_safety_violation(error: anyhow::Error) -> anyhow::Error {
    MySqlReplicationSafetyViolation { source: error }.into()
}

#[must_use]
pub fn is_replication_safety_violation(error: &anyhow::Error) -> bool {
    error
        .chain()
        .any(|cause| cause.is::<MySqlReplicationSafetyViolation>())
}

fn validate_server_uuid(server_uuid: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        server_uuid.len() == 36
            && server_uuid.bytes().enumerate().all(|(index, byte)| {
                if matches!(index, 8 | 13 | 18 | 23) {
                    byte == b'-'
                } else {
                    byte.is_ascii_hexdigit()
                }
            }),
        "MySQL server_uuid is not a canonical UUID"
    );
    Ok(())
}
