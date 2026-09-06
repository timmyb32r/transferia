mod checksum;
mod config;
mod ddl;
mod decoder;
mod identity;
mod offset;
mod position;
mod source;
mod validation;

pub use checksum::{verify_event_checksum, BinlogChecksumError, BinlogChecksumVerifier};
pub use config::MySqlReplicationConfig;
pub use decoder::{
    BinlogDecodeError, CommittedTransaction, DecodedBinlogEvent, DecodedRowsEvent,
    IgnoredBinlogEvent, MySqlBinlogColumnIdentity, MySqlBinlogDecoder, MySqlRowChange,
    MySqlRowOperation, MySqlTableIdentity, MySqlTransactionIdentity, MySqlTransactionMarker,
    RolledBackTransaction, RotatedBinlog,
};
pub(crate) use identity::encode_snapshot_boundary_identity;
#[cfg(test)]
pub(crate) use identity::encode_transaction_identity;
pub(crate) use offset::inspect_existing_replication_offset;
pub(crate) use offset::inspect_replication_membership;
pub use offset::REPLICATION_OFFSET_STATE_KEY;
pub use position::{
    GtidInterval, GtidSet, GtidSid, MySqlBinlogPosition, MySqlResumePosition, PositionError,
};
pub(crate) use source::{validate_replication_column_plan, MySqlReplicationSource};
pub use validation::{
    validate_replication_prerequisites, MySqlReplicationPrerequisites, BINLOG_CHECKSUM_QUERY,
    BINLOG_FORMAT_QUERY, BINLOG_ROW_IMAGE_QUERY, BINLOG_ROW_METADATA_QUERY,
    BINLOG_ROW_VALUE_OPTIONS_QUERY, BINLOG_TRANSACTION_COMPRESSION_QUERY,
    ENFORCE_GTID_CONSISTENCY_QUERY, GTID_MODE_QUERY, LOG_BIN_QUERY,
};

#[cfg(test)]
mod tests;
