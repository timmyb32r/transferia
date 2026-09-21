use std::io::Cursor;
use std::time::Duration;

use tokio_postgres::types::Type;
use transferia_connector_support::external_request::observe_external_request;

use super::slot_recovery::replication_safety_violation;
use crate::connectors::postgres::common::{connect_owned, OwnedConnection, PostgresConnectionConfig};

/// Exact-source execution ownership. A dedicated READ COMMITTED transaction pins
/// its backend through transaction poolers; transaction-scoped locks disappear
/// with that transaction. Ordinary CDC requests use a different connection.
/// No snapshot or row lock is retained while the lease is idle. Its configured
/// operation deadline also bounds cleanup. A failed ownership check is fatal:
/// never reacquire a lost lease implicitly or acknowledge more WAL afterwards.
pub(crate) struct ReplicationLease {
    connection: OwnedConnection,
    backend_pid: i32,
    keys: [u32; 4],
    timeout: Duration,
}

impl ReplicationLease {
    pub(crate) async fn acquire(
        config: &PostgresConnectionConfig,
        resource_key: &str,
        timeout: Duration,
    ) -> anyhow::Result<Self> {
        let connection = tokio::time::timeout(timeout, connect_owned(config)).await
            .map_err(|_| anyhow::anyhow!("PostgreSQL replication lease connection exceeded its configured deadline"))??
            .with_cancellation_timeout(timeout)?;
        // A hash collision can only reject unrelated executions, never grant
        // conflicting ownership. Both halves retain the existing lease identity.
        let digest = murmur3::murmur3_x64_128(&mut Cursor::new(resource_key.as_bytes()), 0)?;
        let bytes = digest.to_le_bytes();
        let first = i64::from_le_bytes(bytes[..8].try_into()?);
        let second = i64::from_le_bytes(bytes[8..].try_into()?);
        let row = tokio::time::timeout(timeout, async {
            observe_external_request(
                "postgres", "begin_replication_execution_lease",
                connection.batch_execute("BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED READ ONLY"),
            ).await?;
            observe_external_request(
                "postgres", "acquire_replication_execution_lease",
                connection.query_typed_one(
                    "SELECT pg_catalog.pg_backend_pid(), pg_catalog.pg_try_advisory_xact_lock($1) AND pg_catalog.pg_try_advisory_xact_lock($2)",
                    &[(&first, Type::INT8), (&second, Type::INT8)],
                ),
            ).await
        }).await.map_err(|_| anyhow::anyhow!("PostgreSQL replication lease acquisition exceeded its configured deadline"))??;
        anyhow::ensure!(row.try_get::<_, bool>(1)?, "PostgreSQL replication slot execution is already active on the exact source");
        let halves = |value: i64| {
            let bytes = value.to_be_bytes();
            [u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]), u32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]])]
        };
        let [first_class, first_object] = halves(first);
        let [second_class, second_object] = halves(second);
        Ok(Self { connection, backend_pid: row.try_get(0)?, keys: [first_class, first_object, second_class, second_object], timeout })
    }

    /// Acknowledge on the backend that owns the transaction lock. A separate
    /// connection would leave a check/advance race after ownership is lost.
    /// Any SQL/transport error poisons this transaction; do not retry it under
    /// a newly acquired lease or publish another source acknowledgement.
    pub(crate) async fn advance_slot(&self, slot: &str, committed_lsn: u64) -> anyhow::Result<()> {
        tokio::time::timeout(self.timeout,
            super::slot_recovery::advance_slot(&self.connection, slot, committed_lsn),
        ).await
            .map_err(|_| replication_safety_violation(anyhow::anyhow!("PostgreSQL replication slot acknowledgement exceeded its configured deadline")))?
            .map_err(replication_safety_violation)
    }

    pub(crate) async fn ensure_held(&self) -> anyhow::Result<()> {
        let result = tokio::time::timeout(self.timeout, observe_external_request(
            "postgres", "validate_replication_execution_lease",
            self.connection.query_typed_one(
                "SELECT pg_catalog.pg_backend_pid() OPERATOR(pg_catalog.=) $5 AND \
                 EXISTS (SELECT 1 FROM pg_catalog.pg_locks WHERE locktype OPERATOR(pg_catalog.=) 'advisory' AND pid OPERATOR(pg_catalog.=) $5 AND classid OPERATOR(pg_catalog.=) $1 AND objid OPERATOR(pg_catalog.=) $2 AND objsubid OPERATOR(pg_catalog.=) 1 AND granted) AND \
                 EXISTS (SELECT 1 FROM pg_catalog.pg_locks WHERE locktype OPERATOR(pg_catalog.=) 'advisory' AND pid OPERATOR(pg_catalog.=) $5 AND classid OPERATOR(pg_catalog.=) $3 AND objid OPERATOR(pg_catalog.=) $4 AND objsubid OPERATOR(pg_catalog.=) 1 AND granted)",
                &[(&self.keys[0], Type::OID), (&self.keys[1], Type::OID), (&self.keys[2], Type::OID), (&self.keys[3], Type::OID), (&self.backend_pid, Type::INT4)],
            ),
        )).await;
        let held = match result {
            Ok(Ok(row)) => row.try_get::<_, bool>(0).unwrap_or(false),
            _ => false,
        };
        if !held {
            return Err(replication_safety_violation(anyhow::anyhow!(
                "PostgreSQL replication execution lease was lost; refusing further reads or acknowledgements"
            )));
        }
        Ok(())
    }
}
