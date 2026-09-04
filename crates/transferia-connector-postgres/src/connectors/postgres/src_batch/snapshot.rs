use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Context as _;
use futures_util::future::BoxFuture;
use tokio::sync::{MappedMutexGuard, Mutex as AsyncMutex, MutexGuard};
use tokio_postgres::Client;

use crate::connectors::postgres::common::{connect, PostgresConnectionConfig};
use crate::connectors::postgres::src_batch_and_stream::ReplicationSlotBootstrap;

/// One exported MVCC snapshot owned by the source connector's coordinator.
///
/// The owning transaction deliberately remains open while any table source can
/// exist. Every table reader imports this identifier into its own repeatable-read
/// transaction, so tables cannot observe different points in database history.
pub struct ExportedSnapshot {
    owner: AsyncMutex<Option<Client>>,

    replication_owner: Mutex<Option<ReplicationSlotBootstrap>>,

    id: String,

    pub(crate) lsn: i64,

    pub(crate) transaction_id: u64,

    pub(crate) timestamp_ns: i64,
}

impl ExportedSnapshot {
    pub(crate) async fn create(config: &PostgresConnectionConfig) -> anyhow::Result<Arc<Self>> {
        let owner = connect(config).await?;
        owner
            .batch_execute(
                "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;\
                 SET LOCAL idle_in_transaction_session_timeout = 0;",
            )
            .await?;
        let result = async {
            let row = owner
                .query_one(
                    "SELECT pg_export_snapshot()::text, \
                            pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::bigint, \
                            txid_current()::text, \
                            (extract(epoch FROM transaction_timestamp()) * 1000000000)::bigint",
                    &[],
                )
                .await?;
            let id = row.try_get::<_, String>(0)?;
            validate_snapshot_id(&id)?;
            Ok::<_, anyhow::Error>((
                id,
                row.try_get::<_, i64>(1)?,
                row.try_get::<_, &str>(2)?.parse::<u64>()?,
                row.try_get::<_, i64>(3)?,
            ))
        }
        .await;
        let (id, lsn, transaction_id, timestamp_ns) = match result {
            Ok(snapshot) => snapshot,
            Err(error) => {
                drop(owner.batch_execute("ROLLBACK").await);
                return Err(error.context("failed to export PostgreSQL snapshot"));
            }
        };
        Ok(Arc::new(Self {
            owner: AsyncMutex::new(Some(owner)),
            replication_owner: Mutex::new(None),
            id,
            lsn,
            transaction_id,
            timestamp_ns,
        }))
    }

    pub(crate) async fn from_replication_slot(
        config: &PostgresConnectionConfig,
        bootstrap: ReplicationSlotBootstrap,
    ) -> anyhow::Result<Arc<Self>> {
        let owner = connect(config).await?;
        let id = bootstrap.snapshot.clone();
        import_snapshot(&owner, &id).await?;
        let metadata = owner
            .query_one(
                "SELECT txid_current()::text, \
                        (extract(epoch FROM transaction_timestamp()) * 1000000000)::bigint",
                &[],
            )
            .await?;
        let lsn = i64::try_from(bootstrap.consistent_lsn)
            .context("PostgreSQL slot consistent LSN exceeds signed source offset range")?;
        Ok(Arc::new(Self {
            owner: AsyncMutex::new(Some(owner)),
            replication_owner: Mutex::new(Some(bootstrap)),
            id,
            lsn,
            transaction_id: metadata.try_get::<_, &str>(0)?.parse::<u64>()?,
            timestamp_ns: metadata.try_get(1)?,
        }))
    }

    pub(crate) async fn import(&self, client: &Client) -> anyhow::Result<()> {
        import_snapshot(client, &self.id).await
    }

    pub(crate) async fn close_replication_owner(
        &self,
        operation_timeout: Duration,
    ) -> anyhow::Result<()> {
        let replication_owner = self
            .replication_owner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        let Some(replication_owner) = replication_owner else {
            return Ok(());
        };
        let owner = self
            .owner
            .lock()
            .await
            .take()
            .ok_or_else(|| anyhow::anyhow!("PostgreSQL snapshot owner is already closed"))?;
        let result = close_owner_with_timeout(
            owner,
            |owner| {
                Box::pin(async move {
                    owner
                        .batch_execute("ROLLBACK")
                        .await
                        .map_err(anyhow::Error::from)
                })
            },
            operation_timeout,
        )
        .await;
        drop(replication_owner);
        result
    }

    pub(crate) async fn client(&self) -> anyhow::Result<MappedMutexGuard<'_, Client>> {
        MutexGuard::try_map(self.owner.lock().await, Option::as_mut)
            .map_err(|_| anyhow::anyhow!("PostgreSQL snapshot owner is already closed"))
    }
}

pub(super) async fn close_owner_with_timeout<T, F>(
    owner: T,
    rollback: F,
    operation_timeout: Duration,
) -> anyhow::Result<()>
where
    T: Sync,
    F: for<'a> FnOnce(&'a T) -> BoxFuture<'a, anyhow::Result<()>>,
{
    tokio::time::timeout(operation_timeout, rollback(&owner))
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "PostgreSQL snapshot owner cleanup timed out after {} ms",
                operation_timeout.as_millis()
            )
        })?
}

async fn import_snapshot(client: &Client, id: &str) -> anyhow::Result<()> {
    client
        .batch_execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .await
        .map_err(|error| transferia_core::failure::DataPlaneFailure::retryable(error.into()))?;
    client
        .batch_execute(
            &set_snapshot_sql(id).map_err(transferia_core::failure::DataPlaneFailure::fatal)?,
        )
        .await
        .map_err(classify_snapshot_import_error)?;
    client
        .batch_execute(
            "SET LOCAL DateStyle = 'ISO, YMD';\
                 SET LOCAL IntervalStyle = 'postgres';\
                 SET LOCAL TimeZone = 'UTC';\
                 SET LOCAL bytea_output = 'hex';\
                 SET LOCAL extra_float_digits = 3;",
        )
        .await
        .map_err(|error| transferia_core::failure::DataPlaneFailure::retryable(error.into()))?;
    Ok(())
}

fn classify_snapshot_import_error(
    error: tokio_postgres::Error,
) -> transferia_core::failure::DataPlaneFailure {
    let fatal = error.as_db_error().is_some_and(|database| {
        matches!(
            *database.code(),
            tokio_postgres::error::SqlState::UNDEFINED_OBJECT
                | tokio_postgres::error::SqlState::INVALID_PARAMETER_VALUE
                | tokio_postgres::error::SqlState::ACTIVE_SQL_TRANSACTION
                | tokio_postgres::error::SqlState::FEATURE_NOT_SUPPORTED
        )
    });
    if fatal {
        return transferia_core::failure::DataPlaneFailure::fatal(anyhow::anyhow!(
            "PostgreSQL exported snapshot is no longer importable"
        ));
    }
    transferia_core::failure::DataPlaneFailure::retryable(error.into())
}

fn validate_snapshot_id(id: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        !id.is_empty()
            && id.len() <= 128
            && id
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() || byte == b'-'),
        "PostgreSQL exported an invalid snapshot identifier"
    );
    Ok(())
}

pub(super) fn set_snapshot_sql(id: &str) -> anyhow::Result<String> {
    validate_snapshot_id(id)?;
    Ok(format!("SET TRANSACTION SNAPSHOT '{id}'"))
}
