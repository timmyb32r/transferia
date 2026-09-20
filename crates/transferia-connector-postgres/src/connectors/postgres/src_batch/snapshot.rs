use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

use anyhow::Context as _;
use futures_util::future::BoxFuture;
use tokio::sync::{MappedMutexGuard, Mutex as AsyncMutex, MutexGuard};
use tokio_postgres::Client;

use crate::connectors::postgres::common::{connect_owned, quote_identifier, OwnedConnection, PostgresConnectionConfig};
use crate::connectors::postgres::source::TableConfig;
use crate::connectors::postgres::src_batch_and_stream::ReplicationSlotBootstrap;
use transferia_connector_support::external_request::observe_external_request;

/// Retains relation identity and topology independently of MVCC visibility.
///
/// This transaction is READ COMMITTED: catalog discovery here must not freeze
/// the later exported snapshot before the necessary locks have been obtained.
/// Relation membership is fixed by this guarded discovery, before row-snapshot
/// export. Ordinary leaves need ACCESS SHARE; existing partition/inheritance
/// parents also need SHARE UPDATE EXCLUSIVE while their children are captured.
/// Each selected table retains its own physical OID closure. A first child
/// attached to a previously childless relation later is outside that closure:
/// leaf reads use ONLY, and parent reads filter tableoid to the captured set.
/// ACCESS SHARE on captured children prevents their detachment/removal. This
/// avoids requiring a maintenance-blocking lock on every ordinary leaf.
/// Those stronger locks require privileges beyond SELECT and are announced
/// before acquisition. The guard lives for the entire snapshot epoch.
pub(crate) struct SnapshotGuard {
    _client: OwnedConnection,
    relations: BTreeMap<(String, String), u32>,
    scopes: BTreeMap<(String, String), SnapshotQueryScope>,
}

/// Fixed physical membership for one selected logical table. Identifiers stay
/// unchanged; only PostgreSQL-owned OIDs describe its captured relation set.
#[derive(Clone)]
pub(crate) struct SnapshotQueryScope {
    only: bool,
    relation_oids: Vec<u32>,
}

impl SnapshotQueryScope {
    pub(crate) fn from_sql(&self, schema: &str, table: &str) -> String {
        let qualified = format!("{}.{}", quote_identifier(schema), quote_identifier(table));
        if self.only { format!("ONLY {qualified}") } else { qualified }
    }

    pub(crate) fn membership_predicate(&self) -> String {
        if self.only { return "TRUE".to_owned(); }
        let values = self.relation_oids.iter().map(u32::to_string).collect::<Vec<_>>().join(",");
        format!("tableoid OPERATOR(pg_catalog.=) ANY (ARRAY[{values}]::pg_catalog.oid[])")
    }
}

impl SnapshotGuard {
    pub(crate) async fn acquire(
        config: &PostgresConnectionConfig,
        tables: &[TableConfig],
        cancellation_timeout: Duration,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(!cancellation_timeout.is_zero(), "PostgreSQL snapshot cancellation timeout must be positive");
        let client = observe_external_request("postgres", "connect_snapshot_guard", connect_owned(config)).await?
            .with_cancellation_timeout(cancellation_timeout)?;
        observe_external_request("postgres", "begin_snapshot_guard", client.batch_execute(
            "BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED; SET LOCAL idle_in_transaction_session_timeout = 0"
        )).await?;
        let mut pending = tables.iter().map(|t| (t.schema.clone(), t.name.clone())).collect::<Vec<_>>();
        pending.sort();
        pending.dedup();
        let mut pending = VecDeque::from(pending);
        let mut seen = BTreeSet::new();
        let mut relations = BTreeMap::new();
        let mut children_by_oid = BTreeMap::new();
        while let Some((schema, name)) = pending.pop_front() {
            let qualified = format!("{}.{}", quote_identifier(&schema), quote_identifier(&name));
            observe_external_request("postgres", "lock_snapshot_relation", client.batch_execute(
                &format!("LOCK TABLE {qualified} IN ACCESS SHARE MODE")
            )).await?;
            let row = observe_external_request("postgres", "inspect_snapshot_relation", client.query_one(
                "SELECT c.oid, c.relkind::pg_catalog.text, EXISTS (SELECT 1 FROM pg_catalog.pg_inherits i WHERE i.inhparent OPERATOR(pg_catalog.=) c.oid) \
                 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid OPERATOR(pg_catalog.=) c.relnamespace \
                 WHERE n.nspname OPERATOR(pg_catalog.=) $1 AND c.relname OPERATOR(pg_catalog.=) $2", &[&schema, &name]
            )).await?;
            let oid: u32 = row.try_get(0)?;
            relations.insert((schema.clone(), name.clone()), oid);
            if !seen.insert(oid) { continue; }
            let kind: &str = row.try_get(1)?;
            let has_children: bool = row.try_get(2)?;
            let mut captured_children = Vec::new();
            if kind == "p" || has_children {
                tracing::info!(schema, table = name, relation_oid = oid,
                    lock_mode = "SHARE UPDATE EXCLUSIVE",
                    "Protecting snapshot table membership; topology changes and parent maintenance wait for snapshot completion");
                observe_external_request("postgres", "lock_snapshot_topology", client.batch_execute(
                    &format!("LOCK TABLE ONLY {qualified} IN SHARE UPDATE EXCLUSIVE MODE")
                )).await.context("snapshot topology protection requires SHARE UPDATE EXCLUSIVE privileges")?;
                let children = observe_external_request("postgres", "discover_snapshot_children", client.query(
                    "SELECT n.nspname::pg_catalog.text, c.relname::pg_catalog.text, c.oid FROM pg_catalog.pg_inherits i \
                     JOIN pg_catalog.pg_class c ON c.oid OPERATOR(pg_catalog.=) i.inhrelid \
                     JOIN pg_catalog.pg_namespace n ON n.oid OPERATOR(pg_catalog.=) c.relnamespace \
                     WHERE i.inhparent OPERATOR(pg_catalog.=) $1 ORDER BY c.oid", &[&oid]
                )).await?;
                for child in children {
                    captured_children.push(child.try_get::<_, u32>(2)?);
                    pending.push_back((child.try_get(0)?, child.try_get(1)?));
                }
            }
            children_by_oid.insert(oid, captured_children);
        }
        let mut scopes = BTreeMap::new();
        for table in tables {
            let key = (table.schema.clone(), table.name.clone());
            let root = *relations.get(&key).context("selected snapshot relation was not guarded")?;
            let relation_oids = captured_closure(root, &children_by_oid)?;
            let only = relation_oids.len() == 1;
            tracing::info!(target: "transferia.postgres.snapshot", schema = %table.schema, table = %table.name,
                relation_oid = root, captured_relations = relation_oids.len(), relation_oids = ?relation_oids,
                membership_cutoff = "guarded_discovery_before_snapshot_export", only,
                "Snapshot relation membership fixed; later attached relations are outside this snapshot plan");
            scopes.insert(key, SnapshotQueryScope { only, relation_oids });
        }
        Ok(Self { _client: client, relations, scopes })
    }
}

fn captured_closure(root: u32, children: &BTreeMap<u32, Vec<u32>>) -> anyhow::Result<Vec<u32>> {
    let mut result = BTreeSet::new();
    let mut pending = VecDeque::from([root]);
    while let Some(oid) = pending.pop_front() {
        anyhow::ensure!(oid != 0, "snapshot relation membership contains an invalid OID");
        if !result.insert(oid) { continue; }
        let descendants = children.get(&oid).context("snapshot relation membership has an unguarded child")?;
        pending.extend(descendants.iter().copied());
    }
    Ok(result.into_iter().collect())
}

#[cfg(test)]
#[path = "tests/snapshot_scope.rs"]
mod scope_tests;

#[cfg(test)]
#[path = "tests/snapshot_cancellation.rs"]
mod cancellation_tests;

/// One exported MVCC snapshot owned by the source connector's coordinator.
///
/// The owning transaction deliberately remains open while any table source can
/// exist. Every table reader imports this identifier into its own repeatable-read
/// transaction, so tables cannot observe different points in database history.
pub struct ExportedSnapshot {
    owner: AsyncMutex<Option<OwnedConnection>>,

    replication_owner: Mutex<Option<ReplicationSlotBootstrap>>,

    id: String,

    guard: Mutex<Option<SnapshotGuard>>,

    pub(crate) lsn: i64,

    pub(crate) transaction_id: u64,

    pub(crate) timestamp_ns: i64,
}

/// Borrow the keeper client without allowing it to be replaced independently
/// of its owned driver or the exported snapshot identity.
pub(crate) struct SnapshotClient<'a>(MappedMutexGuard<'a, OwnedConnection>);

impl std::ops::Deref for SnapshotClient<'_> {
    type Target = Client;

    fn deref(&self) -> &Self::Target { &self.0 }
}

impl ExportedSnapshot {
    pub(crate) async fn create(config: &PostgresConnectionConfig, tables: &[TableConfig], cancellation_timeout: Duration) -> anyhow::Result<Arc<Self>> {
        let guard = SnapshotGuard::acquire(config, tables, cancellation_timeout).await?;
        let owner = observe_external_request("postgres", "connect_snapshot_owner", connect_owned(config)).await?
            .with_cancellation_timeout(cancellation_timeout)?;
        observe_external_request("postgres", "begin_snapshot_owner", owner
            .batch_execute(
                "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;\
                 SET LOCAL idle_in_transaction_session_timeout = 0;\
                 SET LOCAL application_name = 'transferia_snapshot_owner';",
            ))
            .await?;
        let result = async {
            let row = observe_external_request("postgres", "export_snapshot", owner
                .query_one(
                    "SELECT pg_catalog.pg_export_snapshot()::pg_catalog.text, \
                            pg_catalog.pg_wal_lsn_diff(pg_catalog.pg_current_wal_lsn(), '0/0')::pg_catalog.int8, \
                            pg_catalog.txid_current()::pg_catalog.text, \
                            (extract(epoch FROM pg_catalog.transaction_timestamp()) OPERATOR(pg_catalog.*) 1000000000)::pg_catalog.int8",
                    &[],
                ))
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
                return Err(error.context("failed to export PostgreSQL snapshot"));
            }
        };
        Ok(Arc::new(Self {
            owner: AsyncMutex::new(Some(owner)),
            replication_owner: Mutex::new(None),
            id,
            guard: Mutex::new(Some(guard)),
            lsn,
            transaction_id,
            timestamp_ns,
        }))
    }

    pub(crate) async fn from_replication_slot(
        config: &PostgresConnectionConfig,
        bootstrap: ReplicationSlotBootstrap,
        guard: SnapshotGuard,
        cancellation_timeout: Duration,
    ) -> anyhow::Result<Arc<Self>> {
        anyhow::ensure!(!cancellation_timeout.is_zero(), "PostgreSQL snapshot cancellation timeout must be positive");
        let owner = observe_external_request("postgres", "connect_slot_snapshot_owner", connect_owned(config)).await?
            .with_cancellation_timeout(cancellation_timeout)?;
        let id = bootstrap.snapshot.clone();
        import_snapshot(&owner, &id).await?;
        observe_external_request("postgres", "configure_snapshot_owner", owner.batch_execute(
            "SET LOCAL application_name = 'transferia_snapshot_owner'; SET LOCAL idle_in_transaction_session_timeout = 0"
        )).await?;
        let metadata = observe_external_request("postgres", "read_slot_snapshot_metadata", owner
            .query_one(
                "SELECT pg_catalog.txid_current()::pg_catalog.text, \
                        (extract(epoch FROM pg_catalog.transaction_timestamp()) OPERATOR(pg_catalog.*) 1000000000)::pg_catalog.int8",
                &[],
            ))
            .await?;
        let lsn = i64::try_from(bootstrap.consistent_lsn)
            .context("PostgreSQL slot consistent LSN exceeds signed source offset range")?;
        Ok(Arc::new(Self {
            owner: AsyncMutex::new(Some(owner)),
            replication_owner: Mutex::new(Some(bootstrap)),
            id,
            guard: Mutex::new(Some(guard)),
            lsn,
            transaction_id: metadata.try_get::<_, &str>(0)?.parse::<u64>()?,
            timestamp_ns: metadata.try_get(1)?,
        }))
    }

    pub(crate) async fn import(&self, client: &Client) -> anyhow::Result<()> {
        import_snapshot(client, &self.id).await
    }

    pub(crate) fn verify_relation(&self, schema: &str, name: &str, oid: u32) -> anyhow::Result<()> {
        let guard = self.guard.lock().map_err(|_| anyhow::anyhow!("PostgreSQL snapshot guard state is poisoned"))?;
        let guard = guard.as_ref().ok_or_else(|| anyhow::anyhow!("PostgreSQL snapshot guard is closed"))?;
        anyhow::ensure!(
            guard.relations.get(&(schema.to_owned(), name.to_owned())) == Some(&oid),
            "PostgreSQL snapshot relation identity changed for {schema}.{name}"
        );
        Ok(())
    }

    pub(crate) fn query_scope(&self, schema: &str, name: &str, oid: u32) -> anyhow::Result<SnapshotQueryScope> {
        self.verify_relation(schema, name, oid)?;
        let guard = self.guard.lock().map_err(|_| anyhow::anyhow!("PostgreSQL snapshot guard state is poisoned"))?;
        let guard = guard.as_ref().context("PostgreSQL snapshot guard is closed")?;
        guard.scopes.get(&(schema.to_owned(), name.to_owned())).cloned()
            .context("PostgreSQL snapshot has no captured scope for the selected table")
    }

    /// Cancel an epoch without queuing ROLLBACK behind an abandoned probe.
    /// The caller must first drop any future borrowing `client()`. Taking and
    /// cancelling owned connections sends PostgreSQL CancelRequest before their
    /// drivers close: TCP closure alone may not interrupt server lock waits.
    /// Both requests share the configured cleanup deadline. Failure closes the
    /// local drivers and reports that server cancellation could not be sent.
    /// No new snapshot replaces this epoch.
    pub(crate) async fn abort(&self, cancellation_timeout: Duration) -> anyhow::Result<()> {
        anyhow::ensure!(!cancellation_timeout.is_zero(), "PostgreSQL snapshot cancellation timeout must be positive");
        let owner = self.owner.lock().await.take();
        let mut poisoned = false;
        let replication_owner = match self.replication_owner.lock() {
            Ok(mut owner) => owner.take(),
            Err(error) => { poisoned = true; error.into_inner().take() }
        };
        let guard = match self.guard.lock() {
            Ok(mut guard) => guard.take(),
            Err(error) => { poisoned = true; error.into_inner().take() }
        };
        drop(replication_owner);
        let cancelled = tokio::time::timeout(cancellation_timeout, async move {
            tokio::join!(
                async move { if let Some(mut owner) = owner { owner.cancel().await } else { Ok(()) } },
                async move { if let Some(mut guard) = guard { guard._client.cancel().await } else { Ok(()) } },
            )
        }).await.context("PostgreSQL snapshot cancellation exceeded its configured deadline; local drivers closed")?;
        cancelled.0.context("failed to send PostgreSQL snapshot owner cancellation")?;
        cancelled.1.context("failed to send PostgreSQL snapshot guard cancellation")?;
        tracing::info!(target: "transferia.postgres.snapshot", reason = "epoch_aborted", "Snapshot epoch cancellation requests sent and local connections closed; pending queries were not drained");
        anyhow::ensure!(!poisoned, "PostgreSQL snapshot state was poisoned during cancellation; all connections were aborted");
        Ok(())
    }

    pub(crate) async fn close(
        &self,
        operation_timeout: Duration,
    ) -> anyhow::Result<()> {
        let replication_owner = self
            .replication_owner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        let owner = self.owner.lock().await.take();
        let guard = self.guard.lock().map_err(|_| anyhow::anyhow!("PostgreSQL snapshot guard state is poisoned"))?.take();
        let result = if let Some(owner) = owner { close_owner_with_timeout(
            owner,
            |owner| {
                Box::pin(async move {
                    observe_external_request("postgres", "finish_snapshot_owner", owner
                        .batch_execute("ROLLBACK"))
                        .await
                        .map_err(anyhow::Error::from)?;
                    owner.disarm_cancellation();
                    Ok(())
                })
            },
            operation_timeout,
        )
        .await } else { Ok(()) };
        drop(replication_owner);
        let guard_result = if let Some(guard) = guard { close_owner_with_timeout(
            guard._client,
            |client| Box::pin(async move {
                observe_external_request("postgres", "finish_snapshot_guard", client.batch_execute("ROLLBACK"))
                    .await.map_err(anyhow::Error::from)?;
                client.disarm_cancellation();
                Ok(())
            }),
            operation_timeout,
        ).await } else { Ok(()) };
        result?;
        guard_result
    }

    pub(crate) async fn client(&self) -> anyhow::Result<SnapshotClient<'_>> {
        MutexGuard::try_map(self.owner.lock().await, Option::as_mut)
            .map(SnapshotClient)
            .map_err(|_| anyhow::Error::new(transferia_core::failure::DataPlaneFailure::fatal(
                anyhow::anyhow!("PostgreSQL snapshot owner is already closed; this epoch cannot be restarted")
            )))
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
    observe_external_request("postgres", "begin_snapshot_reader", client
        .batch_execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        .await
        .map_err(|error| transferia_core::failure::DataPlaneFailure::retryable(error.into()))?;
    observe_external_request("postgres", "import_snapshot", client
        .batch_execute(
            &set_snapshot_sql(id).map_err(transferia_core::failure::DataPlaneFailure::fatal)?,
        ))
        .await
        .map_err(classify_snapshot_import_error)?;
    observe_external_request("postgres", "configure_snapshot_reader", client
        .batch_execute(
            "SET LOCAL DateStyle = 'ISO, YMD';\
                 SET LOCAL IntervalStyle = 'postgres';\
                 SET LOCAL TimeZone = 'UTC';\
                 SET LOCAL bytea_output = 'hex';\
                 SET LOCAL extra_float_digits = 3;",
        ))
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
