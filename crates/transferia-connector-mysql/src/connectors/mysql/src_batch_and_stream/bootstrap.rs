use core::future::Future;
use std::collections::HashSet;
use std::time::{Duration, Instant};

use mysql_async::prelude::{ColumnIndex, FromValue, Queryable};
use mysql_async::{Conn, Row};
use tokio_util::sync::CancellationToken;
use transferia_connector_support::external_request::observe_external_request;

use super::{
    replication_safety_violation, AuthoritativeColumnIdentity, AuthoritativeTableIdentity,
    MySqlBinlogBoundary, MySqlSourceIdentity, SnapshotStreamTracker, validate_server_uuid,
};
use crate::connectors::mysql::common::{
    connect, connect_with_max_allowed_packet, quote_identifier, validate_identifier,
    validate_mysql_client_packet_limit, MySqlConnectionConfig,
};
use crate::connectors::mysql::src_batch::{
    column_generation, column_visibility, has_column_type_modifier, has_extra_modifier,
    parse_enum_set_values, validate_structured_column_metadata, TableConfig,
    MYSQL_CANONICAL_SNAPSHOT_SQL_MODE,
};
use crate::connectors::mysql::src_stream::{
    validate_replication_prerequisites, GtidSet, MySqlReplicationPrerequisites,
};

const MYSQL_LOCK_NAME_MAX_BYTES: usize = 64;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MySqlReplicationPreflight {
    pub source: MySqlSourceIdentity,

    pub server_version: String,

    binary_log_status_query: &'static str,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MySqlGtidState {
    pub executed: GtidSet,

    pub purged: GtidSet,
}

#[must_use = "dropping a snapshot session closes its connection-owned consistent snapshot"]
pub struct MySqlSnapshotSession {
    table: TableConfig,
    session_connection_id: u64,
    max_row_bytes: usize,
    connection: Conn,
}

impl MySqlSnapshotSession {
    #[must_use]
    pub fn table(&self) -> &TableConfig {
        &self.table
    }

    /// The MySQL session which owns this exact read-only consistent snapshot.
    ///
    /// MySQL does not expose a portable durable transaction identifier for a
    /// read-only InnoDB snapshot. The retained connection plus this server
    /// connection id is its exact lifetime identity.
    #[must_use]
    pub const fn session_connection_id(&self) -> u64 {
        self.session_connection_id
    }

    #[must_use]
    pub const fn max_row_bytes(&self) -> usize {
        self.max_row_bytes
    }

    #[must_use]
    pub fn into_parts(self) -> (TableConfig, u64, usize, Conn) {
        (
            self.table,
            self.session_connection_id,
            self.max_row_bytes,
            self.connection,
        )
    }
}

#[must_use = "dropping the owner connection releases the MySQL execution lock"]
pub struct MySqlExecutionLock {
    lock_name: String,
    connection: Option<Conn>,
    verified: bool,
}

impl MySqlExecutionLock {
    #[must_use]
    pub fn lock_name(&self) -> &str {
        &self.lock_name
    }

    pub async fn verify(
        &mut self,
        request_timeout: Duration,
        cancellation: &CancellationToken,
    ) -> anyhow::Result<()> {
        validate_timeout("request_timeout", request_timeout)
            .map_err(replication_safety_violation)?;
        self.verified = false;
        let connection = self.connection.as_mut().ok_or_else(|| {
            replication_safety_violation(anyhow::anyhow!(
                "MySQL replication execution lock was already released"
            ))
        })?;
        let held = run_request(
            cancellation,
            request_timeout,
            "verify_replication_lock",
            connection.exec_first::<Option<u8>, _, _>(
                "SELECT IS_USED_LOCK(?) = CONNECTION_ID()",
                (&self.lock_name,),
            ),
        )
        .await?
        .flatten();
        if held != Some(1) {
            return Err(replication_safety_violation(anyhow::anyhow!(
                "MySQL replication execution lock '{}' is no longer owned by this execution",
                self.lock_name
            )));
        }
        self.verified = true;
        Ok(())
    }

    /// Read the source's complete executed and purged GTID sets while proving
    /// that this retained connection still owns the execution fence.
    pub async fn read_gtid_state(
        &mut self,
        request_timeout: Duration,
        cancellation: &CancellationToken,
    ) -> anyhow::Result<MySqlGtidState> {
        validate_timeout("request_timeout", request_timeout)
            .map_err(replication_safety_violation)?;
        self.verified = false;
        let connection = self.connection.as_mut().ok_or_else(|| {
            replication_safety_violation(anyhow::anyhow!(
                "MySQL replication execution lock was already released"
            ))
        })?;
        let row = run_request(
            cancellation,
            request_timeout,
            "read_gtid_state",
            connection.exec_first::<Row, _, _>(
                "SELECT IS_USED_LOCK(?) = CONNECTION_ID(), @@GLOBAL.gtid_executed, @@GLOBAL.gtid_purged",
                (&self.lock_name,),
            ),
        )
        .await?
        .ok_or_else(|| {
            replication_safety_violation(anyhow::anyhow!(
                "MySQL GTID state query returned no row"
            ))
        })?;
        let held = required::<Option<u8>, _>(&row, 0, "execution lock ownership")?;
        let executed = required::<String, _>(&row, 1, "@@GLOBAL.gtid_executed")?;
        let purged = required::<String, _>(&row, 2, "@@GLOBAL.gtid_purged")?;
        let state = validate_locked_gtid_state(&self.lock_name, held, &executed, &purged)
            .map_err(replication_safety_violation)?;
        self.verified = true;
        Ok(state)
    }

    /// Capture a boundary on the retained execution-lock connection.
    ///
    /// FTWRL makes the authoritative table identities, filename, position,
    /// executed GTID set, and source timestamp one coherent point even while
    /// application writers are active. The identities must exactly match the
    /// discovery result supplied by the caller.
    pub async fn capture_boundary(
        &mut self,
        preflight: &MySqlReplicationPreflight,
        tables: &[TableConfig],
        expected_authoritative_tables: &[AuthoritativeTableIdentity],
        request_timeout: Duration,
        cancellation: &CancellationToken,
    ) -> anyhow::Result<MySqlBinlogBoundary> {
        validate_timeout("request_timeout", request_timeout)
            .map_err(replication_safety_violation)?;
        validate_authoritative_table_selection(
            &preflight.source.database,
            tables,
            expected_authoritative_tables,
        )
        .map_err(replication_safety_violation)?;
        self.verify(request_timeout, cancellation).await?;
        self.verified = false;
        let lock_tables = {
            let connection = self.connection.as_mut().ok_or_else(|| {
                replication_safety_violation(anyhow::anyhow!(
                    "MySQL replication execution lock was already released"
                ))
            })?;
            run_request(
                cancellation,
                request_timeout,
                "lock_current_stream_boundary",
                connection.query_drop("FLUSH TABLES WITH READ LOCK"),
            )
            .await
        };
        if let Err(primary) = lock_tables {
            let unlock = self.unlock_tables(request_timeout).await;
            return match unlock {
                Ok(()) => Err(primary),
                Err(cleanup) => Err(replication_safety_violation(anyhow::anyhow!(
                    "MySQL current stream boundary lock failed and bounded UNLOCK/drop also failed: {cleanup}; original failure: {primary}"
                ))),
            };
        }

        let boundary = {
            let connection = self.connection.as_mut().ok_or_else(|| {
                replication_safety_violation(anyhow::anyhow!(
                    "MySQL replication execution lock was already released"
                ))
            })?;
            async {
                let locked_preflight =
                    read_preflight(connection, request_timeout, cancellation).await?;
                if locked_preflight != *preflight {
                    return Err(replication_safety_violation(anyhow::anyhow!(
                        "MySQL source identity or replication settings changed before capturing the stream boundary"
                    )));
                }
                let authoritative_tables = read_authoritative_table_identities(
                    connection,
                    &preflight.source.database,
                    tables,
                    request_timeout,
                    cancellation,
                )
                .await?;
                validate_authoritative_tables_unchanged(
                    expected_authoritative_tables,
                    &authoritative_tables,
                )
                .map_err(replication_safety_violation)?;
                read_boundary(connection, preflight, request_timeout, cancellation).await
            }
            .await
        };
        let unlock = self.unlock_tables(request_timeout).await;
        match (boundary, unlock) {
            (Ok(boundary), Ok(())) => {
                self.verified = true;
                Ok(boundary)
            }
            (Err(error), Ok(())) => Err(error),
            (Ok(_), Err(error)) => Err(replication_safety_violation(error.context(
                "MySQL current stream boundary was captured, but UNLOCK TABLES did not complete",
            ))),
            (Err(primary), Err(cleanup)) => Err(replication_safety_violation(anyhow::anyhow!(
                "MySQL current stream boundary failed and UNLOCK TABLES also failed: {cleanup}; original failure: {primary}"
            ))),
        }
    }

    async fn unlock_tables(&mut self, request_timeout: Duration) -> anyhow::Result<()> {
        let unlock = {
            let connection = self.connection.as_mut().ok_or_else(|| {
                replication_safety_violation(anyhow::anyhow!(
                    "MySQL replication execution lock lost its owner while unlocking tables"
                ))
            })?;
            run_bounded_request(
                request_timeout,
                "unlock_current_stream_boundary",
                connection.query_drop("UNLOCK TABLES"),
            )
            .await
        };
        if let Err(primary) = unlock {
            let Some(connection) = self.connection.take() else {
                return Err(primary);
            };
            let disconnect = run_bounded_request(
                request_timeout,
                "drop_failed_stream_boundary_lock",
                connection.disconnect(),
            )
            .await;
            return match disconnect {
                Ok(()) => Err(primary),
                Err(cleanup) => Err(replication_safety_violation(anyhow::anyhow!(
                    "MySQL UNLOCK TABLES failed and bounded connection drop also failed: {cleanup}; original failure: {primary}"
                ))),
            };
        }
        Ok(())
    }

    /// Hand the still-locked connection to the binlog transport.
    ///
    /// The caller must not reconnect: MySQL named locks are connection-owned.
    pub fn into_connection(mut self) -> anyhow::Result<Conn> {
        if !self.verified {
            return Err(replication_safety_violation(anyhow::anyhow!(
                "MySQL replication execution lock must be verified before connection handoff"
            )));
        }
        self.connection.take().ok_or_else(|| {
            replication_safety_violation(anyhow::anyhow!(
                "MySQL replication execution lock was already released"
            ))
        })
    }

    pub async fn release(mut self, cleanup_timeout: Duration) -> anyhow::Result<()> {
        validate_timeout("cleanup_timeout", cleanup_timeout)
            .map_err(replication_safety_violation)?;
        let lock_name = self.lock_name.clone();
        let Some(mut connection) = self.connection.take() else {
            return Ok(());
        };
        let cleanup_started = Instant::now();
        let release = match cleanup_remaining(cleanup_started, cleanup_timeout) {
            Some(remaining) => run_bounded_request(
                remaining,
                "release_replication_lock",
                connection.exec_first::<Option<u8>, _, _>("SELECT RELEASE_LOCK(?)", (&lock_name,)),
            )
            .await
            .and_then(|released| {
                if released.flatten() == Some(1) {
                    Ok(())
                } else {
                    Err(replication_safety_violation(anyhow::anyhow!(
                        "MySQL replication execution lock was not owned while releasing it"
                    )))
                }
            }),
            None => Err(cleanup_deadline_exceeded("execution-lock cleanup")),
        };
        let disconnect = match cleanup_remaining(cleanup_started, cleanup_timeout) {
            Some(remaining) => run_bounded_request(
                remaining,
                "disconnect_replication_lock",
                connection.disconnect(),
            )
            .await,
            None => Err(cleanup_deadline_exceeded("execution-lock cleanup")),
        };
        match (release, disconnect) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
            (Err(primary), Err(cleanup)) => Err(replication_safety_violation(anyhow::anyhow!(
                "MySQL replication-lock release failed and bounded disconnect also failed: {cleanup}; original failure: {primary}"
            ))),
        }
    }
}

#[must_use = "dropping the bootstrap closes all connection-owned snapshot resources"]
pub struct LockedSnapshotBootstrap {
    resources: BootstrapResources,
    source: MySqlSourceIdentity,
    boundary: MySqlBinlogBoundary,
    authoritative_tables: Vec<AuthoritativeTableIdentity>,
}

impl LockedSnapshotBootstrap {
    #[must_use]
    pub fn source(&self) -> &MySqlSourceIdentity {
        &self.source
    }

    #[must_use]
    pub fn boundary(&self) -> &MySqlBinlogBoundary {
        &self.boundary
    }

    #[must_use]
    pub fn authoritative_tables(&self) -> &[AuthoritativeTableIdentity] {
        &self.authoritative_tables
    }

    /// Persist the exact snapshot boundary before allowing writes to resume.
    /// Once the durable CAS succeeds, cancellation no longer interrupts the
    /// bounded `UNLOCK TABLES` cleanup attempt.
    pub async fn persist_and_unlock(
        mut self,
        tracker: &mut SnapshotStreamTracker,
        request_timeout: Duration,
        cleanup_timeout: Duration,
        cancellation: &CancellationToken,
    ) -> anyhow::Result<PreparedMySqlSnapshot> {
        validate_timeout("request_timeout", request_timeout)
            .map_err(replication_safety_violation)?;
        validate_timeout("cleanup_timeout", cleanup_timeout)
            .map_err(replication_safety_violation)?;
        if cancellation.is_cancelled() {
            let error = anyhow::anyhow!(
                "MySQL exact snapshot bootstrap cancelled before durable boundary CAS"
            );
            return Err(cleanup_after_failure(&mut self.resources, cleanup_timeout, error).await);
        }
        if let Err(error) = tracker
            .mark_snapshot_ready(&self.boundary, &self.authoritative_tables)
            .await
        {
            return Err(
                cleanup_after_failure(&mut self.resources, cleanup_timeout, error).await,
            );
        }

        let unlock = {
            let connection = self.resources.owner.as_mut().ok_or_else(|| {
                replication_safety_violation(anyhow::anyhow!(
                    "MySQL exact snapshot bootstrap lost its lock connection"
                ))
            })?;
            run_bounded_request(
                request_timeout,
                "unlock_snapshot_tables",
                connection.query_drop("UNLOCK TABLES"),
            )
            .await
        };
        if let Err(error) = unlock {
            let error = replication_safety_violation(error.context(
                "MySQL exact snapshot boundary was persisted, but UNLOCK TABLES did not complete",
            ));
            return Err(
                cleanup_after_failure(&mut self.resources, cleanup_timeout, error).await,
            );
        }
        self.resources.tables_locked = false;

        let owner = self.resources.owner.take().ok_or_else(|| {
            replication_safety_violation(anyhow::anyhow!(
                "MySQL exact snapshot bootstrap lost its execution-lock connection"
            ))
        })?;
        self.resources.lock_acquired = false;
        Ok(PreparedMySqlSnapshot {
            source: self.source,
            boundary: self.boundary,
            authoritative_tables: self.authoritative_tables,
            execution_lock: MySqlExecutionLock {
                lock_name: self.resources.lock_name,
                connection: Some(owner),
                verified: true,
            },
            sessions: std::mem::take(&mut self.resources.sessions),
        })
    }

    pub async fn abort(mut self, cleanup_timeout: Duration) -> anyhow::Result<()> {
        validate_timeout("cleanup_timeout", cleanup_timeout)
            .map_err(replication_safety_violation)?;
        self.resources.cleanup(cleanup_timeout).await
    }
}

#[must_use = "the retained snapshot sessions and execution lock must be handed to readers"]
pub struct PreparedMySqlSnapshot {
    pub source: MySqlSourceIdentity,

    pub boundary: MySqlBinlogBoundary,

    pub authoritative_tables: Vec<AuthoritativeTableIdentity>,

    pub execution_lock: MySqlExecutionLock,

    pub sessions: Vec<MySqlSnapshotSession>,
}

pub async fn inspect_mysql8_gtid_source(
    config: &MySqlConnectionConfig,
    request_timeout: Duration,
    cancellation: &CancellationToken,
) -> anyhow::Result<MySqlReplicationPreflight> {
    validate_timeout("request_timeout", request_timeout).map_err(replication_safety_violation)?;
    config.validate().map_err(replication_safety_violation)?;
    let mut connection = run_request(
        cancellation,
        request_timeout,
        "connect_replication_preflight",
        connect(config),
    )
    .await?;
    let result = read_preflight(&mut connection, request_timeout, cancellation).await;
    let disconnect = run_bounded_request(
        request_timeout,
        "disconnect_replication_preflight",
        connection.disconnect(),
    )
    .await;
    match (result, disconnect) {
        (Ok(preflight), Ok(())) => Ok(preflight),
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "lock wait and request timeouts are independent explicit safety inputs"
)]
pub async fn acquire_execution_lock(
    config: &MySqlConnectionConfig,
    server_id: u32,
    preflight: &MySqlReplicationPreflight,
    lock_timeout: Duration,
    request_timeout: Duration,
    cancellation: &CancellationToken,
) -> anyhow::Result<MySqlExecutionLock> {
    config.validate().map_err(replication_safety_violation)?;
    validate_timeout("lock_timeout", lock_timeout).map_err(replication_safety_violation)?;
    validate_timeout("request_timeout", request_timeout).map_err(replication_safety_violation)?;
    if server_id == 0 {
        return Err(replication_safety_violation(anyhow::anyhow!(
            "MySQL replication server_id must be non-zero"
        )));
    }
    if preflight.source.database != config.database {
        return Err(replication_safety_violation(anyhow::anyhow!(
            "MySQL replication preflight belongs to database '{}', configuration selects '{}'",
            preflight.source.database,
            config.database
        )));
    }
    let lock_name = replication_lock_name(&preflight.source.server_uuid, server_id)?;
    let mut connection = run_request(
        cancellation,
        request_timeout,
        "connect_replication_lock",
        connect(config),
    )
    .await?;
    let acquired = run_request(
        cancellation,
        request_timeout.saturating_add(lock_timeout),
        "acquire_replication_lock",
        connection.exec_first::<Option<u8>, _, _>(
            "SELECT GET_LOCK(?, ?)",
            (&lock_name, lock_timeout.as_secs_f64()),
        ),
    )
    .await;
    let acquired = match acquired {
        Ok(acquired) => acquired.flatten(),
        Err(error) => {
            return Err(disconnect_after_failure(connection, request_timeout, error).await);
        }
    };
    if acquired != Some(1) {
        let error = anyhow::anyhow!(
            "MySQL replication execution lock '{}' was not acquired within the configured timeout",
            lock_name
        );
        return Err(disconnect_after_failure(connection, request_timeout, error).await);
    }

    match read_preflight(&mut connection, request_timeout, cancellation).await {
        Ok(locked_preflight) if locked_preflight == *preflight => {}
        Ok(_) => {
            let error = replication_safety_violation(anyhow::anyhow!(
                "MySQL source identity or replication settings changed while acquiring the execution lock"
            ));
            let lock = MySqlExecutionLock {
                lock_name,
                connection: Some(connection),
                verified: true,
            };
            return Err(release_after_failure(lock, request_timeout, error).await);
        }
        Err(error) => {
            let lock = MySqlExecutionLock {
                lock_name,
                connection: Some(connection),
                verified: true,
            };
            return Err(release_after_failure(lock, request_timeout, error).await);
        }
    }
    Ok(MySqlExecutionLock {
        lock_name,
        connection: Some(connection),
        verified: true,
    })
}

async fn read_authoritative_table_identities(
    connection: &mut Conn,
    database: &str,
    tables: &[TableConfig],
    request_timeout: Duration,
    cancellation: &CancellationToken,
) -> anyhow::Result<Vec<AuthoritativeTableIdentity>> {
    let mut identities = Vec::with_capacity(tables.len());
    for table in tables {
        identities.push(
            read_authoritative_table_identity(
                connection,
                database,
                table,
                request_timeout,
                cancellation,
            )
            .await?,
        );
    }
    Ok(identities)
}

async fn read_authoritative_table_identity(
    connection: &mut Conn,
    database: &str,
    table: &TableConfig,
    request_timeout: Duration,
    cancellation: &CancellationToken,
) -> anyhow::Result<AuthoritativeTableIdentity> {
    let table_row = run_request(
        cancellation,
        request_timeout,
        "read_authoritative_table_identity",
        connection.exec_first::<Row, _, _>(
            "SELECT ENGINE, TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND TABLE_TYPE = 'BASE TABLE'",
            (database, table.name.as_str()),
        ),
    )
    .await?
    .ok_or_else(|| {
        replication_safety_violation(anyhow::anyhow!(
            "MySQL authoritative table metadata returned no row for '{}.{}'",
            database,
            table.name
        ))
    })?;
    let engine: String = required(&table_row, 0, "table engine")?;
    let actual_database: String = required(&table_row, 1, "table database")?;
    let actual_table: String = required(&table_row, 2, "table name")?;
    if actual_database != database || actual_table != table.name {
        return Err(replication_safety_violation(anyhow::anyhow!(
            "MySQL authoritative metadata resolved configured table '{}.{}' to distinct identifier '{}.{}'",
            database,
            table.name,
            actual_database,
            actual_table
        )));
    }
    if !engine.eq_ignore_ascii_case("InnoDB") {
        return Err(replication_safety_violation(anyhow::anyhow!(
            "MySQL replication table '{}.{}' uses storage engine '{}'; replication requires InnoDB",
            database,
            table.name,
            engine
        )));
    }
    let column_rows = run_request(
        cancellation,
        request_timeout,
        "read_authoritative_column_identities",
        connection.exec::<Row, _, _>(
            "SELECT c.COLUMN_NAME, c.DATA_TYPE, c.COLUMN_TYPE, c.IS_NULLABLE, c.CHARACTER_SET_NAME, c.COLLATION_NAME, col.ID, col.PAD_ATTRIBUTE, c.EXTRA, c.GENERATION_EXPRESSION, c.CHARACTER_MAXIMUM_LENGTH, c.CHARACTER_OCTET_LENGTH, c.NUMERIC_PRECISION, c.NUMERIC_SCALE, c.DATETIME_PRECISION, c.SRS_ID, s.SEQ_IN_INDEX, s.SUB_PART, s.COLLATION FROM information_schema.COLUMNS AS c LEFT JOIN information_schema.COLLATIONS AS col ON col.COLLATION_NAME = c.COLLATION_NAME LEFT JOIN information_schema.STATISTICS AS s ON s.TABLE_SCHEMA = c.TABLE_SCHEMA AND s.TABLE_NAME = c.TABLE_NAME AND s.INDEX_NAME = 'PRIMARY' AND s.COLUMN_NAME = c.COLUMN_NAME WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ? ORDER BY c.ORDINAL_POSITION",
            (database, table.name.as_str()),
        ),
    )
    .await?;
    if column_rows.is_empty() {
        return Err(replication_safety_violation(anyhow::anyhow!(
            "MySQL authoritative column metadata returned no rows for '{}.{}'",
            database,
            table.name
        )));
    }
    let columns = column_rows
        .iter()
        .map(authoritative_column_identity)
        .collect::<anyhow::Result<Vec<_>>>()?;
    if !columns
        .iter()
        .any(|column| column.primary_key_ordinal.is_some())
    {
        return Err(replication_safety_violation(anyhow::anyhow!(
            "MySQL replication table '{}.{}' must have a primary key",
            database,
            table.name
        )));
    }
    Ok(AuthoritativeTableIdentity {
        database: database.to_owned(),
        table: table.name.clone(),
        engine,
        columns,
    })
}

#[allow(
    clippy::too_many_arguments,
    reason = "every bootstrap timeout and identity input is explicit at the safety boundary"
)]
pub async fn begin_locked_snapshot(
    config: &MySqlConnectionConfig,
    tables: &[TableConfig],
    server_id: u32,
    preflight: &MySqlReplicationPreflight,
    max_row_bytes: usize,
    lock_timeout: Duration,
    request_timeout: Duration,
    cleanup_timeout: Duration,
    cancellation: &CancellationToken,
) -> anyhow::Result<LockedSnapshotBootstrap> {
    validate_bootstrap_inputs(
        config,
        tables,
        server_id,
        preflight,
        max_row_bytes,
        lock_timeout,
        request_timeout,
        cleanup_timeout,
    )
    .map_err(replication_safety_violation)?;
    let mut execution_lock = acquire_execution_lock(
        config,
        server_id,
        preflight,
        lock_timeout,
        request_timeout,
        cancellation,
    )
    .await?;
    let lock_name = execution_lock.lock_name.clone();
    let owner = execution_lock.connection.take().ok_or_else(|| {
        replication_safety_violation(anyhow::anyhow!(
            "MySQL replication execution lock lost its owner connection"
        ))
    })?;
    let mut resources = BootstrapResources {
        owner: Some(owner),
        sessions: Vec::with_capacity(tables.len()),
        lock_name,
        lock_acquired: true,
        tables_locked: false,
    };

    let setup = async {
        {
            let owner = resources
                .owner
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("MySQL snapshot lock connection is missing"))?;
            run_request(
                cancellation,
                request_timeout,
                "flush_tables_with_read_lock",
                owner.query_drop("FLUSH TABLES WITH READ LOCK"),
            )
            .await?;
        }
        resources.tables_locked = true;

        let mut authoritative_tables = Vec::with_capacity(tables.len());
        for table in tables {
            let mut connection = run_request(
                cancellation,
                request_timeout,
                "connect_snapshot_session",
                connect_with_max_allowed_packet(config, max_row_bytes),
            )
            .await?;
            run_request(
                cancellation,
                request_timeout,
                "set_snapshot_timezone",
                connection.query_drop("SET SESSION time_zone = '+00:00'"),
            )
            .await?;
            run_request(
                cancellation,
                request_timeout,
                "set_snapshot_sql_mode",
                connection.query_drop(MYSQL_CANONICAL_SNAPSHOT_SQL_MODE),
            )
            .await?;
            let forbidden_sql_mode = run_request(
                cancellation,
                request_timeout,
                "verify_snapshot_sql_mode",
                connection.query_first::<u64, _>(
                    "SELECT FIND_IN_SET('PAD_CHAR_TO_FULL_LENGTH', @@SESSION.sql_mode)",
                ),
            )
            .await?;
            if forbidden_sql_mode != Some(0) {
                return Err(replication_safety_violation(anyhow::anyhow!(
                    "MySQL snapshot session retained PAD_CHAR_TO_FULL_LENGTH after canonical setup"
                )));
            }
            run_request(
                cancellation,
                request_timeout,
                "set_snapshot_isolation",
                connection.query_drop("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"),
            )
            .await?;
            run_request(
                cancellation,
                request_timeout,
                "start_consistent_snapshot",
                connection.query_drop("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY"),
            )
            .await?;
            let session_identity = run_request(
                cancellation,
                request_timeout,
                "read_snapshot_session_identity",
                connection.query_first::<Row, _>("SELECT CONNECTION_ID()"),
            )
            .await?
            .ok_or_else(|| {
                replication_safety_violation(anyhow::anyhow!(
                    "MySQL snapshot session identity returned no row"
                ))
            })?;
            let session_connection_id =
                required::<u64, _>(&session_identity, 0, "CONNECTION_ID()")?;
            let qualified = format!(
                "{}.{}",
                quote_identifier(&config.database),
                quote_identifier(&table.name)
            );
            run_request(
                cancellation,
                request_timeout,
                "hold_snapshot_metadata_lock",
                connection.query_drop(format!("SELECT * FROM {qualified} LIMIT 0")),
            )
            .await?;
            authoritative_tables.push(
                read_authoritative_table_identity(
                    &mut connection,
                    &config.database,
                    table,
                    request_timeout,
                    cancellation,
                )
                .await?,
            );
            resources.sessions.push(MySqlSnapshotSession {
                table: table.clone(),
                session_connection_id,
                max_row_bytes,
                connection,
            });
        }

        let boundary = {
            let owner = resources
                .owner
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("MySQL snapshot lock connection is missing"))?;
            read_boundary(owner, preflight, request_timeout, cancellation).await?
        };
        Ok::<_, anyhow::Error>((boundary, authoritative_tables))
    }
    .await;

    match setup {
        Ok((boundary, authoritative_tables)) => Ok(LockedSnapshotBootstrap {
            resources,
            source: preflight.source.clone(),
            boundary,
            authoritative_tables,
        }),
        Err(error) => {
            Err(cleanup_after_failure(&mut resources, cleanup_timeout, error).await)
        }
    }
}

pub fn replication_lock_name(server_uuid: &str, server_id: u32) -> anyhow::Result<String> {
    validate_server_uuid(server_uuid).map_err(replication_safety_violation)?;
    if server_id == 0 {
        return Err(replication_safety_violation(anyhow::anyhow!(
            "MySQL replication server_id must be non-zero"
        )));
    }
    let name = format!("transferia:mysql:{server_uuid}:{server_id}");
    if name.len() > MYSQL_LOCK_NAME_MAX_BYTES {
        return Err(replication_safety_violation(anyhow::anyhow!(
            "exact MySQL replication lock name exceeds {MYSQL_LOCK_NAME_MAX_BYTES} bytes"
        )));
    }
    Ok(name)
}

struct BootstrapResources {
    owner: Option<Conn>,
    sessions: Vec<MySqlSnapshotSession>,
    lock_name: String,
    lock_acquired: bool,
    tables_locked: bool,
}

impl BootstrapResources {
    async fn cleanup(&mut self, cleanup_timeout: Duration) -> anyhow::Result<()> {
        let mut owner = self.owner.take();
        let mut sessions = std::mem::take(&mut self.sessions);
        let tables_locked = self.tables_locked;
        let lock_acquired = self.lock_acquired;
        let lock_name = self.lock_name.clone();
        let cleanup_started = Instant::now();
        let mut first_error = None;
        if let Some(connection) = owner.as_mut() {
            if tables_locked {
                let unlock = match cleanup_remaining(cleanup_started, cleanup_timeout) {
                    Some(remaining) => run_bounded_request(
                        remaining,
                        "abort_unlock_snapshot_tables",
                        connection.query_drop("UNLOCK TABLES"),
                    )
                    .await,
                    None => Err(cleanup_deadline_exceeded("snapshot cleanup")),
                };
                if let Err(error) = unlock {
                    first_error = Some(error);
                }
            }
        }
        for mut session in sessions.drain(..) {
            let rollback = match cleanup_remaining(cleanup_started, cleanup_timeout) {
                Some(remaining) => run_bounded_request(
                    remaining,
                    "abort_snapshot_transaction",
                    session.connection.query_drop("ROLLBACK"),
                )
                .await,
                None => Err(cleanup_deadline_exceeded("snapshot cleanup")),
            };
            if let Err(error) = rollback {
                first_error.get_or_insert(error);
            }
            let disconnect = match cleanup_remaining(cleanup_started, cleanup_timeout) {
                Some(remaining) => run_bounded_request(
                    remaining,
                    "disconnect_snapshot_session",
                    session.connection.disconnect(),
                )
                .await,
                None => Err(cleanup_deadline_exceeded("snapshot cleanup")),
            };
            if let Err(error) = disconnect {
                first_error.get_or_insert(error);
            }
        }
        if let Some(mut connection) = owner {
            if lock_acquired {
                let release = match cleanup_remaining(cleanup_started, cleanup_timeout) {
                    Some(remaining) => run_bounded_request(
                        remaining,
                        "abort_release_replication_lock",
                        connection
                            .exec_first::<Option<u8>, _, _>("SELECT RELEASE_LOCK(?)", (&lock_name,)),
                    )
                    .await
                    .and_then(|released| {
                        if released.flatten() == Some(1) {
                            Ok(())
                        } else {
                            Err(replication_safety_violation(anyhow::anyhow!(
                                "MySQL replication execution lock was not owned during snapshot cleanup"
                            )))
                        }
                    }),
                    None => Err(cleanup_deadline_exceeded("snapshot cleanup")),
                };
                if let Err(error) = release {
                    first_error.get_or_insert(error);
                }
            }
            let disconnect = match cleanup_remaining(cleanup_started, cleanup_timeout) {
                Some(remaining) => run_bounded_request(
                    remaining,
                    "disconnect_snapshot_lock",
                    connection.disconnect(),
                )
                .await,
                None => Err(cleanup_deadline_exceeded("snapshot cleanup")),
            };
            if let Err(error) = disconnect {
                first_error.get_or_insert(error);
            }
        }
        self.tables_locked = false;
        self.lock_acquired = false;
        first_error.map_or(Ok(()), Err)
    }
}

async fn cleanup_after_failure(
    resources: &mut BootstrapResources,
    cleanup_timeout: Duration,
    primary: anyhow::Error,
) -> anyhow::Error {
    match resources.cleanup(cleanup_timeout).await {
        Ok(()) => primary,
        Err(cleanup) => replication_safety_violation(anyhow::anyhow!(
            "MySQL snapshot operation failed and bounded cleanup also failed: {cleanup}; original failure: {primary}"
        )),
    }
}

async fn disconnect_after_failure(
    connection: Conn,
    cleanup_timeout: Duration,
    primary: anyhow::Error,
) -> anyhow::Error {
    match run_bounded_request(
        cleanup_timeout,
        "disconnect_failed_replication_lock",
        connection.disconnect(),
    )
    .await
    {
        Ok(()) => primary,
        Err(cleanup) => replication_safety_violation(anyhow::anyhow!(
            "MySQL replication lock operation failed and connection cleanup also failed: {cleanup}; original failure: {primary}"
        )),
    }
}

async fn release_after_failure(
    lock: MySqlExecutionLock,
    cleanup_timeout: Duration,
    primary: anyhow::Error,
) -> anyhow::Error {
    match lock.release(cleanup_timeout).await {
        Ok(()) => primary,
        Err(cleanup) => replication_safety_violation(anyhow::anyhow!(
            "MySQL replication lock operation failed and lock cleanup also failed: {cleanup}; original failure: {primary}"
        )),
    }
}

async fn read_preflight(
    connection: &mut Conn,
    request_timeout: Duration,
    cancellation: &CancellationToken,
) -> anyhow::Result<MySqlReplicationPreflight> {
    let row = run_request(
        cancellation,
        request_timeout,
        "read_replication_preflight",
        connection.query_first::<Row, _>(
            "SELECT VERSION(), @@GLOBAL.gtid_mode, @@GLOBAL.enforce_gtid_consistency, @@GLOBAL.log_bin, @@GLOBAL.binlog_format, @@GLOBAL.binlog_row_image, @@GLOBAL.binlog_row_metadata, @@GLOBAL.binlog_row_value_options, @@GLOBAL.binlog_transaction_compression, @@GLOBAL.binlog_checksum, @@server_uuid, DATABASE()",
        ),
    )
    .await?
    .ok_or_else(|| {
        replication_safety_violation(anyhow::anyhow!(
            "MySQL replication preflight returned no row"
        ))
    })?;
    let server_version = required::<String, _>(&row, 0, "VERSION()")?;
    let gtid_mode = required::<String, _>(&row, 1, "gtid_mode")?;
    let enforce_gtid = required::<String, _>(&row, 2, "enforce_gtid_consistency")?;
    let log_bin = required::<String, _>(&row, 3, "log_bin")?;
    let binlog_format = required::<String, _>(&row, 4, "binlog_format")?;
    let row_image = required::<String, _>(&row, 5, "binlog_row_image")?;
    let row_metadata = required::<String, _>(&row, 6, "binlog_row_metadata")?;
    let row_value_options = required::<String, _>(&row, 7, "binlog_row_value_options")?;
    let transaction_compression =
        required::<String, _>(&row, 8, "binlog_transaction_compression")?;
    let binlog_checksum = required::<String, _>(&row, 9, "binlog_checksum")?;
    let server_uuid = required::<String, _>(&row, 10, "server_uuid")?;
    let database = required::<Option<String>, _>(&row, 11, "DATABASE()")?.ok_or_else(|| {
        replication_safety_violation(anyhow::anyhow!(
            "MySQL replication connection has no current database"
        ))
    })?;
    let binary_log_status_query = validate_replication_preflight(
        &server_version,
        &gtid_mode,
        &enforce_gtid,
        &log_bin,
        &binlog_format,
        &row_image,
        &row_metadata,
        &row_value_options,
        &transaction_compression,
        &binlog_checksum,
        &server_uuid,
    )?;
    Ok(MySqlReplicationPreflight {
        source: MySqlSourceIdentity {
            server_uuid,
            database,
        },
        server_version,
        binary_log_status_query,
    })
}

async fn read_boundary(
    connection: &mut Conn,
    preflight: &MySqlReplicationPreflight,
    request_timeout: Duration,
    cancellation: &CancellationToken,
) -> anyhow::Result<MySqlBinlogBoundary> {
    let status = run_request(
        cancellation,
        request_timeout,
        "read_binary_log_status",
        connection.query_first::<Row, _>(preflight.binary_log_status_query),
    )
    .await?
    .ok_or_else(|| {
        replication_safety_violation(anyhow::anyhow!(
            "MySQL binary log status returned no row"
        ))
    })?;
    let filename = required::<String, _>(&status, 0, "binary log File")?;
    let position = required::<u64, _>(&status, 1, "binary log Position")?;
    let identity = run_request(
        cancellation,
        request_timeout,
        "read_snapshot_gtid_boundary",
        connection.query_first::<Row, _>(
            "SELECT @@server_uuid, DATABASE(), @@GLOBAL.gtid_executed, TIMESTAMPDIFF(MICROSECOND, '1970-01-01 00:00:00.000000', UTC_TIMESTAMP(6))",
        ),
    )
    .await?
    .ok_or_else(|| {
        replication_safety_violation(anyhow::anyhow!(
            "MySQL GTID boundary query returned no row"
        ))
    })?;
    let boundary_server_uuid = required::<String, _>(&identity, 0, "boundary server_uuid")?;
    let boundary_database =
        required::<Option<String>, _>(&identity, 1, "boundary DATABASE()")?;
    let gtid_executed = required::<String, _>(&identity, 2, "boundary gtid_executed")?;
    let source_timestamp_micros =
        required::<i64, _>(&identity, 3, "boundary source timestamp")?;
    if boundary_server_uuid != preflight.source.server_uuid
        || boundary_database.as_deref() != Some(preflight.source.database.as_str())
    {
        return Err(replication_safety_violation(anyhow::anyhow!(
            "MySQL source identity changed while capturing the exact snapshot boundary"
        )));
    }
    if filename.is_empty() || filename.contains('\0') || position < 4 {
        return Err(replication_safety_violation(anyhow::anyhow!(
            "MySQL returned an invalid binary log boundary"
        )));
    }
    if source_timestamp_micros < 0 {
        return Err(replication_safety_violation(anyhow::anyhow!(
            "MySQL returned a snapshot timestamp before the Unix epoch"
        )));
    }
    let boundary = MySqlBinlogBoundary {
        filename,
        position,
        gtid_executed,
        source_timestamp_micros,
    };
    super::phase::validate_boundary(&boundary).map_err(replication_safety_violation)?;
    Ok(boundary)
}

fn validate_bootstrap_inputs(
    config: &MySqlConnectionConfig,
    tables: &[TableConfig],
    server_id: u32,
    preflight: &MySqlReplicationPreflight,
    max_row_bytes: usize,
    lock_timeout: Duration,
    request_timeout: Duration,
    cleanup_timeout: Duration,
) -> anyhow::Result<()> {
    config.validate()?;
    validate_mysql_client_packet_limit(max_row_bytes)?;
    validate_timeout("lock_timeout", lock_timeout)?;
    validate_timeout("request_timeout", request_timeout)?;
    validate_timeout("cleanup_timeout", cleanup_timeout)?;
    anyhow::ensure!(server_id != 0, "MySQL replication server_id must be non-zero");
    anyhow::ensure!(
        preflight.source.database == config.database,
        "MySQL replication preflight belongs to database '{}', configuration selects '{}'",
        preflight.source.database,
        config.database
    );
    anyhow::ensure!(
        !tables.is_empty(),
        "MySQL exact snapshot requires at least one table"
    );
    let mut unique = HashSet::with_capacity(tables.len());
    for table in tables {
        validate_identifier("table", &table.name)?;
        anyhow::ensure!(
            unique.insert(table.name.as_str()),
            "MySQL exact snapshot repeats table '{}'",
            table.name
        );
    }
    replication_lock_name(&preflight.source.server_uuid, server_id)?;
    Ok(())
}

fn validate_authoritative_table_selection(
    database: &str,
    tables: &[TableConfig],
    expected: &[AuthoritativeTableIdentity],
) -> anyhow::Result<()> {
    validate_identifier("database", database)?;
    anyhow::ensure!(
        !tables.is_empty(),
        "MySQL stream boundary requires at least one authoritative table"
    );
    anyhow::ensure!(
        tables.len() == expected.len(),
        "MySQL stream boundary received {} configured tables but {} authoritative identities",
        tables.len(),
        expected.len()
    );
    let mut names = HashSet::with_capacity(tables.len());
    for (table, identity) in tables.iter().zip(expected) {
        validate_identifier("table", &table.name)?;
        anyhow::ensure!(
            names.insert(table.name.as_str()),
            "MySQL stream boundary repeats configured table '{}'",
            table.name
        );
        anyhow::ensure!(
            identity.database == database && identity.table == table.name,
            "MySQL authoritative identity does not exactly match configured table '{}.{}'",
            database,
            table.name
        );
    }
    Ok(())
}

fn validate_authoritative_tables_unchanged(
    expected: &[AuthoritativeTableIdentity],
    actual: &[AuthoritativeTableIdentity],
) -> anyhow::Result<()> {
    anyhow::ensure!(
        actual == expected,
        "MySQL authoritative table schema changed after discovery and before the exact stream boundary"
    );
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    reason = "each independently unsafe MySQL server setting is named at validation"
)]
fn validate_replication_preflight(
    server_version: &str,
    gtid_mode: &str,
    enforce_gtid: &str,
    log_bin: &str,
    binlog_format: &str,
    row_image: &str,
    row_metadata: &str,
    row_value_options: &str,
    transaction_compression: &str,
    binlog_checksum: &str,
    server_uuid: &str,
) -> anyhow::Result<&'static str> {
    let (major, minor) =
        mysql_version(server_version).map_err(replication_safety_violation)?;
    if major != 8 {
        return Err(replication_safety_violation(anyhow::anyhow!(
            "MySQL GTID replication currently requires MySQL 8.x; server reports '{server_version}'"
        )));
    }
    validate_replication_prerequisites(&MySqlReplicationPrerequisites {
        log_bin: log_bin.to_owned(),
        gtid_mode: gtid_mode.to_owned(),
        enforce_gtid_consistency: enforce_gtid.to_owned(),
        binlog_format: binlog_format.to_owned(),
        binlog_row_image: row_image.to_owned(),
        binlog_row_metadata: row_metadata.to_owned(),
        binlog_row_value_options: row_value_options.to_owned(),
        binlog_transaction_compression: transaction_compression.to_owned(),
        binlog_checksum: binlog_checksum.to_owned(),
    })
    .map_err(|error| replication_safety_violation(anyhow::Error::msg(error)))?;
    validate_server_uuid(server_uuid).map_err(replication_safety_violation)?;
    Ok(if minor >= 4 {
        "SHOW BINARY LOG STATUS"
    } else {
        "SHOW MASTER STATUS"
    })
}

fn mysql_version(server_version: &str) -> anyhow::Result<(u64, u64)> {
    anyhow::ensure!(
        !server_version.to_ascii_lowercase().contains("mariadb"),
        "MySQL 8 GTID replication does not support MariaDB wire events; MariaDB snapshot-only mode remains supported"
    );
    let release = server_version
        .split_once('-')
        .map_or(server_version, |(release, _)| release);
    let mut parts = release.split('.');
    let major = parts
        .next()
        .ok_or_else(|| anyhow::anyhow!("MySQL server version is empty"))?
        .parse::<u64>()?;
    let minor = parts
        .next()
        .ok_or_else(|| anyhow::anyhow!("MySQL server version has no minor component"))?
        .parse::<u64>()?;
    Ok((major, minor))
}

fn validate_timeout(name: &str, timeout: Duration) -> anyhow::Result<()> {
    anyhow::ensure!(!timeout.is_zero(), "MySQL {name} must be positive");
    Ok(())
}

fn cleanup_remaining(started: Instant, timeout: Duration) -> Option<Duration> {
    timeout
        .checked_sub(started.elapsed())
        .filter(|remaining| !remaining.is_zero())
}

fn cleanup_deadline_exceeded(scope: &str) -> anyhow::Error {
    anyhow::anyhow!(
        "MySQL {scope} exceeded its configured timeout; owned connections were dropped"
    )
}

fn validate_locked_gtid_state(
    lock_name: &str,
    held: Option<u8>,
    executed: &str,
    purged: &str,
) -> anyhow::Result<MySqlGtidState> {
    anyhow::ensure!(
        held == Some(1),
        "MySQL replication execution lock '{lock_name}' is no longer owned by this execution"
    );
    let executed = GtidSet::parse_mysql(executed)
        .map_err(|error| anyhow::anyhow!("MySQL returned an invalid executed GTID set: {error}"))?;
    let purged = GtidSet::parse_mysql(purged)
        .map_err(|error| anyhow::anyhow!("MySQL returned an invalid purged GTID set: {error}"))?;
    Ok(MySqlGtidState { executed, purged })
}

fn required<T, I>(row: &Row, index: I, name: &str) -> anyhow::Result<T>
where
    T: FromValue,
    I: ColumnIndex,
{
    match row.get_opt(index) {
        Some(Ok(value)) => Ok(value),
        Some(Err(_)) => Err(replication_safety_violation(anyhow::anyhow!(
            "MySQL response returned an invalid value for required {name}"
        ))),
        None => Err(replication_safety_violation(anyhow::anyhow!(
            "MySQL response omitted required {name}"
        ))),
    }
}

fn authoritative_column_identity(row: &Row) -> anyhow::Result<AuthoritativeColumnIdentity> {
    let name = required::<String, _>(row, 0, "column name")?;
    validate_identifier("column", &name).map_err(replication_safety_violation)?;
    let data_type = required::<String, _>(row, 1, "column data type")?.to_ascii_lowercase();
    let column_type = required::<String, _>(row, 2, "column type")?;
    if column_type.is_empty() {
        return Err(replication_safety_violation(anyhow::anyhow!(
            "MySQL authoritative metadata returned an empty type for column '{name}'"
        )));
    }
    let nullable = match required::<String, _>(row, 3, "column nullability")?.as_str() {
        "YES" => true,
        "NO" => false,
        value => {
            return Err(replication_safety_violation(anyhow::anyhow!(
                "MySQL authoritative metadata returned invalid nullability '{value}' for column '{name}'"
            )));
        }
    };
    let character_set = required::<Option<String>, _>(row, 4, "column character set")?;
    let collation = required::<Option<String>, _>(row, 5, "column collation")?;
    let collation_id = required::<Option<u64>, _>(row, 6, "column collation id")?
        .map(u16::try_from)
        .transpose()
        .map_err(|_| {
            replication_safety_violation(anyhow::anyhow!(
                "MySQL authoritative metadata returned a collation id outside the binlog protocol range for column '{name}'"
            ))
        })?;
    let collation_padding = required::<Option<String>, _>(row, 7, "column collation padding")?
        .map(|value| match value.as_str() {
            "PAD SPACE" => Ok(super::MySqlCollationPadding::PadSpace),
            "NO PAD" => Ok(super::MySqlCollationPadding::NoPad),
            other => Err(replication_safety_violation(anyhow::anyhow!(
                "MySQL authoritative metadata returned unknown collation padding attribute '{other}' for column '{name}'"
            ))),
        })
        .transpose()?;
    if character_set.is_some() != collation_padding.is_some() {
        return Err(replication_safety_violation(anyhow::anyhow!(
            "MySQL authoritative metadata returned inconsistent collation padding identity for column '{name}'"
        )));
    }
    let extra = required::<String, _>(row, 8, "column extra modifiers")?;
    let generation_expression =
        required::<Option<String>, _>(row, 9, "column generation expression")?;
    let character_maximum_length =
        required::<Option<u64>, _>(row, 10, "character maximum length")?
            .map(usize::try_from)
            .transpose()
            .map_err(|error| replication_safety_violation(error.into()))?;
    let character_octet_length =
        required::<Option<u64>, _>(row, 11, "character octet length")?
            .map(usize::try_from)
            .transpose()
            .map_err(|error| replication_safety_violation(error.into()))?;
    let numeric_precision = required::<Option<u64>, _>(row, 12, "numeric precision")?;
    let numeric_scale = required::<Option<u64>, _>(row, 13, "numeric scale")?;
    let datetime_precision = required::<Option<u64>, _>(row, 14, "datetime precision")?;
    let srs_id = required::<Option<u64>, _>(row, 15, "spatial reference system id")?
        .map(u32::try_from)
        .transpose()
        .map_err(|_| {
            replication_safety_violation(anyhow::anyhow!(
                "MySQL authoritative metadata returned an SRS id outside the u32 range for column '{name}'"
            ))
        })?;
    let primary_key_ordinal =
        required::<Option<u64>, _>(row, 16, "primary-key ordinal")?;
    let primary_key_prefix_length =
        required::<Option<u64>, _>(row, 17, "primary-key prefix length")?;
    let primary_key_direction =
        required::<Option<String>, _>(row, 18, "primary-key direction")?;
    if primary_key_ordinal.is_none()
        && (primary_key_prefix_length.is_some() || primary_key_direction.is_some())
    {
        return Err(replication_safety_violation(anyhow::anyhow!(
            "MySQL authoritative metadata returned partial primary-key identity for column '{name}'"
        )));
    }
    let unsigned = has_column_type_modifier(&column_type, "unsigned");
    let zerofill = has_column_type_modifier(&column_type, "zerofill");
    let auto_increment = has_extra_modifier(&extra, "auto_increment");
    if zerofill && !unsigned {
        return Err(replication_safety_violation(anyhow::anyhow!(
            "MySQL authoritative metadata returned ZEROFILL without UNSIGNED for column '{name}'"
        )));
    }
    let enum_set_values =
        parse_enum_set_values(&data_type, &column_type).map_err(replication_safety_violation)?;
    let visibility = column_visibility(&extra);
    let generation = column_generation(&extra, generation_expression.as_deref())
        .map_err(replication_safety_violation)?;
    validate_structured_column_metadata(
        &name,
        &data_type,
        character_maximum_length,
        character_octet_length,
        numeric_precision,
        numeric_scale,
        datetime_precision,
        character_set.as_deref(),
        srs_id,
        enum_set_values.as_deref(),
    )
    .map_err(replication_safety_violation)?;
    Ok(AuthoritativeColumnIdentity {
        name,
        data_type,
        column_type,
        unsigned,
        zerofill,
        auto_increment,
        nullable,
        character_maximum_length,
        character_octet_length,
        numeric_precision,
        numeric_scale,
        datetime_precision,
        character_set,
        collation,
        collation_id,
        collation_padding,
        enum_set_values,
        srs_id,
        visibility,
        generation,
        extra,
        generation_expression,
        primary_key_ordinal,
        primary_key_prefix_length,
        primary_key_direction,
    })
}

async fn run_request<T, E>(
    cancellation: &CancellationToken,
    timeout: Duration,
    operation: &'static str,
    request: impl Future<Output = Result<T, E>>,
) -> anyhow::Result<T>
where
    E: Into<anyhow::Error>,
{
    observe_external_request("mysql", operation, async move {
        tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                anyhow::bail!("MySQL external request '{operation}' cancelled")
            }
            result = tokio::time::timeout(timeout, request) => {
                match result {
                    Ok(result) => result.map_err(Into::into),
                    Err(_) => anyhow::bail!(
                        "MySQL external request '{operation}' exceeded its configured timeout"
                    ),
                }
            }
        }
    })
    .await
}

async fn run_bounded_request<T, E>(
    timeout: Duration,
    operation: &'static str,
    request: impl Future<Output = Result<T, E>>,
) -> anyhow::Result<T>
where
    E: Into<anyhow::Error>,
{
    observe_external_request("mysql", operation, async move {
        match tokio::time::timeout(timeout, request).await {
            Ok(result) => result.map_err(Into::into),
            Err(_) => anyhow::bail!(
                "MySQL external request '{operation}' exceeded its configured timeout"
            ),
        }
    })
    .await
}

#[cfg(test)]
#[path = "tests/bootstrap.rs"]
mod tests;
