use super::*;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use transferia_delivery::delivery::preparation::PreviewDiscoveryProvider;
use transferia_delivery_contracts::DeliveryType;
use transferia_registry::{SourceMetadataReader, TableIdentity};
use transferia_registry::table_selection::TableSelection;
use transferia_server_contracts::api::{
    MetadataConnectRequest, MetadataConnection, MetadataSchemasRequest, MetadataStatus,
    MetadataValidationPhase, MetadataValidationProgress, TableMetadataError,
};

// This is an editor preload policy, not a delivery/table-count limit.
const AUTOMATIC_SCHEMA_CATALOG_BOUNDARY: usize = 1000;
// Query capacity only: larger catalogs are split, never rejected or truncated.
const SCHEMA_BATCH_SIZE: usize = 100;

type SchemaEntry = tokio::sync::OnceCell<Result<(), String>>;

pub(super) struct MetadataSession {
    pub(super) id: String,
    connector: String,
    identity: Value,
    resolved_identity: Value,
    delivery_type: DeliveryType,
    reader: Arc<dyn SourceMetadataReader>,
    catalog: Vec<TableIdentity>,
    entries: BTreeMap<TableIdentity, SchemaEntry>,
    load_gate: Mutex<()>,
    active_loads: AtomicUsize,
    cancellation: CancellationToken,
    validation: Mutex<Option<(MetadataValidationProgress, Vec<TableIdentity>)>>,
    pub(super) validation_gate: Arc<Mutex<()>>,
}

struct Loading<'a>(&'a AtomicUsize);
impl<'a> Loading<'a> {
    fn new(count: &'a AtomicUsize) -> Self {
        count.fetch_add(1, Ordering::Relaxed);
        Self(count)
    }
}
impl Drop for Loading<'_> {
    fn drop(&mut self) { self.0.fetch_sub(1, Ordering::Relaxed); }
}

impl MetadataSession {
    pub(super) fn ensure_active(&self) -> anyhow::Result<()> {
        anyhow::ensure!(!self.cancellation.is_cancelled(), "Metadata was released; connect and load metadata again");
        Ok(())
    }

    pub(super) async fn run<T>(&self, cancellation: &CancellationToken,
        operation: impl std::future::Future<Output = anyhow::Result<T>>) -> anyhow::Result<T> {
        tokio::select! {
            biased;
            () = self.cancellation.cancelled() => anyhow::bail!("Metadata was released; connect and load metadata again"),
            () = cancellation.cancelled() => anyhow::bail!("Metadata operation cancelled"),
            result = operation => result,
        }
    }

    fn prefetch(self: &Arc<Self>, tasks: &tokio_util::task::TaskTracker) {
        if self.catalog.len() >= AUTOMATIC_SCHEMA_CATALOG_BOUNDARY { return; }
        self.active_loads.fetch_add(1, Ordering::Relaxed);
        let session = Arc::clone(self);
        tasks.spawn(async move {
            let _loading = Loading(&session.active_loads);
            drop(session.ensure_tables(&session.catalog).await);
        });
    }

    pub(super) async fn sample(&self, source: &transferia_server_contracts::api::TransformPreviewSource,
        table: TableIdentity, limits: transferia_registry::TableSampleLimits, cancellation: CancellationToken)
        -> anyhow::Result<transferia_core::TableData> {
        anyhow::ensure!(!self.cancellation.is_cancelled(), "Metadata was released; connect and load metadata again");
        anyhow::ensure!(source.connector == self.connector && metadata_identity(&source.config) == self.identity,
            "Source changed; refresh metadata before preview");
        anyhow::ensure!(self.selected(&source.config)?.contains(&table), "Sample table is not selected by the source");
        anyhow::ensure!(matches!(self.entries.get(&table).and_then(SchemaEntry::get), Some(Ok(()))),
            "Load this table's schema before running preview");
        self.run(&cancellation, self.reader.sample_table(table, limits, cancellation.child_token())).await
    }
    fn selected(&self, config: &Value) -> anyhow::Result<Vec<TableIdentity>> {
        let selection: TableSelection = serde_json::from_value(config.get("tables").cloned()
            .ok_or_else(|| anyhow::anyhow!("Select source tables first"))?)?;
        let hide = config.get("hide_system_tables").and_then(Value::as_bool).unwrap_or(true);
        let visible = self.catalog.iter().filter(|table| self.reader.includes_table(table, hide))
            .cloned().collect::<Vec<_>>();
        selection.compile()?.resolve(&visible)?.selected_tables()
    }

    async fn ensure_tables(&self, tables: &[TableIdentity]) -> anyhow::Result<()> {
        self.ensure_active()?;
        let tables = tables.iter().cloned().collect::<BTreeSet<_>>().into_iter().collect::<Vec<_>>();
        for table in &tables {
            anyhow::ensure!(self.entries.contains_key(table),
                "Table {} is not in the authenticated catalog; refresh metadata", table.qualified_name());
        }
        let pending = tables.iter().filter(|table| self.entries[*table].get().is_none()).cloned().collect::<Vec<_>>();
        for chunk in pending.chunks(SCHEMA_BATCH_SIZE) {
            // Recheck cache after locking: foreground requests may overlap a
            // prefetch. Release between batches so foreground work can interleave.
            let _gate = self.run(&self.cancellation, async { Ok(self.load_gate.lock().await) }).await?;
            let missing = chunk.iter().filter(|table| self.entries[*table].get().is_none()).cloned().collect::<Vec<_>>();
            if missing.is_empty() { continue; }
            let result = self.reader.load_tables(missing.clone(), self.cancellation.child_token()).await;
            self.ensure_active()?;
            let result = result.and_then(|results| {
                anyhow::ensure!(results.len() == missing.len() && missing.iter().all(|table| results.contains_key(table)),
                    "Source metadata batch returned different table identities");
                Ok(results)
            });
            let results = match result {
                Ok(results) => results,
                Err(error) => {
                    let message = format!("{error:#}");
                    missing.into_iter().map(|table| (table, Err(message.clone()))).collect()
                },
            };
            for (table, result) in results {
                self.entries[&table].set(result).map_err(|_| anyhow::anyhow!("Metadata entry initialized twice"))?;
            }
        }
        self.ensure_active()?;
        for table in &tables {
            if let Some(Err(message)) = self.entries[table].get() {
                anyhow::bail!("{}: {message}", table.qualified_name());
            }
        }
        Ok(())
    }

    async fn status(&self) -> MetadataStatus {
        let mut loaded = Vec::new();
        let mut errors = Vec::new();
        for (table, entry) in &self.entries {
            match entry.get() {
                Some(Ok(())) => loaded.push(table.clone()),
                Some(Err(message)) => errors.push(TableMetadataError { table: table.clone(), message: message.clone() }),
                None => {},
            }
        }
        let validation = self.validation.lock().await.as_ref().map(|(progress, selected)| {
            let mut progress = progress.clone();
            progress.checked = selected.iter().filter(|table| self.entries.get(*table)
                .is_some_and(|entry| entry.get().is_some())).count();
            progress
        });
        MetadataStatus { id: self.id.clone(), catalog_count: self.catalog.len(), loaded, errors,
            loading: self.active_loads.load(Ordering::Relaxed) > 0, validation }
    }

    pub(super) async fn begin_validation(&self, id: &str, revision: u64, config: &Value) -> Result<(), ServiceError> {
        self.ensure_active()?;
        let (kind, source) = configured_source(config).map_err(ServiceError::Internal)?;
        if kind != self.connector || metadata_identity(source) != self.identity {
            return Err(ServiceError::Validation("Source changed; refresh metadata before validation".into()));
        }
        let selected = self.selected(source).map_err(|error| ServiceError::Validation(format!("{error:#}")))?;
        *self.validation.lock().await = Some((MetadataValidationProgress {
            delivery_id: id.to_owned(), revision, checked: 0, total: selected.len(),
            phase: MetadataValidationPhase::Schemas,
        }, selected));
        Ok(())
    }

    pub(super) async fn finish_validation(&self, id: &str, revision: u64, success: bool) {
        if let Some((progress, _)) = self.validation.lock().await.as_mut() {
            if progress.delivery_id == id && progress.revision == revision {
                progress.phase = if success { MetadataValidationPhase::Complete } else { MetadataValidationPhase::Failed };
            }
        }
    }
}

/// Deliberately excludes membership filters, but retains every connection,
/// authentication, decoding and source-mode option. Exact equality is checked;
/// credentials never become logs, URLs, cache IDs or client-visible hashes.
fn metadata_identity(config: &Value) -> Value {
    let mut identity = config.clone();
    if let Some(object) = identity.as_object_mut() {
        object.remove("tables");
        object.remove("hide_system_tables");
    }
    identity
}

fn metadata_scan_config(config: &Value) -> anyhow::Result<Value> {
    let mut config = config.clone();
    let object = config.as_object_mut().ok_or_else(|| anyhow::anyhow!("Invalid source configuration"))?;
    // Metadata operations scan the full authenticated catalog, independently
    // of an unfinished delivery Include or the presentation-only system filter.
    object.insert("tables".into(), serde_json::json!({"type": "all"}));
    object.insert("hide_system_tables".into(), Value::Bool(false));
    Ok(config)
}

fn configured_source(config: &Value) -> anyhow::Result<(&str, &Value)> {
    let source = config.get("source").and_then(Value::as_object)
        .filter(|source| source.len() == 1)
        .ok_or_else(|| anyhow::anyhow!("Choose exactly one source"))?;
    let (kind, source) = source.iter().next().ok_or_else(|| anyhow::anyhow!("Choose a source"))?;
    Ok((kind, source))
}

pub(super) struct CachedDiscovery {
    session: Arc<MetadataSession>,
    selected: Vec<TableIdentity>,
}

impl CachedDiscovery {
    pub(super) fn new(session: Arc<MetadataSession>, config: &Value) -> anyhow::Result<Self> {
        session.ensure_active()?;
        let (kind, source) = configured_source(config)?;
        anyhow::ensure!(session.connector == kind && session.identity == metadata_identity(source),
            "Source configuration changed; connect and load metadata again");
        let selected = session.selected(source)?;
        Ok(Self { session, selected })
    }
}

impl PreviewDiscoveryProvider for CachedDiscovery {
    fn discover<'a>(&'a self, kind: &'a str, resolved: &'a serde_yaml::Value, context: SourceDiscoveryContext)
        -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<DeliveryDiscovery>> + Send + 'a>> {
        Box::pin(async move {
            anyhow::ensure!(kind == self.session.connector && context.delivery_type == self.session.delivery_type,
                "Source mode changed; connect and load metadata again");
            anyhow::ensure!(metadata_identity(&serde_json::to_value(resolved)?) == self.session.resolved_identity,
                "Resolved source endpoint changed; refresh metadata");
            let _loading = Loading::new(&self.session.active_loads);
            self.session.run(&context.cancellation, self.session.ensure_tables(&self.selected)).await?;
            if let Some((progress, _)) = self.session.validation.lock().await.as_mut() {
                progress.phase = MetadataValidationPhase::Pipeline;
            }
            self.session.run(&context.cancellation, self.session.reader.discovery(
                self.selected.clone(), context.request, context.cancellation.child_token())).await
        })
    }
}

impl ControlPlane {
    pub async fn connect_metadata(&self, request: MetadataConnectRequest, cancellation: CancellationToken)
        -> Result<MetadataConnection, ServiceError> {
        if let Some(id) = &request.replace_metadata_id {
            match self.release_metadata(id).await {
                Ok(_) | Err(ServiceError::NotFound(_)) => {},
                Err(error) => return Err(error),
            }
        }
        let identity = metadata_identity(&request.source.config);
        let check_config = metadata_scan_config(&request.source.config)?;
        let (connection, resolved) = self.check_connection_resolved(&request.source.connector,
            EndpointRole::Source, check_config, cancellation.clone()).await?;
        let tables = connection.tables.clone().ok_or_else(|| ServiceError::Validation(
            "Connection did not return an authenticated table catalog".to_owned()))?;
        if !matches!(connection.status, transferia_registry::ConnectionCheckStatus::Verified) {
            return Err(ServiceError::Validation("Authenticate the source to load metadata".to_owned()));
        }
        if resolved.len() != 1 {
            return Err(ServiceError::Validation("Metadata requires one resolved source endpoint".to_owned()));
        }
        let raw = serde_json::to_value(&resolved[0]).map_err(anyhow::Error::from)?;
        let resolved_identity = metadata_identity(&raw);
        let raw = metadata_scan_config(&raw)?;
        let catalog = self.transferia.build_registry(&Arc::new(transferia_connectors::metrics::MetricsRegistry::new()))?;
        let source = catalog.build_source(&request.source.connector, serde_yaml::to_value(raw).map_err(anyhow::Error::from)?)?;
        if !source.compatibility(request.delivery_type).supports_delivery_type(request.delivery_type) {
            return Err(ServiceError::Validation("Source does not support the requested delivery type".into()));
        }
        let reader = source.metadata_reader(request.delivery_type)?.ok_or_else(|| ServiceError::Validation(
            "This source does not support cached table metadata".to_owned()))?;
        let id = new_run_id()?.0;
        let session = Arc::new(MetadataSession {
            id: id.clone(), connector: request.source.connector, identity, resolved_identity,
            delivery_type: request.delivery_type, reader,
            entries: tables.iter().cloned().map(|table| (table, SchemaEntry::new())).collect(),
            load_gate: Mutex::new(()),
            catalog: tables, active_loads: AtomicUsize::new(0), cancellation: self.shutdown.child_token(),
            validation: Mutex::new(None), validation_gate: Arc::new(Mutex::new(())),
        });
        self.metadata_sessions.lock().await.insert(id, Arc::clone(&session));
        session.prefetch(&self.metadata_tasks);
        Ok(MetadataConnection { connection, metadata: session.status().await })
    }

    pub(super) async fn metadata_session(&self, id: &str) -> Result<Arc<MetadataSession>, ServiceError> {
        self.metadata_sessions.lock().await.get(id).cloned().ok_or_else(|| ServiceError::NotFound(
            "Metadata cache is no longer available; connect and load metadata again".to_owned()))
    }

    pub async fn metadata_status(&self, id: &str) -> Result<MetadataStatus, ServiceError> {
        Ok(self.metadata_session(id).await?.status().await)
    }

    pub async fn release_metadata(&self, id: &str) -> Result<MetadataStatus, ServiceError> {
        // Serialize release with the final saved-validation state transition.
        let _mutation = self.mutation.lock().await;
        let session = self.metadata_sessions.lock().await.remove(id).ok_or_else(|| ServiceError::NotFound(
            "Metadata cache is no longer available".to_owned()))?;
        session.cancellation.cancel();
        Ok(session.status().await)
    }

    pub async fn load_metadata_schemas(&self, id: &str, request: MetadataSchemasRequest, cancellation: CancellationToken)
        -> Result<MetadataStatus, ServiceError> {
        let session = self.metadata_session(id).await?;
        session.ensure_active()?;
        if session.connector != request.source.connector || session.identity != metadata_identity(&request.source.config) {
            return Err(ServiceError::Validation("Source changed; connect and load metadata again".to_owned()));
        }
        let selected: BTreeSet<_> = session.selected(&request.source.config)?.into_iter().collect();
        for table in &request.tables {
            if !selected.contains(table) {
                return Err(ServiceError::Validation(format!("Table {} is not selected by the source", table.qualified_name())));
            }
        }
        let _loading = Loading::new(&session.active_loads);
        session.run(&cancellation, session.ensure_tables(&request.tables)).await
            .map_err(|error| ServiceError::Validation(format!("{error:#}")))?;
        drop(_loading);
        Ok(session.status().await)
    }

    pub async fn cached_source_discovery(&self, id: &str, config: &Value, cancellation: CancellationToken)
        -> Result<DiscoveryResult, ServiceError> {
        let session = self.metadata_session(id).await?;
        let provider = CachedDiscovery::new(Arc::clone(&session), config)?;
        let mode: DeliveryType = serde_json::from_value(config.get("delivery_type").cloned().unwrap_or(Value::Null))
            .map_err(anyhow::Error::from)?;
        if mode != session.delivery_type { return Err(ServiceError::Validation("Source mode changed; refresh metadata".to_owned())); }
        // Background schema preview is cache-only, including for large catalogs.
        // Manual loading and explicit Validate are the only cache-miss readers.
        for table in &provider.selected {
            match session.entries.get(table).and_then(SchemaEntry::get) {
                Some(Ok(())) => {},
                Some(Err(message)) => return Err(ServiceError::Validation(format!("{}: {message}", table.qualified_name()))),
                None => return Err(ServiceError::Validation("Load the selected table schemas or run Validate".to_owned())),
            }
        }
        let discovery = session.run(&cancellation, session.reader.discovery(provider.selected,
            DeliveryDiscoveryRequest { keep_system_columns: true }, cancellation.child_token())).await?;
        Ok(source_discovery_result(session.connector.clone(), 1, &discovery))
    }
}

#[cfg(test)]
#[path = "tests/metadata.rs"]
mod tests;
