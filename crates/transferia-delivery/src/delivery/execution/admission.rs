use std::sync::Arc;
use futures_util::future::BoxFuture;
use transferia_core::{DiscoveredDataset, Sink};
use transferia_core::failure::{DataPlaneFailure, DataPlaneResult};
use transferia_delivery_contracts::middleware::Middleware;
use transferia_delivery_contracts::semantics::EndpointDescriptor;
use transferia_registry::{SinkBuildContext, SinkConnector, SinkPrepare};

pub(super) struct AdmissionCoordinator {
    pub sink: Arc<dyn SinkConnector>,
    pub source: EndpointDescriptor,
    pub middlewares: Arc<Vec<Box<dyn Middleware>>>,
    pub context: SinkBuildContext,
}

impl transferia_pipeline::DatasetAdmission for AdmissionCoordinator {
    fn prepare(&mut self, mut dataset: DiscoveredDataset) -> BoxFuture<'_, DataPlaneResult<Box<dyn Sink>>> {
        Box::pin(async move {
            retain_system_columns(&mut dataset, self.context.keep_system_columns);
            let mut added = self.context.discovery.as_ref().clone();
            added.datasets = vec![dataset];
            let added = crate::delivery::preparation::validate_middlewares(&self.middlewares, added)
                .await.map_err(DataPlaneFailure::fatal)?;
            let mut combined = self.context.discovery.as_ref().clone();
            combined.datasets.extend(added.datasets.iter().cloned());
            crate::delivery::preparation::validate_discovered_pipeline(
                &self.source, &self.sink.compatibility(), self.sink.limits(),
                &combined, self.context.keep_system_columns,
            ).map_err(DataPlaneFailure::fatal)?;
            let preparation = SinkPrepare::from_discovery(&added, self.context.finite_source,
                self.context.durable.delivery_id.clone(), self.context.replay_identity.clone())
                .map_err(DataPlaneFailure::fatal)?;
            if let Some(preparation) = preparation {
                // Never re-prepare existing tables: some explicit destination
                // policies replace data during preparation.
                self.sink.prepare(preparation).await.map_err(DataPlaneFailure::retryable_or_passthrough)?;
            }
            let mut context = self.context.clone();
            context.discovery = Arc::new(combined);
            let sink = self.sink.build_sink(context.clone()).await.map_err(DataPlaneFailure::retryable_or_passthrough)?;
            self.context = context;
            Ok(sink)
        })
    }
}

pub(super) fn retain_system_columns(dataset: &mut DiscoveredDataset, keep: bool) {
    if !keep { return; }
    use transferia_core::data::system_columns::SystemColumnKind;
    use transferia_core::data::schema::SchemaColumn;
    for column in &dataset.system_columns {
        let kind = column.kind;
        if !matches!(kind, SystemColumnKind::ChangeOperation | SystemColumnKind::ChangedColumns) {
            dataset.stored_schema.columns.push(SchemaColumn::new(
                column.name.to_string(), kind.data_type(), false));
        }
    }
}
