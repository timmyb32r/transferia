use futures_util::future::BoxFuture;
use std::sync::Arc;
use transferia_core::data::schema::SchemaColumn;
use transferia_core::data::system_columns::SystemColumnKind;
use transferia_core::failure::{DataPlaneFailure, DataPlaneResult};
use transferia_core::{DeliveryDiscovery, DiscoveredDataset};
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
    fn pending_datasets(&self, delivery: &transferia_core::sink::Delivery) -> DataPlaneResult<Vec<DiscoveredDataset>> {
        let mut pending = Vec::new();
        for batch in &delivery.outputs {
            let existing = self.context.discovery.datasets.iter().find(|dataset| {
                dataset.name == batch.table && dataset.namespace == batch.namespace
                    && dataset.role == transferia_core::DatasetRole::from_is_dlq(batch.is_dlq)
            });
            if existing.is_some_and(|dataset| dataset.incoming_schema == *batch.schema) { continue; }
            let mut dataset = batch.dataset(self.context.keep_system_columns).map_err(DataPlaneFailure::fatal)?;
            if let Some(existing) = existing { dataset.update_policy = existing.update_policy; }
            if !pending.iter().any(|previous: &DiscoveredDataset| previous.name == dataset.name
                && previous.namespace == dataset.namespace && previous.role == dataset.role
                && previous.incoming_schema == dataset.incoming_schema) {
                pending.push(dataset);
            }
        }
        Ok(pending)
    }

    fn prepare(
        &mut self,
        mut dataset: DiscoveredDataset,
        stage: transferia_pipeline::DatasetStage,
    ) -> BoxFuture<'_, DataPlaneResult<Arc<DeliveryDiscovery>>> {
        Box::pin(async move {
            if stage == transferia_pipeline::DatasetStage::Source {
                retain_system_columns(&mut dataset, self.context.keep_system_columns);
            }
            let mut added = self.context.discovery.as_ref().clone();
            added.datasets = vec![dataset];
            let added = if stage == transferia_pipeline::DatasetStage::Source {
                crate::delivery::preparation::validate_middlewares(&self.middlewares, added).await.map_err(DataPlaneFailure::fatal)?
            } else { added };
            let mut combined = self.context.discovery.as_ref().clone();
            let previous = added.datasets.first().and_then(|new| self.context.discovery.datasets.iter().find(|existing| {
                existing.namespace == new.namespace && existing.name == new.name && existing.role == new.role
            })).cloned();
            if added.datasets.iter().all(|new| self.context.discovery.datasets.iter().any(|existing| existing == new)) {
                return Ok(Arc::clone(&self.context.discovery));
            }
            if stage == transferia_pipeline::DatasetStage::Source && !self.middlewares.is_empty() {
                combined.datasets.extend(added.datasets.iter().cloned());
                crate::delivery::preparation::merge_compatible_datasets(&mut combined.datasets).map_err(DataPlaneFailure::fatal)?;
                if combined.datasets == self.context.discovery.datasets {
                    return Ok(Arc::clone(&self.context.discovery));
                }
            } else {
                for dataset in &added.datasets {
                    combined.datasets.retain(|existing| !(existing.namespace == dataset.namespace && existing.name == dataset.name && existing.role == dataset.role));
                    combined.datasets.push(dataset.clone());
                }
            }
            crate::delivery::preparation::validate_discovered_pipeline(
                &self.source,
                &self.sink.compatibility(),
                self.sink.limits(),
                &combined,
                self.context.keep_system_columns,
            )
            .map_err(DataPlaneFailure::fatal)?;
            let preparation = SinkPrepare::from_discovery(
                &added,
                self.context.finite_source,
                self.context.durable.delivery_id.clone(),
                self.context.replay_identity.clone(),
            )
            .map_err(DataPlaneFailure::fatal)?;
            if let Some(preparation) = preparation {
                // Never re-prepare existing tables: some explicit destination
                // policies replace data during preparation.
                self.sink
                    .prepare_runtime(preparation, previous)
                    .await
                    .map_err(DataPlaneFailure::retryable_or_passthrough)?;
            }
            let mut context = self.context.clone();
            context.discovery = Arc::new(combined);
            let discovery = Arc::clone(&context.discovery);
            self.context = context;
            Ok(discovery)
        })
    }
}

pub(super) fn retain_system_columns(dataset: &mut DiscoveredDataset, keep: bool) {
    if !keep {
        return;
    }
    for column in &dataset.system_columns {
        let kind = column.kind;
        if !matches!(
            kind,
            SystemColumnKind::ChangeOperation | SystemColumnKind::ChangedColumns
        ) {
            dataset.stored_schema.columns.push(SchemaColumn::new(
                column.name.to_string(),
                kind.data_type(),
                false,
            ));
        }
    }
}
