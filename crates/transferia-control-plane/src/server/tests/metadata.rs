use super::*;
type BoxFuture<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;
use transferia_core::delivery::{DatasetRole, UpdatePolicy};
use transferia_core::{
    DatasetSchema, DiscoveredDataset, SchemaColumn, SchemaOrigin, SourceTopology,
};

#[derive(Default)]
struct Reader {
    different_schema: bool,
    loads: AtomicUsize,
    assemblies: AtomicUsize,
    samples: AtomicUsize,
    batches: std::sync::Mutex<Vec<Vec<TableIdentity>>>,
}

impl SourceMetadataReader for Reader {
    fn sample_table(
        &self,
        table: TableIdentity,
        _: transferia_registry::TableSampleLimits,
        _: CancellationToken,
    ) -> BoxFuture<'_, anyhow::Result<transferia_core::TableData>> {
        Box::pin(async move {
            self.samples.fetch_add(1, Ordering::SeqCst);
            let schema = Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
            ]));
            let batch = arrow::record_batch::RecordBatch::try_new(schema, vec![
                Arc::new(arrow::array::Int64Array::from(vec![1])),
            ])?;
            Ok(transferia_core::TableData::new(Arc::from(table.name), false, batch, Default::default())
                .with_namespace(Arc::from(table.namespace)))
        })
    }

    fn list_tables(
        &self,
        _: CancellationToken,
    ) -> BoxFuture<'_, anyhow::Result<Vec<TableIdentity>>> {
        Box::pin(async { Ok(vec![table("good"), table("bad")]) })
    }

    fn includes_table(&self, table: &TableIdentity, hide: bool) -> bool {
        !hide || table.namespace != "pg_catalog"
    }

    fn load_tables(
        &self,
        tables: Vec<TableIdentity>,
        _: CancellationToken,
    ) -> BoxFuture<'_, anyhow::Result<BTreeMap<TableIdentity, Result<(), String>>>> {
        Box::pin(async move {
            self.loads.fetch_add(tables.len(), Ordering::SeqCst);
            self.batches.lock().unwrap().push(tables.clone());
            tokio::task::yield_now().await;
            Ok(tables
                .into_iter()
                .map(|table| {
                    let result = if table.name == "bad" {
                        Err("unsupported column in bad".into())
                    } else {
                        Ok(())
                    };
                    (table, result)
                })
                .collect())
        })
    }

    fn discovery(
        &self,
        selected: Vec<TableIdentity>,
        request: DeliveryDiscoveryRequest,
        _: CancellationToken,
    ) -> BoxFuture<'_, anyhow::Result<DeliveryDiscovery>> {
        Box::pin(async move {
            self.assemblies.fetch_add(1, Ordering::SeqCst);
            Ok(DeliveryDiscovery {
                source_name: Arc::from("metadata fixture"),
                source_topology: SourceTopology::StaticPartitions(
                    (0..i64::try_from(selected.len())?).collect(),
                ),
                schema_origin: SchemaOrigin::SourceNative,
                keep_system_columns: request.keep_system_columns,
                datasets: selected
                    .into_iter()
                    .map(|table| {
                        let schema = DatasetSchema::new(vec![SchemaColumn::new(
                            if self.different_schema && table.name == "other" { "other_column" } else { "id" }.into(),
                            arrow::datatypes::DataType::Int64,
                            false,
                        )]);
                        DiscoveredDataset {
                            namespace: Some(Arc::from(table.namespace)),
                            name: Arc::from(table.name),
                            role: DatasetRole::Main,
                            update_policy: UpdatePolicy::Strict,
                            incoming_schema: schema.clone(),
                            stored_schema: schema,
                            system_columns: vec![],
                        }
                    })
                    .collect(),
                performance_advice: vec![],
            })
        })
    }
}

fn table(name: &str) -> TableIdentity {
    TableIdentity {
        namespace: "public".into(),
        name: name.into(),
    }
}

fn source() -> Value {
    serde_json::json!({"installation":{"type":"on_premise","host":"127.0.0.1","port":5432,
        "trusted_plaintext":true}, "database":"db", "username":"reader", "password":"",
        "tables":{"type":"all"}, "hide_system_tables":false})
}

fn source_sample_request(name: &str) -> transferia_server_contracts::api::SourcePreviewRequest {
    transferia_server_contracts::api::SourcePreviewRequest {
        metadata_id: Some("cache".into()),
        source: transferia_server_contracts::api::TransformPreviewSource { connector: "postgres".into(), config: source() },
        table: Some(table(name)), row_limit: 20, max_sample_bytes: 16_777_216, timeout_ms: 30_000,
    }
}

#[tokio::test]
async fn source_viewer_reads_raw_rows_and_only_loads_the_requested_schema() -> anyhow::Result<()> {
    let reader = Arc::new(Reader::default());
    let metadata = session("cache", vec![table("good"), table("bad")], reader.clone());
    let service = super::super::tests::service();
    service.metadata_sessions.lock().await.insert("cache".into(), metadata.clone());
    for _ in 0..2 {
        let result = service.preview_source(source_sample_request("good"), CancellationToken::new()).await?;
        assert_eq!(result.frames.len(), 1);
        let frame = &result.frames[0];
        assert_eq!(frame.table.name, "good");
        assert_eq!(frame.table.namespace.as_deref(), Some("public"));
        assert_eq!(frame.columns[0].arrow_type, "Int64");
        assert_eq!(frame.rows[0]["id"].as_deref(), Some("1"));
    }
    assert_eq!(reader.loads.load(Ordering::SeqCst), 1);
    assert_eq!(reader.samples.load(Ordering::SeqCst), 2);
    assert_eq!(reader.assemblies.load(Ordering::SeqCst), 0);
    assert!(metadata.preview_validation.lock().await.is_none());
    assert_eq!(metadata.active_loads.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn source_viewer_rejects_invalid_limits_changed_source_and_unselected_tables_before_io() {
    let reader = Arc::new(Reader::default());
    let service = super::super::tests::service();
    service.metadata_sessions.lock().await.insert("cache".into(), session("cache", vec![table("good")], reader.clone()));
    for case in 0..7 {
        let mut request = source_sample_request("good");
        match case {
            0 => request.row_limit = 0,
            1 => request.max_sample_bytes = 0,
            2 => request.timeout_ms = 0,
            3 => request.source.connector = "mysql".into(),
            4 => request.source.config["installation"]["host"] = "changed".into(),
            5 => request.table = Some(table("missing")),
            _ => request.metadata_id = Some("missing".into()),
        }
        assert!(service.preview_source(request, CancellationToken::new()).await.is_err());
    }
    let cancellation = CancellationToken::new(); cancellation.cancel();
    assert!(service.preview_source(source_sample_request("good"), cancellation).await.is_err());
    assert_eq!(reader.loads.load(Ordering::SeqCst), 0);
    assert_eq!(reader.samples.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn source_viewer_does_not_sample_failed_schemas() {
    let reader = Arc::new(Reader::default());
    let service = super::super::tests::service();
    service.metadata_sessions.lock().await.insert("cache".into(), session("cache", vec![table("bad")], reader.clone()));
    assert!(service.preview_source(source_sample_request("bad"), CancellationToken::new()).await.is_err());
    assert_eq!(reader.samples.load(Ordering::SeqCst), 0);
}

fn resolved_source() -> Value {
    serde_json::json!({"host":"127.0.0.1", "port":5432, "database":"db", "username":"reader",
        "password":"", "trusted_plaintext":true, "tables":{"type":"all"}, "hide_system_tables":false})
}

fn session(id: &str, catalog: Vec<TableIdentity>, reader: Arc<Reader>) -> Arc<MetadataSession> {
    Arc::new(MetadataSession {
        id: id.into(),
        connector: "postgres".into(),
        identity: metadata_identity(&source()),
        resolved_identity: metadata_identity(&resolved_source()),
        delivery_type: DeliveryType::Batch,
        reader,
        entries: catalog
            .iter()
            .cloned()
            .map(|table| (table, SchemaEntry::new()))
            .collect(),
        load_gate: Mutex::new(()),
        catalog,
        active_loads: AtomicUsize::new(0),
        cancellation: CancellationToken::new(),
        validation: Mutex::new(None),
        preview_validation: Mutex::new(None),
        validation_gate: Arc::new(Mutex::new(())),
    })
}

fn context() -> SourceDiscoveryContext {
    SourceDiscoveryContext {
        request: DeliveryDiscoveryRequest {
            keep_system_columns: true,
        },
        cancellation: CancellationToken::new(),
        delivery_type: DeliveryType::Batch,
    }
}

fn rename_preview_request(steps: Value, source_config: Value) -> anyhow::Result<transferia_server_contracts::api::TransformPreviewRequest> {
    let through_step = steps.as_array().unwrap().len() - 1;
    Ok(serde_json::from_value(serde_json::json!({
        "metadata_id":"cache", "middlewares":steps, "through_step":through_step,
        "source":{"connector":"postgres", "config":source_config},
        "table":{"namespace":"public", "name":"events"}, "row_limit":20,
        "max_sample_bytes":16_777_216, "memory_limit_bytes":268_435_456, "timeout_ms":30_000,
    }))?)
}

#[tokio::test]
async fn rename_preview_validates_unsampled_matched_tables_before_any_source_read() -> anyhow::Result<()> {
    let reader = Arc::new(Reader::default());
    let session = session("cache", vec![table("events"), table("other")], reader.clone());
    // No schema is loaded: regex failure must precede even schema readiness checks.
    let request = rename_preview_request(serde_json::json!([
        {"rename_table":{"mode":"regex", "pattern":"^events$", "replacement":"events2", "last_part_only":true}}
    ]), source())?;
    let error = ControlPlane::preview_transforms_with(
        &Transferia::public()?, request, CancellationToken::new(), Some(session),
    ).await.unwrap_err().to_string();
    assert!(error.contains("public.other"), "{error}");
    assert!(error.contains("does not match"), "{error}");
    assert_eq!(reader.samples.load(Ordering::SeqCst), 0);
    assert_eq!(reader.loads.load(Ordering::SeqCst), 0);
    assert_eq!(reader.assemblies.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn rename_preview_honors_source_and_step_scopes_and_projects_preceding_names() -> anyhow::Result<()> {
    for scope in ["source", "include", "exclude", "system", "preceding"] {
        let reader = Arc::new(Reader::default());
        let other = if scope == "system" { TableIdentity { namespace:"pg_catalog".into(), name:"other".into() } } else { table("other") };
        let session = session("cache", vec![table("events"), other], reader.clone());
        session.ensure_tables(&[table("events")]).await?;
        let mut source_config = source();
        let mut steps = serde_json::json!([
            {"rename_table":{"mode":"regex", "pattern":"^public\\.events$", "replacement":"archive.events2"}}
        ]);
        match scope {
            "source" => source_config["tables"] = serde_json::json!({"type":"selected", "rules":[{"include":"public.events"}]}),
            "include" => steps[0]["tables"] = serde_json::json!({"include":"public.events"}),
            "exclude" => steps[0]["tables"] = serde_json::json!({"include":"*", "exclude":"public.other"}),
            "system" => source_config["hide_system_tables"] = Value::Bool(true),
            "preceding" => steps = serde_json::json!([
                {"tables":{"include":"public.events"}, "rename_table":{"mode":"exact", "name":"archive.events"}},
                {"tables":{"include":"archive.*"}, "rename_table":{"mode":"regex", "pattern":"^archive\\.events$", "replacement":"archive.events2"}},
            ]),
            _ => unreachable!(),
        }
        let result = ControlPlane::preview_transforms_with(
            &Transferia::public()?, rename_preview_request(steps, source_config)?, CancellationToken::new(), Some(session),
        ).await?;
        assert_eq!(result.before.table.namespace.as_deref(), Some(if scope == "preceding" { "archive" } else { "public" }));
        assert_eq!(result.after.table.namespace.as_deref(), Some("archive"));
        assert_eq!(result.after.table.name, "events2");
        assert_eq!(reader.samples.load(Ordering::SeqCst), 1);
    }
    Ok(())
}

#[tokio::test]
async fn rename_preview_rejects_invalid_outputs_in_unsampled_tables_and_source_mismatch() -> anyhow::Result<()> {
    for invalid_source in [false, true] {
        let reader = Arc::new(Reader::default());
        let session = session("cache", vec![table("events"), table("other")], reader.clone());
        let mut config = source();
        if invalid_source { config["database"] = Value::String("different".into()); }
        let request = rename_preview_request(serde_json::json!([
            {"rename_table":{"mode":"regex", "pattern":"^(events)?(?:other)?$", "replacement":"$1", "last_part_only":true}}
        ]), config)?;
        let error = ControlPlane::preview_transforms_with(
            &Transferia::public()?, request, CancellationToken::new(), Some(session),
        ).await.unwrap_err().to_string();
        if invalid_source {
            assert!(error.contains("Source changed"), "{error}");
            assert!(!error.contains("other"), "catalog must not leak on source mismatch: {error}");
        } else {
            assert!(error.contains("capture 1 did not participate"), "{error}");
            assert!(error.contains("other"), "{error}");
        }
        assert_eq!(reader.samples.load(Ordering::SeqCst), 0);
    }
    Ok(())
}

fn config() -> Value {
    serde_json::json!({"delivery_type":"batch", "source":{"postgres":source()}, "sink":{"discard":{}}})
}

#[tokio::test]
async fn preview_and_preparation_reject_the_same_unsampled_schema_errors() -> anyhow::Result<()> {
    for action in [
        serde_json::json!({"rename_table":{"mode":"exact", "name":"united"}}),
        serde_json::json!({"rename_table":{"mode":"regex", "pattern":".*", "replacement":"united"}}),
        serde_json::json!({"datafusion":{"sql":"SELECT id FROM input"}}),
    ] {
        let reader = Arc::new(Reader { different_schema: true, ..Reader::default() });
        let session = session("cache", vec![table("events"), table("other")], reader.clone());
        let request = rename_preview_request(serde_json::json!([action.clone()]), source())?;
        let error = ControlPlane::preview_transforms_with(
            &Transferia::public()?, request, CancellationToken::new(), Some(session.clone()),
        ).await.unwrap_err();
        assert_eq!(reader.samples.load(Ordering::SeqCst), 0, "must not sample the valid table before checking other");
        let discovery = session.preview_discovery(&transferia_server_contracts::api::TransformPreviewSource {
            connector: "postgres".into(), config: source(),
        }, &CancellationToken::new()).await?;
        let registry = Transferia::public()?.build_registry(&Arc::new(transferia_connectors::metrics::MetricsRegistry::new()))?;
        let middleware = transferia_delivery::middleware::build_middlewares(&registry, &[serde_json::from_value(action)?])?;
        let validation = transferia_delivery::delivery::preparation::validate_middlewares(&middleware, discovery).await.unwrap_err();
        assert!(error.to_string().contains(&format!("{validation:#}")), "preview: {error}; validate: {validation:#}");
    }
    Ok(())
}

#[tokio::test]
async fn preview_accepts_equal_schema_merge_and_reads_the_requested_physical_table() -> anyhow::Result<()> {
    let reader = Arc::new(Reader::default());
    let session = session("cache", vec![table("events"), table("other")], reader.clone());
    let result = ControlPlane::preview_transforms_with(&Transferia::public()?,
        rename_preview_request(serde_json::json!([{"rename_table":{"mode":"exact", "name":"united"}}]), source())?,
        CancellationToken::new(), Some(session),
    ).await?;
    assert_eq!(result.after.table.name, "united");
    assert_eq!(reader.samples.load(Ordering::SeqCst), 1);
    assert_eq!(reader.loads.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test]
async fn preview_reuses_successful_schema_validation_but_revalidates_changed_configuration() -> anyhow::Result<()> {
    let reader = Arc::new(Reader::default());
    let session = session("cache", vec![table("events"), table("other")], reader.clone());
    for target in ["united", "united", "renamed"] {
        ControlPlane::preview_transforms_with(&Transferia::public()?,
            rename_preview_request(serde_json::json!([{"rename_table":{"mode":"exact", "name":target}}]), source())?,
            CancellationToken::new(), Some(session.clone()),
        ).await?;
    }
    assert_eq!(reader.samples.load(Ordering::SeqCst), 3);
    assert_eq!(reader.loads.load(Ordering::SeqCst), 2);
    assert_eq!(reader.assemblies.load(Ordering::SeqCst), 2, "same prefix is validated once, changed prefix is revalidated");
    Ok(())
}

#[tokio::test]
async fn merge_compares_schemas_after_preceding_sql_not_original_source_schemas() -> anyhow::Result<()> {
    let reader = Arc::new(Reader { different_schema: true, ..Reader::default() });
    let session = session("cache", vec![table("events"), table("other")], reader.clone());
    let result = ControlPlane::preview_transforms_with(&Transferia::public()?,
        rename_preview_request(serde_json::json!([
            {"datafusion":{"sql":"SELECT 1 AS id FROM input"}},
            {"rename_table":{"mode":"exact", "name":"united"}},
        ]), source())?, CancellationToken::new(), Some(session),
    ).await?;
    assert_eq!(result.after.table.name, "united");
    assert_eq!(reader.samples.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn schemas_and_errors_are_loaded_once_across_concurrent_and_repeated_requests() {
    let reader = Arc::new(Reader::default());
    let session = session("cache", vec![table("good"), table("bad")], reader.clone());
    for name in ["good", "bad"] {
        let table = table(name);
        let (first, concurrent) = tokio::join!(
            session.ensure_tables(std::slice::from_ref(&table)),
            session.ensure_tables(std::slice::from_ref(&table))
        );
        assert_eq!(first.is_ok(), name == "good");
        assert_eq!(concurrent.is_ok(), name == "good");
        assert_eq!(
            session.ensure_tables(&[table]).await.is_ok(),
            name == "good"
        );
    }
    assert_eq!(reader.loads.load(Ordering::SeqCst), 2);
    let status = session.status().await;
    assert_eq!(status.loaded, vec![table("good")]);
    assert_eq!(status.errors[0].table, table("bad"));
}

#[tokio::test]
async fn metadata_fetches_hundreds_of_schemas_in_batches_and_keeps_individual_errors(
) -> anyhow::Result<()> {
    let reader = Arc::new(Reader::default());
    let mut catalog = (0..204)
        .map(|index| table(&format!("table{index:03}")))
        .collect::<Vec<_>>();
    catalog.push(table("bad"));
    let session = session("cache", catalog.clone(), reader.clone());
    let (background, foreground) = tokio::join!(
        session.ensure_tables(&catalog),
        session.ensure_tables(&catalog)
    );
    assert!(background.is_err());
    assert!(foreground.is_err());
    assert_eq!(
        reader
            .batches
            .lock()
            .unwrap()
            .iter()
            .map(Vec::len)
            .collect::<Vec<_>>(),
        vec![100, 100, 5]
    );
    assert_eq!(reader.loads.load(Ordering::SeqCst), 205);
    let status = session.status().await;
    assert_eq!(status.loaded.len(), 204);
    assert_eq!(status.errors.len(), 1);
    assert_eq!(status.errors[0].table, table("bad"));
    assert!(session.ensure_tables(&catalog).await.is_err());
    assert_eq!(reader.batches.lock().unwrap().len(), 3);
    Ok(())
}

#[tokio::test]
async fn prefetch_boundary_is_strict_and_individual_errors_do_not_stop_the_catalog() {
    for count in [999, 1000, 1001] {
        let reader = Arc::new(Reader::default());
        let mut catalog = (1..count)
            .map(|index| table(&format!("table{index}")))
            .collect::<Vec<_>>();
        catalog.insert(0, table("bad"));
        let session = session("cache", catalog, reader.clone());
        let tasks = tokio_util::task::TaskTracker::new();
        session.prefetch(&tasks);
        assert_eq!(session.status().await.loading, count < 1000);
        tasks.close();
        tasks.wait().await;
        assert_eq!(
            reader.loads.load(Ordering::SeqCst),
            if count < 1000 { count } else { 0 }
        );
        if count < 1000 {
            assert_eq!(
                reader
                    .batches
                    .lock()
                    .unwrap()
                    .iter()
                    .map(Vec::len)
                    .collect::<Vec<_>>(),
                [vec![100; 9], vec![99]].concat()
            );
            let status = session.status().await;
            assert_eq!(status.loaded.len(), count - 1);
            assert_eq!(status.errors.len(), 1);
            assert!(!status.loading);
        }
    }
}

#[tokio::test]
async fn membership_filters_reuse_full_catalog_and_progress_counts_only_selected_tables(
) -> anyhow::Result<()> {
    let reader = Arc::new(Reader::default());
    let system = TableIdentity {
        namespace: "pg_catalog".into(),
        name: "pg_class".into(),
    };
    let session = session("cache", vec![table("good"), system.clone()], reader.clone());
    let mut selected = config();
    selected["source"]["postgres"]["hide_system_tables"] = Value::Bool(true);
    session.ensure_tables(&[system]).await?;
    session.begin_validation("delivery", 7, &selected).await?;
    let progress = session.status().await.validation.unwrap();
    assert_eq!((progress.checked, progress.total), (0, 1));
    let provider = CachedDiscovery::new(session.clone(), &selected)?;
    provider
        .discover(
            "postgres",
            &serde_yaml::to_value(resolved_source())?,
            context(),
        )
        .await?;
    let progress = session.status().await.validation.unwrap();
    assert_eq!((progress.checked, progress.total), (1, 1));
    assert!(matches!(progress.phase, MetadataValidationPhase::Pipeline));
    assert_eq!(session.selected(&source())?.len(), 2);
    assert_eq!(reader.loads.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test]
async fn cached_discovery_rejects_another_endpoint_or_mode_before_loading() -> anyhow::Result<()> {
    let reader = Arc::new(Reader::default());
    let session = session("cache", vec![table("good")], reader.clone());
    let provider = CachedDiscovery::new(session, &config())?;
    let mut different = resolved_source();
    different["username"] = Value::String("another-reader".into());
    assert!(provider
        .discover("postgres", &serde_yaml::to_value(different)?, context())
        .await
        .is_err());
    let mut mode = context();
    mode.delivery_type = DeliveryType::Stream;
    assert!(provider
        .discover("postgres", &serde_yaml::to_value(resolved_source())?, mode)
        .await
        .is_err());
    assert_eq!(reader.loads.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn release_cancels_the_whole_operation_even_after_schema_cache_hits() -> anyhow::Result<()> {
    let session = session("cache", vec![table("good")], Arc::new(Reader::default()));
    session.ensure_tables(&[table("good")]).await?;
    let running = Arc::clone(&session);
    let started = Arc::new(tokio::sync::Notify::new());
    let waiting = Arc::clone(&started);
    let operation = tokio::spawn(async move {
        running
            .run(&CancellationToken::new(), async {
                waiting.notify_one();
                std::future::pending::<anyhow::Result<()>>().await
            })
            .await
    });
    started.notified().await;
    session.cancellation.cancel();
    assert!(operation
        .await?
        .unwrap_err()
        .to_string()
        .contains("released"));
    assert!(session.ensure_tables(&[table("good")]).await.is_err());
    Ok(())
}

#[tokio::test]
async fn discovery_is_cache_only_and_refresh_is_explicit() -> anyhow::Result<()> {
    let service = super::super::tests::service();
    let reader = Arc::new(Reader::default());
    let session = session("cache", vec![table("good")], reader.clone());
    service
        .metadata_sessions
        .lock()
        .await
        .insert(session.id.clone(), session.clone());
    assert!(service
        .cached_source_discovery("cache", &config(), CancellationToken::new())
        .await
        .is_err());
    assert_eq!(reader.loads.load(Ordering::SeqCst), 0);
    session.ensure_tables(&[table("good")]).await?;
    for _ in 0..2 {
        service
            .cached_source_discovery("cache", &config(), CancellationToken::new())
            .await?;
    }
    assert_eq!(reader.loads.load(Ordering::SeqCst), 1);
    service.release_metadata("cache").await?;
    assert!(service
        .cached_source_discovery("cache", &config(), CancellationToken::new())
        .await
        .is_err());
    Ok(())
}

#[tokio::test]
async fn validate_pins_the_requested_cache_and_still_checks_transform_columns() -> anyhow::Result<()>
{
    let service = super::super::tests::service();
    let reader = Arc::new(Reader::default());
    let chosen = session("chosen", vec![table("good")], reader.clone());
    let other_reader = Arc::new(Reader::default());
    let other = session("other", vec![table("bad")], other_reader.clone());
    service.metadata_sessions.lock().await.extend([
        (chosen.id.clone(), chosen.clone()),
        (other.id.clone(), other),
    ]);
    let directory =
        std::env::temp_dir().join(format!("transferia-metadata-test-{}", new_run_id()?.0));
    let mut config = config();
    config["durable_storage"] = serde_json::json!({"type":"local_file", "path":directory});
    let mut record = service
        .create_draft("cached validation".into(), String::new(), config)
        .await?;
    for _ in 0..2 {
        let result = service
            .validate_saved(
                &record.id,
                record.revision,
                record.record_version,
                Some("chosen"),
                CancellationToken::new(),
            )
            .await?;
        assert!(
            result.discovery.is_some(),
            "{:?}",
            result.delivery.validation
        );
        record = result.delivery;
    }
    assert_eq!(reader.loads.load(Ordering::SeqCst), 1);
    assert_eq!(other_reader.loads.load(Ordering::SeqCst), 0);
    record.config["middlewares"] = serde_json::json!([{"filter":{"field":"missing","value":"x"}}]);
    let result = service
        .validate_preview(&record.config, CancellationToken::new(), Some(chosen))
        .await;
    assert!(result.is_err());
    assert_eq!(reader.loads.load(Ordering::SeqCst), 1);
    if directory.exists() {
        std::fs::remove_dir_all(directory)?;
    }
    Ok(())
}
#[test]
fn metadata_scan_preserves_required_clickhouse_fields_without_changing_delivery_selection(
) -> anyhow::Result<()> {
    let service = super::super::tests::service();
    let source = serde_json::json!({"hosts":["127.0.0.1"],"port":9000,"http_port":8123,
        "trusted_plaintext":true,"username":"reader","password":"",
        "tables":{"type":"selected","rules":[]},"hide_system_tables":true});
    let scanned = metadata_scan_config(&source)?;
    assert_eq!(scanned["tables"], serde_json::json!({"type":"all"}));
    assert_eq!(scanned["hide_system_tables"], false);
    assert_eq!(source["tables"]["rules"], serde_json::json!([]));
    for field in [
        "hosts",
        "port",
        "http_port",
        "trusted_plaintext",
        "username",
        "password",
    ] {
        assert_eq!(
            scanned[field], source[field],
            "metadata scan changed {field}"
        );
    }
    let registry = service.transferia.build_registry(&Arc::new(
        transferia_connectors::metrics::MetricsRegistry::new(),
    ))?;
    registry.build_source("clickhouse", serde_yaml::to_value(scanned)?)?;
    Ok(())
}

#[tokio::test]
async fn startup_after_cached_validation_still_contacts_the_source() -> anyhow::Result<()> {
    use tokio::io::AsyncReadExt as _;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let service = super::super::tests::service();
    let reader = Arc::new(Reader::default());
    let mut cached = session("cache", vec![table("good")], reader.clone());
    let mut config = config();
    config["source"]["postgres"]["installation"]["port"] = listener.local_addr()?.port().into();
    let state = Arc::get_mut(&mut cached).unwrap();
    state.identity = metadata_identity(&config["source"]["postgres"]);
    let mut resolved = resolved_source();
    resolved["port"] = listener.local_addr()?.port().into();
    state.resolved_identity = metadata_identity(&resolved);
    let directory =
        std::env::temp_dir().join(format!("transferia-fresh-start-test-{}", new_run_id()?.0));
    config["durable_storage"] = serde_json::json!({"type":"local_file", "path":directory});
    config["delivery_id"] = "fresh-start".into();
    config["delivery_name"] = "Fresh start".into();
    service
        .validate_preview(&config, CancellationToken::new(), Some(cached))
        .await?;
    assert_eq!(reader.loads.load(Ordering::SeqCst), 1);
    let wire = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await?;
        let startup_length = socket.read_u32().await?;
        anyhow::ensure!(startup_length > 4, "expected a PostgreSQL startup packet");
        Ok::<_, anyhow::Error>(()) // Close deliberately: startup discovery must fail, not reuse the editor cache.
    });
    let parsed = Config::from_yaml(&serde_yaml::to_string(&config)?)?;
    let started = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        build_delivery_plan_with(parsed, CancellationToken::new(), &service.transferia),
    )
    .await?;
    assert!(started.is_err());
    tokio::time::timeout(std::time::Duration::from_secs(2), wire).await???;
    assert_eq!(reader.loads.load(Ordering::SeqCst), 1);
    if directory.exists() {
        std::fs::remove_dir_all(directory)?;
    }
    Ok(())
}
