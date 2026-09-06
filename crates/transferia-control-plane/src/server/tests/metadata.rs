use super::*;
type BoxFuture<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;
use transferia_core::{DatasetSchema, DiscoveredDataset, SchemaColumn, SchemaOrigin, SourceTopology};
use transferia_core::delivery::{DatasetRole, UpdatePolicy};

#[derive(Default)]
struct Reader {
    loads: AtomicUsize,
    assemblies: AtomicUsize,
    batches: std::sync::Mutex<Vec<Vec<TableIdentity>>>,
}

impl SourceMetadataReader for Reader {
    fn includes_table(&self, table: &TableIdentity, hide: bool) -> bool {
        !hide || table.namespace != "pg_catalog"
    }

    fn load_tables(&self, tables: Vec<TableIdentity>, _: CancellationToken)
        -> BoxFuture<'_, anyhow::Result<BTreeMap<TableIdentity, Result<(), String>>>> {
        Box::pin(async move {
            self.loads.fetch_add(tables.len(), Ordering::SeqCst);
            self.batches.lock().unwrap().push(tables.clone());
            tokio::task::yield_now().await;
            Ok(tables.into_iter().map(|table| {
                let result = if table.name == "bad" { Err("unsupported column in bad".into()) } else { Ok(()) };
                (table, result)
            }).collect())
        })
    }

    fn discovery(&self, selected: Vec<TableIdentity>, request: DeliveryDiscoveryRequest,
        _: CancellationToken) -> BoxFuture<'_, anyhow::Result<DeliveryDiscovery>> {
        Box::pin(async move {
            self.assemblies.fetch_add(1, Ordering::SeqCst);
            Ok(DeliveryDiscovery {
                source_name: Arc::from("metadata fixture"),
                source_topology: SourceTopology::StaticPartitions((0..selected.len() as i64).collect()),
                schema_origin: SchemaOrigin::SourceNative,
                keep_system_columns: request.keep_system_columns,
                datasets: selected.into_iter().map(|table| {
                    let schema = DatasetSchema::new(vec![SchemaColumn::new("id".into(), arrow::datatypes::DataType::Int64, false)]);
                    DiscoveredDataset { namespace: Some(Arc::from(table.namespace)), name: Arc::from(table.name),
                        role: DatasetRole::Main, update_policy: UpdatePolicy::Strict,
                        incoming_schema: schema.clone(), stored_schema: schema, system_columns: vec![] }
                }).collect(),
                performance_advice: vec![],
            })
        })
    }
}

fn table(name: &str) -> TableIdentity { TableIdentity { namespace: "public".into(), name: name.into() } }

fn source() -> Value {
    serde_json::json!({"host":"127.0.0.1", "port":5432, "database":"db", "username":"reader",
        "password":"", "trusted_plaintext":true, "tables":{"type":"all"}, "hide_system_tables":false})
}

fn session(id: &str, catalog: Vec<TableIdentity>, reader: Arc<Reader>) -> Arc<MetadataSession> {
    Arc::new(MetadataSession {
        id: id.into(), connector: "postgres".into(), identity: metadata_identity(&source()),
        resolved_identity: metadata_identity(&source()), delivery_type: DeliveryType::Batch, reader,
        entries: catalog.iter().cloned().map(|table| (table, SchemaEntry::new())).collect(),
        load_gate: Mutex::new(()),
        catalog, active_loads: AtomicUsize::new(0), cancellation: CancellationToken::new(),
        validation: Mutex::new(None), validation_gate: Arc::new(Mutex::new(())),
    })
}

fn context() -> SourceDiscoveryContext {
    SourceDiscoveryContext { request: DeliveryDiscoveryRequest { keep_system_columns: true },
        cancellation: CancellationToken::new(), delivery_type: DeliveryType::Batch }
}

fn config() -> Value {
    serde_json::json!({"delivery_type":"batch", "source":{"postgres":source()}, "sink":{"discard":{}}})
}

#[tokio::test]
async fn schemas_and_errors_are_loaded_once_across_concurrent_and_repeated_requests() {
    let reader = Arc::new(Reader::default());
    let session = session("cache", vec![table("good"), table("bad")], reader.clone());
    for name in ["good", "bad"] {
        let table = table(name);
        let (first, concurrent) = tokio::join!(session.ensure_tables(std::slice::from_ref(&table)), session.ensure_tables(std::slice::from_ref(&table)));
        assert_eq!(first.is_ok(), name == "good");
        assert_eq!(concurrent.is_ok(), name == "good");
        assert_eq!(session.ensure_tables(&[table]).await.is_ok(), name == "good");
    }
    assert_eq!(reader.loads.load(Ordering::SeqCst), 2);
    let status = session.status().await;
    assert_eq!(status.loaded, vec![table("good")]);
    assert_eq!(status.errors[0].table, table("bad"));
}

#[tokio::test]
async fn metadata_fetches_hundreds_of_schemas_in_batches_and_keeps_individual_errors() -> anyhow::Result<()> {
    let reader = Arc::new(Reader::default());
    let mut catalog = (0..204).map(|index| table(&format!("table{index:03}"))).collect::<Vec<_>>();
    catalog.push(table("bad"));
    let session = session("cache", catalog.clone(), reader.clone());
    let (background, foreground) = tokio::join!(session.ensure_tables(&catalog), session.ensure_tables(&catalog));
    assert!(background.is_err());
    assert!(foreground.is_err());
    assert_eq!(reader.batches.lock().unwrap().iter().map(Vec::len).collect::<Vec<_>>(), vec![100, 100, 5]);
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
        let mut catalog = (1..count).map(|index| table(&format!("table{index}"))).collect::<Vec<_>>();
        catalog.insert(0, table("bad"));
        let session = session("cache", catalog, reader.clone());
        let tasks = tokio_util::task::TaskTracker::new();
        session.prefetch(&tasks);
        assert_eq!(session.status().await.loading, count < 1000);
        tasks.close();
        tasks.wait().await;
        assert_eq!(reader.loads.load(Ordering::SeqCst), if count < 1000 { count } else { 0 });
        if count < 1000 {
            assert_eq!(reader.batches.lock().unwrap().iter().map(Vec::len).collect::<Vec<_>>(),
                [vec![100; 9], vec![99]].concat());
            let status = session.status().await;
            assert_eq!(status.loaded.len(), count - 1);
            assert_eq!(status.errors.len(), 1);
            assert!(!status.loading);
        }
    }
}

#[tokio::test]
async fn membership_filters_reuse_full_catalog_and_progress_counts_only_selected_tables() -> anyhow::Result<()> {
    let reader = Arc::new(Reader::default());
    let system = TableIdentity { namespace: "pg_catalog".into(), name: "pg_class".into() };
    let session = session("cache", vec![table("good"), system.clone()], reader.clone());
    let mut selected = config();
    selected["source"]["postgres"]["hide_system_tables"] = Value::Bool(true);
    session.ensure_tables(&[system]).await?;
    session.begin_validation("delivery", 7, &selected).await?;
    let progress = session.status().await.validation.unwrap();
    assert_eq!((progress.checked, progress.total), (0, 1));
    let provider = CachedDiscovery::new(session.clone(), &selected)?;
    provider.discover("postgres", &serde_yaml::to_value(source())?, context()).await?;
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
    let mut different = source();
    different["username"] = Value::String("another-reader".into());
    assert!(provider.discover("postgres", &serde_yaml::to_value(different)?, context()).await.is_err());
    let mut mode = context();
    mode.delivery_type = DeliveryType::Stream;
    assert!(provider.discover("postgres", &serde_yaml::to_value(source())?, mode).await.is_err());
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
        running.run(&CancellationToken::new(), async {
            waiting.notify_one();
            std::future::pending::<anyhow::Result<()>>().await
        }).await
    });
    started.notified().await;
    session.cancellation.cancel();
    assert!(operation.await?.unwrap_err().to_string().contains("released"));
    assert!(session.ensure_tables(&[table("good")]).await.is_err());
    Ok(())
}

#[tokio::test]
async fn discovery_is_cache_only_and_refresh_is_explicit() -> anyhow::Result<()> {
    let service = super::super::tests::service();
    let reader = Arc::new(Reader::default());
    let session = session("cache", vec![table("good")], reader.clone());
    service.metadata_sessions.lock().await.insert(session.id.clone(), session.clone());
    assert!(service.cached_source_discovery("cache", &config(), CancellationToken::new()).await.is_err());
    assert_eq!(reader.loads.load(Ordering::SeqCst), 0);
    session.ensure_tables(&[table("good")]).await?;
    for _ in 0..2 {
        service.cached_source_discovery("cache", &config(), CancellationToken::new()).await?;
    }
    assert_eq!(reader.loads.load(Ordering::SeqCst), 1);
    service.release_metadata("cache").await?;
    assert!(service.cached_source_discovery("cache", &config(), CancellationToken::new()).await.is_err());
    Ok(())
}

#[tokio::test]
async fn validate_pins_the_requested_cache_and_still_checks_transform_columns() -> anyhow::Result<()> {
    let service = super::super::tests::service();
    let reader = Arc::new(Reader::default());
    let chosen = session("chosen", vec![table("good")], reader.clone());
    let other_reader = Arc::new(Reader::default());
    let other = session("other", vec![table("bad")], other_reader.clone());
    service.metadata_sessions.lock().await.extend([(chosen.id.clone(), chosen.clone()), (other.id.clone(), other)]);
    let directory = std::env::temp_dir().join(format!("transferia-metadata-test-{}", new_run_id()?.0));
    let mut config = config();
    config["durable_storage"] = serde_json::json!({"type":"local_file", "path":directory});
    let mut record = service.create_draft("cached validation".into(), String::new(), config).await?;
    for _ in 0..2 {
        let result = service.validate_saved(&record.id, record.revision, record.record_version,
            Some("chosen"), CancellationToken::new()).await?;
        assert!(result.discovery.is_some(), "{:?}", result.delivery.validation);
        record = result.delivery;
    }
    assert_eq!(reader.loads.load(Ordering::SeqCst), 1);
    assert_eq!(other_reader.loads.load(Ordering::SeqCst), 0);
    record.config["middlewares"] = serde_json::json!([{"filter":{"field":"missing","value":"x"}}]);
    let result = service.validate_preview(&record.config, CancellationToken::new(), Some(chosen)).await;
    assert!(result.is_err());
    assert_eq!(reader.loads.load(Ordering::SeqCst), 1);
    if directory.exists() { std::fs::remove_dir_all(directory)?; }
    Ok(())
}
#[test]
fn metadata_scan_preserves_required_clickhouse_fields_without_changing_delivery_selection() -> anyhow::Result<()> {
    let service = super::super::tests::service();
    let source = serde_json::json!({"hosts":["127.0.0.1"],"username":"reader","password":"",
        "tables":{"type":"selected","rules":[]},"hide_system_tables":true});
    let scanned = metadata_scan_config(&source)?;
    assert_eq!(scanned["tables"], serde_json::json!({"type":"all"}));
    assert_eq!(scanned["hide_system_tables"], false);
    assert_eq!(source["tables"]["rules"], serde_json::json!([]));
    let registry = service.transferia.build_registry(&Arc::new(transferia_connectors::metrics::MetricsRegistry::new()))?;
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
    config["source"]["postgres"]["port"] = listener.local_addr()?.port().into();
    let state = Arc::get_mut(&mut cached).unwrap();
    state.identity = metadata_identity(&config["source"]["postgres"]);
    state.resolved_identity = state.identity.clone();
    let directory = std::env::temp_dir().join(format!("transferia-fresh-start-test-{}", new_run_id()?.0));
    config["durable_storage"] = serde_json::json!({"type":"local_file", "path":directory});
    config["delivery_id"] = "fresh-start".into();
    config["delivery_name"] = "Fresh start".into();
    service.validate_preview(&config, CancellationToken::new(), Some(cached)).await?;
    assert_eq!(reader.loads.load(Ordering::SeqCst), 1);
    let wire = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await?;
        let startup_length = socket.read_u32().await?;
        anyhow::ensure!(startup_length > 4, "expected a PostgreSQL startup packet");
        Ok::<_, anyhow::Error>(()) // Close deliberately: startup discovery must fail, not reuse the editor cache.
    });
    let parsed = Config::from_yaml(&serde_yaml::to_string(&config)?)?;
    let started = tokio::time::timeout(std::time::Duration::from_secs(2),
        build_delivery_plan_with(parsed, CancellationToken::new(), &service.transferia)).await?;
    assert!(started.is_err());
    tokio::time::timeout(std::time::Duration::from_secs(2), wire).await???;
    assert_eq!(reader.loads.load(Ordering::SeqCst), 1);
    if directory.exists() { std::fs::remove_dir_all(directory)?; }
    Ok(())
}
