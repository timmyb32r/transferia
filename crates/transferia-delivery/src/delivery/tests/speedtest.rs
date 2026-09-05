use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::{
    Array, Decimal128Array, FixedSizeBinaryArray, Int64Array, Int8Array, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use futures_util::future::BoxFuture;
use tokio::sync::Notify;
use tokio_util::task::TaskTracker;

use super::*;
use transferia_core::data::schema::{DatasetSchema, SchemaColumn, META_PRIMARY_KEY};
use transferia_core::data::system_columns::SystemColumns;
use transferia_core::delivery::{
    DeliveryDiscovery, DiscoveredDataset, SchemaOrigin, SourceTopology, NO_LIMITS,
};
use transferia_core::sink::{DeliveryId, DeliveryMeta, SinkBatch};
use transferia_delivery_contracts::semantics::EndpointDescriptor;
use transferia_registry::{
    SinkConnector, SourceConnector, SourceDiscoveryContext, SpeedtestPhysicalTarget,
};

struct CommitCountingSource {
    commits: Arc<AtomicUsize>,
}

struct ShutdownFailingSource;

struct UnisolatedSourceConnector;

impl SourceConnector for UnisolatedSourceConnector {
    fn compatibility(
        &self,
        _delivery_type: transferia_delivery_contracts::DeliveryType,
    ) -> EndpointDescriptor {
        EndpointDescriptor::Discard
    }

    fn delivery_discovery(
        &self,
        _context: SourceDiscoveryContext,
    ) -> BoxFuture<'_, anyhow::Result<DeliveryDiscovery>> {
        Box::pin(async { anyhow::bail!("unused discovery") })
    }

    fn build_source(
        &self,
        _context: SourceBuildContext,
    ) -> BoxFuture<'_, anyhow::Result<Box<dyn Source>>> {
        Box::pin(async { anyhow::bail!("unsafe production source must not be built") })
    }

    fn parser(&self) -> Arc<dyn transferia_delivery_contracts::parser::ParserFactory> {
        panic!("unused parser")
    }

    fn parses_rows(&self) -> bool {
        false
    }
}

impl Source for CommitCountingSource {
    fn read_batch(&mut self) -> BoxFuture<'_, DataPlaneResult<SourceBatch>> {
        Box::pin(async { Ok(SourceBatch::Finished) })
    }

    fn commit_offsets<'a>(
        &'a mut self,
        _markers: &'a [CommitMarker],
    ) -> BoxFuture<'a, DataPlaneResult<()>> {
        self.commits.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(()) })
    }
}

impl Source for ShutdownFailingSource {
    fn read_batch(&mut self) -> BoxFuture<'_, DataPlaneResult<SourceBatch>> {
        Box::pin(async { Ok(SourceBatch::Finished) })
    }

    fn commit_offsets<'a>(
        &'a mut self,
        _markers: &'a [CommitMarker],
    ) -> BoxFuture<'a, DataPlaneResult<()>> {
        Box::pin(async { Ok(()) })
    }

    fn shutdown(&mut self) -> BoxFuture<'_, DataPlaneResult<()>> {
        Box::pin(async {
            Err(DataPlaneFailure::fatal(anyhow::anyhow!(
                "injected secret=password shutdown failure"
            )))
        })
    }
}

#[tokio::test]
async fn speedtest_source_never_commits_the_real_source() -> anyhow::Result<()> {
    let commits = Arc::new(AtomicUsize::new(0));
    let (mut source, shutdown_failed) = NoCommitSource::new(Box::new(CommitCountingSource {
        commits: Arc::clone(&commits),
    }));

    source.commit_offsets(&[]).await?;

    assert_eq!(commits.load(Ordering::SeqCst), 0);
    assert!(!shutdown_failed.load(Ordering::Acquire));
    Ok(())
}

#[tokio::test]
async fn speedtest_source_cleanup_failure_is_observable_and_credential_safe() {
    let (mut source, shutdown_failed) = NoCommitSource::new(Box::new(ShutdownFailingSource));

    assert!(source.shutdown().await.is_err());
    let error = ensure_source_cleanup_succeeded(&shutdown_failed)
        .expect_err("failed source cleanup must fail the speedtest operation");

    assert!(error.is::<SpeedtestSourceCleanupFailure>());
    assert!(!error.to_string().contains("secret=password"));
}

#[tokio::test]
async fn source_without_explicit_isolation_fails_closed() {
    let connector = UnisolatedSourceConnector;
    let error = connector
        .build_speedtest_source(SourceBuildContext {
            partition_id: 0,
            delivery_type: transferia_delivery_contracts::DeliveryType::Batch,
            phase: transferia_registry::SourcePhase::Snapshot,
            replay_identity: None,
            cancellation: CancellationToken::new(),
            memory: PipelineMemory::new(1_024),
            durable: ephemeral_durable("test", "source"),
        })
        .await
        .err()
        .expect("the default source hook must never touch production state");

    assert!(error.to_string().contains("non-disruptive isolated"));
}

#[test]
fn unrepresentable_speedtest_window_is_rejected_without_panicking() {
    let error = validate_speedtest_window(Duration::MAX)
        .expect_err("an unrepresentable Tokio deadline must fail before endpoint I/O");

    assert!(error.to_string().contains("too large"));
}

#[test]
fn profile_reports_ranges_lengths_and_fixed_memory_cardinality() -> anyhow::Result<()> {
    let strings = StringArray::from(vec![Some("a"), Some("alphabet"), Some("a"), None]);
    let profile = profile_column_state("value", &strings)?.finish()?;

    assert_eq!(profile.min_length, Some(1));
    assert_eq!(profile.max_length, Some(8));
    assert_eq!(profile.distinct_count, Some(2));
    assert_eq!(profile.min_value, None);
    assert_eq!(profile.max_value, None);
    assert_eq!(profile.null_count, 1);

    let numbers = Int64Array::from(vec![Some(9), Some(-2), None, Some(4)]);
    let profile = profile_column_state("number", &numbers)?.finish()?;
    assert_eq!(profile.min_value.as_deref(), Some("-2"));
    assert_eq!(profile.max_value.as_deref(), Some("9"));
    assert_eq!(profile.range_kind, Some(SpeedtestRangeKind::Numeric));
    assert_eq!(profile.distinct_count, Some(3));
    Ok(())
}

#[test]
fn decimal_range_preserves_precision_and_scale() -> anyhow::Result<()> {
    let decimals = Decimal128Array::from(vec![Some(42_i128), None, Some(-100_i128)])
        .with_precision_and_scale(10, 2)?;

    let profile = profile_column_state("decimal", &decimals)?.finish()?;

    assert_eq!(profile.arrow_type, "Decimal128(10, 2)");
    assert_eq!(profile.min_value.as_deref(), Some("-1.00"));
    assert_eq!(profile.max_value.as_deref(), Some("0.42"));
    assert_eq!(profile.range_kind, Some(SpeedtestRangeKind::Numeric));
    assert_eq!(profile.distinct_count, Some(2));
    Ok(())
}

#[test]
fn aggregate_profile_merges_multibatch_ranges_and_cardinality() -> anyhow::Result<()> {
    let batches = [
        RecordBatch::try_from_iter(vec![(
            "value",
            Arc::new(Int64Array::from(vec![10, 20])) as ArrayRef,
        )])?,
        RecordBatch::try_from_iter(vec![(
            "value",
            Arc::new(Int64Array::from(vec![-5, 20, 100])) as ArrayRef,
        )])?,
    ];
    let samples = batches
        .iter()
        .map(|batch| {
            Ok(SpooledDelivery {
                outputs: Arc::from([SpooledOutput {
                    table: Arc::from("dataset"),
                    is_dlq: false,
                    system_columns: SystemColumns::default(),
                    schema: batch.schema(),
                    arrow_bytes: batch.get_array_memory_size(),
                    file: Arc::new(spool_record_batch(batch)?),
                    profile: profile_batch_state("dataset", false, batch)?,
                }]),
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    let profiles = aggregate_sample_profiles(&samples)?;

    assert_eq!(profiles.len(), 1);
    assert_eq!(profiles[0].rows, 5);
    assert_eq!(profiles[0].columns[0].min_value.as_deref(), Some("-5"));
    assert_eq!(profiles[0].columns[0].max_value.as_deref(), Some("100"));
    assert_eq!(profiles[0].columns[0].distinct_count, Some(4));
    Ok(())
}

#[tokio::test]
async fn loaded_speedtest_sample_restores_source_field_metadata() -> anyhow::Result<()> {
    let field = Field::new("id", DataType::Int64, false)
        .with_metadata([(META_PRIMARY_KEY.to_owned(), "true".to_owned())].into());
    let schema = Arc::new(Schema::new(vec![field]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1_i64, 2]))],
    )?;
    let sample = SpooledDelivery {
        outputs: Arc::from([SpooledOutput {
            table: Arc::from("dataset"),
            is_dlq: false,
            system_columns: SystemColumns::default(),
            schema: Arc::clone(&schema),
            arrow_bytes: batch.get_array_memory_size(),
            file: Arc::new(spool_record_batch(&batch)?),
            profile: profile_batch_state("dataset", false, &batch)?,
        }]),
    };

    let loaded = load_spooled_deliveries(&[sample], &PipelineMemory::new(1024 * 1024)).await?;

    assert_eq!(loaded[0].outputs[0].batch.schema(), schema);
    assert_eq!(
        loaded[0].outputs[0]
            .batch
            .schema()
            .field(0)
            .metadata()
            .get(META_PRIMARY_KEY)
            .map(String::as_str),
        Some("true")
    );
    Ok(())
}

#[tokio::test]
async fn loaded_ipc_sample_does_not_multiply_shared_record_block_memory() -> anyhow::Result<()> {
    let rows = 16_384;
    let columns = (0..16)
        .map(|column| {
            (
                format!("value_{column}"),
                Arc::new(StringArray::from_iter_values(
                    (0..rows).map(|row| format!("{column}-{row:08}")),
                )) as ArrayRef,
            )
        })
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_from_iter(columns)?;
    let original_bytes = batch.get_array_memory_size();
    let file = Arc::new(spool_record_batch(&batch)?);
    let uncompacted_bytes = read_spooled_batch(&file)?.get_array_memory_size();
    assert!(
        uncompacted_bytes > original_bytes.saturating_mul(2),
        "fixture must expose Arrow IPC's shared record-block retention"
    );
    let sample = SpooledDelivery {
        outputs: Arc::from([SpooledOutput {
            table: Arc::from("logical"),
            is_dlq: false,
            system_columns: SystemColumns::default(),
            schema: batch.schema(),
            arrow_bytes: original_bytes,
            file,
            profile: profile_batch_state("logical", false, &batch)?,
        }]),
    };
    let memory = PipelineMemory::new(original_bytes.saturating_mul(2));

    let loaded = load_spooled_deliveries(&[sample], &memory).await?;
    let loaded_bytes = loaded[0].outputs[0].batch.get_array_memory_size();

    assert!(
        loaded_bytes <= original_bytes.saturating_mul(2),
        "IPC replay retained {loaded_bytes} bytes for {original_bytes} bytes of source arrays"
    );
    assert_eq!(memory.used(), loaded_bytes.max(1));
    Ok(())
}

#[test]
fn unsupported_range_type_omits_bounds_without_rejecting_profile() -> anyhow::Result<()> {
    let binary = arrow::array::BinaryArray::from(vec![Some(b"b".as_slice()), Some(b"a")]);

    let profile = profile_column_state("binary", &binary)?.finish()?;

    assert_eq!(profile.min_value, None);
    assert_eq!(profile.max_value, None);
    assert_eq!(profile.range_kind, None);
    Ok(())
}

#[test]
fn generated_string_primary_keys_are_unique_across_replays() -> anyhow::Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(vec!["first", "second"]))],
    )?;
    let key = UniqueKey {
        column: 0,
        kind: UniqueKeyKind::Utf8 {
            suffix_width: 2,
            max_iteration: 1_295,
        },
        namespace: 7,
        sample_rows: 2,
        forbidden_iterations: BTreeSet::new(),
    };

    let (first, first_extra_bytes) = rewrite_unique_key(&batch, &key, 0, 0)?;
    let (second, second_extra_bytes) = rewrite_unique_key(&batch, &key, 1, 0)?;
    assert_eq!(
        first, batch,
        "the first replay must preserve the sample exactly"
    );
    assert_eq!(first_extra_bytes, 0);
    assert!(second_extra_bytes > 0);
    let first = first
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let second = second
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    assert_eq!(first.value(0), "first");
    assert_eq!(first.value(1), "second");
    assert_ne!(first.value(0), second.value(0));
    assert_ne!(first.value(1), second.value(1));
    Ok(())
}

#[test]
fn empirical_replay_preserves_non_key_distribution_exactly() -> anyhow::Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["first", "second", "third"])),
            Arc::new(Int64Array::from(vec![Some(-7), None, Some(42)])),
        ],
    )?;
    let key = UniqueKey {
        column: 0,
        kind: UniqueKeyKind::Utf8 {
            suffix_width: 2,
            max_iteration: 1_295,
        },
        namespace: 7,
        sample_rows: 3,
        forbidden_iterations: BTreeSet::new(),
    };

    let (generated, _) = rewrite_unique_key(&batch, &key, 1, 0)?;

    assert_eq!(generated.column(1), batch.column(1));
    assert_eq!(
        profile_column_state("value", generated.column(1).as_ref())?.finish()?,
        profile_column_state("value", batch.column(1).as_ref())?.finish()?
    );
    Ok(())
}

#[test]
fn bounded_string_primary_key_without_suffix_room_fails_preflight() -> anyhow::Result<()> {
    let batch = RecordBatch::try_from_iter(vec![(
        "id",
        Arc::new(StringArray::from(vec!["x"])) as ArrayRef,
    )])?;
    let column = SchemaColumn::new("id".to_owned(), DataType::Utf8, false).with_constraints(
        true,
        false,
        Some(1),
    );

    let error = build_unique_key_kind_many(&[&batch], 0, &column)
        .expect_err("replay must not exceed a declared string width");

    assert!(error.to_string().contains("no room"));
    Ok(())
}

#[test]
fn narrow_integer_primary_key_fails_closed_at_exhaustion() -> anyhow::Result<()> {
    let batch = RecordBatch::try_from_iter(vec![(
        "id",
        Arc::new(Int8Array::from(vec![126_i8, 127])) as ArrayRef,
    )])?;
    let column = SchemaColumn::new("id".to_owned(), DataType::Int8, false)
        .with_constraints(true, false, None);
    let kind = build_unique_key_kind_many(&[&batch], 0, &column)?;
    let key = UniqueKey {
        column: 0,
        kind,
        namespace: 0,
        sample_rows: 2,
        forbidden_iterations: BTreeSet::new(),
    };

    rewrite_unique_key(&batch, &key, 127, 0)?;
    let error = rewrite_unique_key(&batch, &key, 128, 0)
        .expect_err("finite key space must not wrap or duplicate values");

    assert!(error.to_string().contains("space exhausted"));
    Ok(())
}

#[test]
fn text_primary_key_replay_skips_values_already_present_in_sample() -> anyhow::Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(vec!["a", "a0001"]))],
    )?;
    let kind = UniqueKeyKind::Utf8 {
        suffix_width: 4,
        max_iteration: 36_u128.pow(4) - 1,
    };
    let forbidden_iterations = forbidden_replay_iterations_many(&[&batch], 0, kind)?;
    assert_eq!(forbidden_iterations, BTreeSet::from([1]));
    let key = UniqueKey {
        column: 0,
        kind,
        namespace: 0,
        sample_rows: 2,
        forbidden_iterations,
    };

    let (generated, _) = rewrite_unique_key(&batch, &key, 1, 0)?;
    let generated = generated
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    assert_eq!(generated.value(0), "a0002");
    assert_ne!(generated.value(0), "a0001");
    Ok(())
}

#[test]
fn fixed_binary_replay_namespace_never_aliases_sample_prefix() -> anyhow::Result<()> {
    let seed = 0x0102_0304_0506_0708_u64;
    let mut occupied = [0_u8; 16];
    occupied[..8].copy_from_slice(&seed.to_be_bytes());
    let mut free = [0_u8; 16];
    free[..8].copy_from_slice(&seed.wrapping_add(2).to_be_bytes());
    let batch = RecordBatch::try_from_iter(vec![(
        "id",
        Arc::new(FixedSizeBinaryArray::try_from_iter(
            [occupied.as_slice(), free.as_slice()].into_iter(),
        )?) as ArrayRef,
    )])?;

    let namespace = fixed_binary_namespace_many(&[&batch], 0, seed)?;

    assert_eq!(namespace, seed.wrapping_add(1));
    Ok(())
}

#[test]
fn unbounded_string_primary_key_has_no_hidden_four_digit_iteration_limit() -> anyhow::Result<()> {
    let batch = RecordBatch::try_from_iter(vec![(
        "id",
        Arc::new(StringArray::from(vec!["source-key"])) as ArrayRef,
    )])?;
    let column = SchemaColumn::new("id".to_owned(), DataType::Utf8, false)
        .with_constraints(true, false, None);
    let kind = build_unique_key_kind_many(&[&batch], 0, &column)?;
    assert!(matches!(kind, UniqueKeyKind::UnboundedUtf8));
    let key = UniqueKey {
        column: 0,
        kind,
        namespace: unbounded_namespace_many(&[&batch], 0, kind, 7)?,
        sample_rows: 1,
        forbidden_iterations: BTreeSet::new(),
    };

    let iteration = 36_u128.pow(4) + 1;
    let (generated, _) = rewrite_unique_key(&batch, &key, iteration, 0)?;
    let generated = generated
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    assert_ne!(generated.value(0), "source-key");
    assert!(generated.value(0).ends_with(&format!("{iteration:032x}")));
    Ok(())
}

#[test]
fn fixed_binary_ordinals_are_disjoint_across_sampled_deliveries() -> anyhow::Result<()> {
    let first = [1_u8; 16];
    let second = [2_u8; 16];
    let first = RecordBatch::try_from_iter(vec![(
        "id",
        Arc::new(FixedSizeBinaryArray::try_from_iter(
            [first.as_slice()].into_iter(),
        )?) as ArrayRef,
    )])?;
    let second = RecordBatch::try_from_iter(vec![(
        "id",
        Arc::new(FixedSizeBinaryArray::try_from_iter(
            [second.as_slice()].into_iter(),
        )?) as ArrayRef,
    )])?;
    let key = UniqueKey {
        column: 0,
        kind: UniqueKeyKind::FixedSizeBinary {
            width: 16,
            max_iteration: u128::from(u64::MAX) / 2,
        },
        namespace: 42,
        sample_rows: 2,
        forbidden_iterations: BTreeSet::new(),
    };

    let (first, _) = rewrite_unique_key(&first, &key, 1, 0)?;
    let (second, _) = rewrite_unique_key(&second, &key, 1, 1)?;
    let first = first
        .column(0)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let second = second
        .column(0)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();

    assert_ne!(first.value(0), second.value(0));
    assert_eq!(&first.value(0)[8..], &2_u64.to_be_bytes());
    assert_eq!(&second.value(0)[8..], &3_u64.to_be_bytes());
    Ok(())
}

#[tokio::test]
async fn collector_keeps_one_accounted_sample_for_every_dataset() -> anyhow::Result<()> {
    let memory = PipelineMemory::new(1_024 * 1_024);
    let collector = ProfileCollector::new(1_024 * 1_024);
    for (id, table) in [(0, "first"), (1, "second")] {
        let batch = RecordBatch::try_from_iter(vec![(
            "value",
            Arc::new(Int64Array::from(vec![id])) as ArrayRef,
        )])?;
        let bytes = batch.get_array_memory_size();
        let reservation = memory.reserve(bytes).await;
        collector
            .add(&Delivery {
                id: DeliveryId::new(id as u64),
                outputs: vec![SinkBatch {
                    table: Arc::from(table),
                    is_dlq: false,
                    batch,
                    byte_size: bytes,
                    memory: reservation,
                    system_columns: SystemColumns::default(),
                }],
                meta: DeliveryMeta::default(),
            })
            .await?;
    }

    let snapshot = collector.snapshot()?;
    assert_eq!(snapshot.samples.len(), 2);
    let tables = snapshot
        .samples
        .iter()
        .flat_map(|delivery| delivery.outputs.iter())
        .map(|output| output.table.as_ref())
        .collect::<Vec<_>>();
    assert_eq!(tables, ["first", "second"]);
    assert_eq!(
        memory.used(),
        0,
        "file-spooled samples must not steal source pipeline capacity"
    );
    Ok(())
}

#[tokio::test]
async fn sampled_delivery_sequence_preserves_batch_sizes_dataset_mix_and_dlq_frequency(
) -> anyhow::Result<()> {
    let memory = PipelineMemory::new(2 * 1_024 * 1_024);
    let collector = ProfileCollector::new(2 * 1_024 * 1_024);
    for (id, outputs) in [
        vec![("a", false, 1_usize)],
        vec![("a", false, 4_096)],
        vec![("b", false, 2), ("dlq", true, 1)],
    ]
    .into_iter()
    .enumerate()
    {
        let mut batches = Vec::new();
        for (table, is_dlq, rows) in outputs {
            let row_count = i64::try_from(rows)?;
            let batch = RecordBatch::try_from_iter(vec![(
                "value",
                Arc::new(Int64Array::from_iter_values(0..row_count)) as ArrayRef,
            )])?;
            let bytes = batch.get_array_memory_size();
            batches.push(SinkBatch {
                table: Arc::from(table),
                is_dlq,
                batch,
                byte_size: bytes,
                memory: memory.reserve(bytes).await,
                system_columns: SystemColumns::default(),
            });
        }
        collector
            .add(&Delivery {
                id: DeliveryId::new(id as u64),
                outputs: batches,
                meta: DeliveryMeta::default(),
            })
            .await?;
    }

    let sampled = collector.snapshot()?;
    let mut discovered = discovery("a");
    discovered.datasets.push(DiscoveredDataset {
        role: DatasetRole::Main,
        name: Arc::from("b"),
        incoming_schema: DatasetSchema::default(),
        stored_schema: DatasetSchema::default(),
        system_columns: Vec::new(),
    });
    discovered.datasets.push(DiscoveredDataset {
        role: DatasetRole::DeadLetterQueue,
        name: Arc::from("dlq"),
        incoming_schema: DatasetSchema::default(),
        stored_schema: DatasetSchema::default(),
        system_columns: Vec::new(),
    });
    let discovered = Arc::new(discovered);
    let isolation =
        SinkSpeedtestIsolation::no_external_writes(cleanup_connector(), Arc::clone(&discovered));
    let mut source = ProfileGeneratorSource::new(
        &sampled.samples,
        &discovered,
        &isolation,
        "00000000000000000000000000000001",
        PipelineMemory::new(2 * 1_024 * 1_024),
        Duration::from_secs(1),
    )
    .await?;

    let mut observed = Vec::new();
    for _ in 0..4 {
        let SourceBatch::Typed { tables, .. } = source.read_batch().await? else {
            panic!("sample sequence ended unexpectedly");
        };
        observed.push(
            tables
                .iter()
                .map(|table| {
                    (
                        table.table.to_string(),
                        table.is_dlq,
                        table.batch.num_rows(),
                    )
                })
                .collect::<Vec<_>>(),
        );
    }
    assert_eq!(
        observed,
        [
            vec![("a".to_owned(), false, 1)],
            vec![("a".to_owned(), false, 4_096)],
            vec![("b".to_owned(), false, 2), ("dlq".to_owned(), true, 1)],
            vec![("a".to_owned(), false, 1)],
        ]
    );
    Ok(())
}

#[tokio::test]
async fn oversized_first_progress_sample_is_retained_and_reported_as_truncated(
) -> anyhow::Result<()> {
    let memory = PipelineMemory::new(1_024);
    let collector = ProfileCollector::new(1);
    let batch = RecordBatch::try_from_iter(vec![(
        "value",
        Arc::new(Int64Array::from_iter_values(0..128)) as ArrayRef,
    )])?;
    let bytes = batch.get_array_memory_size();
    collector
        .add(&Delivery {
            id: DeliveryId::new(0),
            outputs: vec![SinkBatch {
                table: Arc::from("a"),
                is_dlq: false,
                batch,
                byte_size: bytes,
                memory: memory.reserve(bytes).await,
                system_columns: SystemColumns::default(),
            }],
            meta: DeliveryMeta::default(),
        })
        .await?;

    let snapshot = collector.snapshot()?;
    assert_eq!(snapshot.samples.len(), 1);
    assert!(snapshot.truncated);
    assert!(snapshot.sampled_arrow_bytes > snapshot.sample_limit_bytes);
    Ok(())
}

#[tokio::test]
async fn destination_first_replay_does_not_double_reserve_the_sample() -> anyhow::Result<()> {
    let batch = RecordBatch::try_from_iter(vec![
        (
            "id",
            Arc::new(StringArray::from_iter_values(
                (0..8_192).map(|value| format!("key-{value}")),
            )) as ArrayRef,
        ),
        (
            "value",
            Arc::new(Int64Array::from_iter_values(0..8_192)) as ArrayRef,
        ),
    ])?;
    let arrow_bytes = batch.get_array_memory_size();
    let output = SpooledOutput {
        table: Arc::from("logical"),
        is_dlq: false,
        system_columns: SystemColumns::default(),
        schema: batch.schema(),
        arrow_bytes,
        file: Arc::new(spool_record_batch(&batch)?),
        profile: profile_batch_state("logical", false, &batch)?,
    };
    let mut discovery = discovery("logical");
    let schema = DatasetSchema::new(vec![
        SchemaColumn::new("id".to_owned(), DataType::Utf8, false)
            .with_constraints(true, false, None),
        SchemaColumn::new("value".to_owned(), DataType::Int64, false),
    ]);
    discovery.datasets[0].incoming_schema = schema.clone();
    discovery.datasets[0].stored_schema = schema;
    let discovery = Arc::new(discovery);
    let isolation =
        SinkSpeedtestIsolation::no_external_writes(cleanup_connector(), Arc::clone(&discovery));
    let memory = PipelineMemory::new(arrow_bytes);
    let mut source = ProfileGeneratorSource::new(
        &[SpooledDelivery {
            outputs: Arc::from([output]),
        }],
        &discovery,
        &isolation,
        "00000000000000000000000000000001",
        memory,
        Duration::from_secs(1),
    )
    .await?;

    let first = tokio::time::timeout(Duration::from_secs(1), source.read_batch()).await??;
    drop(first);
    let second = tokio::time::timeout(Duration::from_secs(1), source.read_batch()).await??;

    assert!(matches!(second, SourceBatch::Typed { .. }));
    Ok(())
}

struct CleanupConnector {
    cleaned: Arc<Mutex<Vec<String>>>,

    notification: Arc<Notify>,
}

struct RetryCleanupConnector {
    attempts: Arc<AtomicUsize>,

    cleaned: Arc<AtomicUsize>,

    failures_before_success: usize,
}

impl SinkConnector for CleanupConnector {
    fn compatibility(&self) -> EndpointDescriptor {
        EndpointDescriptor::Discard
    }

    fn limits(&self) -> &dyn transferia_core::delivery::SinkLimits {
        &NO_LIMITS
    }

    fn destination_type(
        &self,
        _column: &transferia_core::data::schema::SchemaColumn,
    ) -> anyhow::Result<String> {
        Ok("test".to_owned())
    }

    fn prepare(&self, _request: SinkPrepare) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn build_sink(
        &self,
        _context: SinkBuildContext,
    ) -> BoxFuture<'_, anyhow::Result<Box<dyn Sink>>> {
        Box::pin(async { anyhow::bail!("unused test sink") })
    }

    fn cleanup_speedtest<'a>(
        &'a self,
        isolation: &'a SinkSpeedtestIsolation,
    ) -> BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(async move {
            let mut cleaned = self.cleaned.lock().unwrap();
            cleaned.extend(
                isolation
                    .physical_targets()
                    .iter()
                    .map(|target| target.scratch.to_string()),
            );
            drop(cleaned);
            self.notification.notify_one();
            Ok(())
        })
    }
}

impl SinkConnector for RetryCleanupConnector {
    fn compatibility(&self) -> EndpointDescriptor {
        EndpointDescriptor::Discard
    }

    fn limits(&self) -> &dyn transferia_core::delivery::SinkLimits {
        &NO_LIMITS
    }

    fn destination_type(
        &self,
        _column: &transferia_core::data::schema::SchemaColumn,
    ) -> anyhow::Result<String> {
        Ok("test".to_owned())
    }

    fn prepare(&self, _request: SinkPrepare) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn build_sink(
        &self,
        _context: SinkBuildContext,
    ) -> BoxFuture<'_, anyhow::Result<Box<dyn Sink>>> {
        Box::pin(async { anyhow::bail!("unused test sink") })
    }

    fn cleanup_speedtest<'a>(
        &'a self,
        _isolation: &'a SinkSpeedtestIsolation,
    ) -> BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(async move {
            if self.attempts.fetch_add(1, Ordering::SeqCst) < self.failures_before_success {
                anyhow::bail!("injected secret=password cleanup failure");
            }
            self.cleaned.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    }
}

fn discovery(name: &str) -> DeliveryDiscovery {
    DeliveryDiscovery {
        source_name: Arc::from("test"),
        source_topology: SourceTopology::StaticPartitions(vec![0]),
        schema_origin: SchemaOrigin::SourceNative,
        keep_system_columns: false,
        datasets: vec![DiscoveredDataset {
            role: DatasetRole::Main,
            name: Arc::from(name),
            incoming_schema: DatasetSchema::default(),
            stored_schema: DatasetSchema::default(),
            system_columns: Vec::new(),
        }],
        performance_advice: Vec::new(),
    }
}

fn cleanup_connector() -> Arc<dyn SinkConnector> {
    Arc::new(CleanupConnector {
        cleaned: Arc::new(Mutex::new(Vec::new())),
        notification: Arc::new(Notify::new()),
    })
}

#[test]
fn scratch_isolation_rejects_every_production_physical_target_alias() {
    let original = discovery("logical");
    let isolated = discovery("logical");
    let error = SinkSpeedtestIsolation::scratch(
        cleanup_connector(),
        &original,
        isolated,
        BTreeMap::from([(Arc::from("logical"), Arc::from("logical"))]),
        vec![SpeedtestPhysicalTarget {
            production: Arc::from("db.production"),
            scratch: Arc::from("db.production"),
        }],
    )
    .err()
    .expect("production alias must fail closed");

    assert!(error.to_string().contains("aliases a production target"));
}

#[test]
fn scratch_isolation_allows_identity_logical_mapping_for_explicit_paths() -> anyhow::Result<()> {
    let original = discovery("logical");
    let isolated = discovery("logical");

    let isolation = SinkSpeedtestIsolation::scratch(
        cleanup_connector(),
        &original,
        isolated,
        BTreeMap::from([(Arc::from("logical"), Arc::from("logical"))]),
        vec![SpeedtestPhysicalTarget {
            production: Arc::from("/production/path"),
            scratch: Arc::from("/scratch/path"),
        }],
    )?;

    assert_eq!(isolation.table_name("logical")?.as_ref(), "logical");
    assert_eq!(isolation.safety(), SinkSpeedtestIsolationSafety::Scratch);
    Ok(())
}

#[test]
fn scratch_isolation_requires_one_physical_target_per_dataset() {
    let mut original = discovery("first");
    original.datasets.push(DiscoveredDataset {
        name: Arc::from("second"),
        ..original.datasets[0].clone()
    });
    let isolated = original.clone();
    let error = SinkSpeedtestIsolation::scratch(
        cleanup_connector(),
        &original,
        isolated,
        BTreeMap::from([
            (Arc::from("first"), Arc::from("first")),
            (Arc::from("second"), Arc::from("second")),
        ]),
        vec![SpeedtestPhysicalTarget {
            production: Arc::from("db.production"),
            scratch: Arc::from("db.scratch"),
        }],
    )
    .err()
    .expect("a shared or missing physical target must fail closed");

    assert!(error
        .to_string()
        .contains("one physical target per dataset"));
}

#[tokio::test]
async fn isolated_connector_rejects_substituted_prepare_before_io() -> anyhow::Result<()> {
    let original = discovery("production");
    let isolated = discovery("scratch");
    let isolation = SinkSpeedtestIsolation::scratch(
        cleanup_connector(),
        &original,
        isolated,
        BTreeMap::from([(Arc::from("production"), Arc::from("scratch"))]),
        vec![SpeedtestPhysicalTarget {
            production: Arc::from("db.production"),
            scratch: Arc::from("db.scratch"),
        }],
    )?;
    let substituted = discovery("production");
    let request =
        SinkPrepare::from_discovery(&substituted, true, "speedtest", None)?.expect("one dataset");

    let error = isolation
        .connector()
        .prepare(request)
        .await
        .expect_err("production discovery must not reach the isolated connector");

    assert!(error.to_string().contains("changed isolated dataset"));
    Ok(())
}

#[test]
fn scratch_isolation_rejects_changed_dataset_contract() {
    let original = discovery("production");
    let mut isolated = discovery("scratch");
    isolated.datasets[0].role = DatasetRole::DeadLetterQueue;

    let error = SinkSpeedtestIsolation::scratch(
        cleanup_connector(),
        &original,
        isolated,
        BTreeMap::from([(Arc::from("production"), Arc::from("scratch"))]),
        vec![SpeedtestPhysicalTarget {
            production: Arc::from("db.production"),
            scratch: Arc::from("db.scratch"),
        }],
    )
    .err()
    .expect("scratch discovery must preserve roles and schemas");

    assert!(error.to_string().contains("changed the schema or role"));
}

#[tokio::test]
async fn cancelled_owner_still_cleans_only_the_scratch_target() -> anyhow::Result<()> {
    let cleaned = Arc::new(Mutex::new(Vec::new()));
    let notification = Arc::new(Notify::new());
    let connector: Arc<dyn SinkConnector> = Arc::new(CleanupConnector {
        cleaned: Arc::clone(&cleaned),
        notification: Arc::clone(&notification),
    });
    let original = discovery("production");
    let isolated = discovery("scratch");
    let isolation = SinkSpeedtestIsolation::scratch(
        connector,
        &original,
        isolated,
        BTreeMap::from([(Arc::from("production"), Arc::from("scratch"))]),
        vec![SpeedtestPhysicalTarget {
            production: Arc::from("db.production"),
            scratch: Arc::from("db.__speedtest_123"),
        }],
    )?;
    let started = Arc::new(Notify::new());
    let task_started = Arc::clone(&started);
    let cleanup_tasks = TaskTracker::new();
    let task_cleanup_tasks = cleanup_tasks.clone();
    let task = tokio::spawn(async move {
        let _guard = CleanupGuard::new(isolation, Duration::from_secs(1), task_cleanup_tasks);
        task_started.notify_one();
        std::future::pending::<()>().await;
    });

    started.notified().await;
    // Control-plane shutdown closes the tracker before waiting for all tracked
    // request owners. Dropping an aborted owner must still register cleanup on
    // that closed tracker before the owner itself leaves the tracked set.
    cleanup_tasks.close();
    task.abort();
    drop(task.await);
    tokio::time::timeout(Duration::from_secs(1), cleanup_tasks.wait()).await?;
    tokio::time::timeout(Duration::from_secs(1), notification.notified()).await?;

    assert_eq!(&*cleaned.lock().unwrap(), &["db.__speedtest_123"]);
    Ok(())
}

#[tokio::test]
async fn panicking_destination_still_awaits_scratch_cleanup() -> anyhow::Result<()> {
    let cleaned = Arc::new(Mutex::new(Vec::new()));
    let connector: Arc<dyn SinkConnector> = Arc::new(CleanupConnector {
        cleaned: Arc::clone(&cleaned),
        notification: Arc::new(Notify::new()),
    });
    let original = discovery("production");
    let isolation = SinkSpeedtestIsolation::scratch(
        connector,
        &original,
        discovery("scratch"),
        BTreeMap::from([(Arc::from("production"), Arc::from("scratch"))]),
        vec![SpeedtestPhysicalTarget {
            production: Arc::from("db.production"),
            scratch: Arc::from("db.__speedtest_panic"),
        }],
    )?;

    let error = run_with_cleanup(
        CleanupGuard::new(isolation, Duration::from_secs(1), TaskTracker::new()),
        async {
            panic!("injected destination panic");
            #[allow(
                unreachable_code,
                reason = "the expression type after the intentional panic is required by the test"
            )]
            Ok::<(), anyhow::Error>(())
        },
        CancellationToken::new(),
    )
    .await
    .expect_err("destination panic must be reported");

    assert!(error.to_string().contains("panicked"));
    assert_eq!(&*cleaned.lock().unwrap(), &["db.__speedtest_panic"]);
    Ok(())
}

#[tokio::test]
async fn transient_cleanup_is_retried_in_tracked_scope_before_shutdown_completes(
) -> anyhow::Result<()> {
    let attempts = Arc::new(AtomicUsize::new(0));
    let cleaned = Arc::new(AtomicUsize::new(0));
    let connector: Arc<dyn SinkConnector> = Arc::new(RetryCleanupConnector {
        attempts: Arc::clone(&attempts),
        cleaned: Arc::clone(&cleaned),
        failures_before_success: 1,
    });
    let original = discovery("production");
    let isolation = SinkSpeedtestIsolation::scratch(
        connector,
        &original,
        discovery("scratch"),
        BTreeMap::from([(Arc::from("production"), Arc::from("scratch"))]),
        vec![SpeedtestPhysicalTarget {
            production: Arc::from("db.production"),
            scratch: Arc::from("db.__speedtest_retry"),
        }],
    )?;
    let cleanup_tasks = TaskTracker::new();
    let mut cleanup = CleanupGuard::new(isolation, Duration::from_secs(1), cleanup_tasks.clone());
    cleanup.cleanup().await?;

    cleanup_tasks.close();
    tokio::time::timeout(Duration::from_secs(1), cleanup_tasks.wait()).await?;
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert_eq!(cleaned.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn exhausted_cleanup_reports_exact_target_without_connector_error_secrets(
) -> anyhow::Result<()> {
    let connector: Arc<dyn SinkConnector> = Arc::new(RetryCleanupConnector {
        attempts: Arc::new(AtomicUsize::new(0)),
        cleaned: Arc::new(AtomicUsize::new(0)),
        failures_before_success: usize::MAX,
    });
    let original = discovery("production");
    let isolation = SinkSpeedtestIsolation::scratch(
        connector,
        &original,
        discovery("scratch"),
        BTreeMap::from([(Arc::from("production"), Arc::from("scratch"))]),
        vec![SpeedtestPhysicalTarget {
            production: Arc::from("db.production"),
            scratch: Arc::from("db.__speedtest_manual_cleanup"),
        }],
    )?;

    let error = cleanup_until_deadline(
        Arc::clone(isolation.connector()),
        isolation,
        Duration::from_millis(20),
    )
    .await
    .expect_err("a permanently failing cleanup must require manual recovery");
    let message = error.to_string();

    assert!(error.downcast_ref::<SpeedtestCleanupFailure>().is_some());
    assert!(message.contains("manual cleanup required"));
    assert!(message.contains("db.__speedtest_manual_cleanup"));
    assert!(!message.contains("secret"));
    assert!(!message.contains("password"));
    Ok(())
}

#[tokio::test]
async fn source_and_destination_ephemeral_state_are_disjoint() -> anyhow::Result<()> {
    let source = ephemeral_durable("delivery", "source");
    let destination = ephemeral_durable("delivery", "destination");

    source
        .storage
        .compare_exchange("offset", None, b"source")
        .await?;

    assert_ne!(source.delivery_id, destination.delivery_id);
    assert!(destination.storage.read("offset").await?.is_none());
    Ok(())
}
