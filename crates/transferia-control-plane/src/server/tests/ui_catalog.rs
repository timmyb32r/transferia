use super::build_ui_catalog;

// Resolve authored fields through references and every union branch, carrying
// visibility/grouping from parent objects. Looking at a leaf alone would miss
// hidden ancestors and controls grouped by a whole reader/writer object.
fn performance_placements(
    root: &serde_json::Value,
    node: &serde_json::Value,
    path: &[&str],
    performance: bool,
    visible: bool,
) -> Vec<bool> {
    let performance = performance || node["x-ui"]["section"] == "performance";
    let visible = visible && node["x-ui"]["widget"] != "hidden";
    if let Some(reference) = node["$ref"].as_str() {
        return performance_placements(
            root,
            root.pointer(reference.strip_prefix('#').unwrap()).unwrap(),
            path,
            performance,
            visible,
        );
    }
    if path.is_empty() {
        return vec![performance && visible];
    }
    let mut placements = Vec::new();
    if let Some(child) = node["properties"].get(path[0]) {
        placements.extend(performance_placements(
            root,
            child,
            &path[1..],
            performance,
            visible,
        ));
    }
    for keyword in ["oneOf", "anyOf", "allOf"] {
        if let Some(branches) = node[keyword].as_array() {
            for branch in branches {
                placements.extend(performance_placements(
                    root,
                    branch,
                    path,
                    performance,
                    visible,
                ));
            }
        }
    }
    placements
}

#[test]
fn every_registered_autotuning_parameter_is_editable_in_performance_options() -> anyhow::Result<()>
{
    use transferia_registry::{Composition, EndpointRole};
    let transferia = transferia_connectors::extension::Transferia::public()?;
    let registry = transferia.build_registry(&std::sync::Arc::new(
        transferia_connectors::metrics::MetricsRegistry::new(),
    ))?;
    for connector in transferia.composition().connector_definitions() {
        for (endpoint, role) in [
            (connector.source.as_ref(), EndpointRole::Source),
            (connector.sink.as_ref(), EndpointRole::Sink),
        ] {
            let Some(endpoint) = endpoint else { continue };
            for parameter in registry.tuning_parameters(connector.key, role)? {
                let path = parameter.pointer().split('/').skip(1).collect::<Vec<_>>();
                let placements =
                    performance_placements(&endpoint.schema, &endpoint.schema, &path, false, true);
                assert!(
                    !placements.is_empty() && placements.iter().all(|placed| *placed),
                    "{} {role:?} {} is missing, hidden, or outside Performance options",
                    connector.key,
                    parameter.pointer()
                );
            }
        }
    }
    Ok(())
}

#[test]
fn manually_benchmarked_controls_remain_editable_in_performance_options() -> anyhow::Result<()> {
    // Manual profiles include knobs beyond the bounded automatic search space.
    // Provenance and the full inventory are in docs/performance-options.md.
    let catalog = build_ui_catalog()?;
    for (key, source, paths) in [
        ("clickhouse", true, "batch_rows snapshot_reader.type snapshot_reader.compression snapshot_reader.max_threads snapshot_reader.row_group_rows snapshot_reader.decode_threads"),
        ("clickhouse", false, "insert_format compression insert_target_rows insert_target_bytes insert_concurrency parquet_row_group_rows flush_interval_ms retry_initial_ms retry_max_ms retry_max_attempts"),
        ("iceberg", true, "read_batch_rows read_data_file_concurrency read_manifest_concurrency parquet_metadata_size_hint_bytes parquet_range_coalesce_bytes parquet_range_fetch_concurrency"),
        ("iceberg", false, "parquet_compression parquet_row_group_rows write_concurrency target_file_size_bytes commit_target_size_bytes"),
        ("postgres", true, "batch_rows copy_to_format"),
        ("postgres", false, "copy_from_format"),
        ("mysql", true, "batch_rows read_protocol"),
        ("mysql", false, "insert_rows"),
        ("opensearch", true, "page_rows read_concurrency request_timeout_ms max_response_bytes retry_initial_ms retry_max_ms retry_max_attempts pit_keep_alive_ms"),
        ("opensearch", false, "bulk_target_rows bulk_target_bytes bulk_concurrency request_timeout_ms max_response_bytes retry_initial_ms retry_max_ms retry_max_attempts flush_interval_ms"),
        ("ytsaurus", true, "batch_rows read_ordering.type read_ordering.compressed_data_size_per_partition read_ordering.max_partition_count read_ordering.concurrency"),
        ("ytsaurus", false, "write_target_bytes write_concurrency write_row_buffer_bytes table_writer.desired_chunk_size"),
        ("logbroker", true, "pqv1_decompression_concurrency"),
        ("s3", false, "rotation.max_rows rotation.max_bytes buffering.max_epoch_buffers buffering.max_pending_upload_objects buffering.max_buffered_bytes buffering.max_epoch_bytes upload.multipart_threshold upload.part_size upload.parallel_parts upload.max_in_flight_objects upload.operation_timeout retry.initial_backoff retry.max_backoff retry.max_attempts"),
    ] {
        let connector = catalog.connectors.iter().find(|connector| connector.key == key).unwrap();
        let endpoint = if source { connector.source.as_ref() } else { connector.sink.as_ref() }.unwrap();
        for path in paths.split_whitespace() {
            let segments = path.split('.').collect::<Vec<_>>();
            let placements = performance_placements(&endpoint.schema, &endpoint.schema, &segments, false, true);
            assert!(!placements.is_empty() && placements.iter().all(|placed| *placed), "{key} source={source}: {path}");
        }
    }
    Ok(())
}

#[test]
fn database_type_examples_reach_the_ui_catalog_without_losing_errors() -> anyhow::Result<()> {
    let catalog = build_ui_catalog()?;
    for key in [
        "postgres",
        "clickhouse",
        "mysql",
        "ydb",
        "ytsaurus",
        "iceberg",
        "opensearch",
    ] {
        let connector = catalog
            .connectors
            .iter()
            .find(|connector| connector.key == key)
            .unwrap();
        for endpoint in [connector.source.as_ref(), connector.sink.as_ref()] {
            let mapping = endpoint
                .unwrap()
                .type_mapping
                .as_ref()
                .expect("native endpoints must expose runtime examples");
            assert!(!mapping.context.is_empty());
            assert!(
                mapping.rows.iter().any(|row| row.output.is_some()),
                "{key}: every example was rejected"
            );
            assert!(mapping
                .rows
                .iter()
                .all(|row| row.output.is_some() != row.error.is_some()));
            let inputs = mapping
                .rows
                .iter()
                .map(|row| &row.input)
                .collect::<std::collections::BTreeSet<_>>();
            assert_eq!(
                inputs.len(),
                mapping.rows.len(),
                "{key}: duplicate example inputs"
            );
        }
    }
    Ok(())
}

#[test]
fn middleware_schema_is_derived_from_registered_components() -> anyhow::Result<()> {
    let catalog = build_ui_catalog()?;
    let alternatives = catalog.common_schema["properties"]["middlewares"]["items"]["oneOf"]
        .as_array()
        .expect("middleware items must be a oneOf schema");
    let keys = alternatives
        .iter()
        .map(|alternative| {
            alternative["required"][0]
                .as_str()
                .expect("each middleware variant must require its action")
        })
        .collect::<Vec<_>>();

    assert_eq!(keys, ["filter", "rename_table", "datafusion"]);
    assert_eq!(
        alternatives[2]["properties"]["datafusion"]["properties"]["sql"]["type"],
        "string"
    );
    Ok(())
}

#[test]
fn each_transform_variant_exposes_one_shared_table_scope() -> anyhow::Result<()> {
    let catalog = build_ui_catalog()?;
    let alternatives = catalog.common_schema["properties"]["middlewares"]["items"]["oneOf"]
        .as_array()
        .unwrap();
    for alternative in alternatives {
        let scope = &alternative["properties"]["tables"];
        assert_eq!(scope["properties"]["include"]["type"], "string");
        assert_eq!(scope["required"], serde_json::json!(["include"]));
        assert_eq!(scope["additionalProperties"], false);
        assert_eq!(
            scope["properties"]["include_mode"]["enum"],
            serde_json::json!(["glob", "regex"])
        );
        assert_eq!(alternative["properties"].as_object().unwrap().len(), 2);
    }
    Ok(())
}
