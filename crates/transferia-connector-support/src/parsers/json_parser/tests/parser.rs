use super::*;
use crate::parsers::json_parser::{
    ColumnMapping, ConversionErrorPolicy, EpochUnit, JsonDataType, JsonParserConfig,
    TimeConversion, UnknownFieldPolicy,
};
use arrow::array::{Array as _, AsArray as _};
use base64::Engine as _;
use bytes::Bytes;

fn mapping(jsonpath: &str, name: &str, arrow_type: &str, nullable: bool) -> ColumnMapping {
    ColumnMapping::new(jsonpath.into(), name.into(), arrow_type.into(), nullable)
}

fn parser_config(columns: Vec<ColumnMapping>, json_framing: JsonFramingMode) -> JsonParserConfig {
    JsonParserConfig {
        columns,
        json_framing,
        conversion_error: ConversionErrorPolicy::Dlq,
        unknown_fields: UnknownFieldPolicy::Fail,
        keys: Vec::new(),
    }
}

fn parser_for(
    columns: Vec<crate::parsers::json_parser::ColumnMapping>,
) -> anyhow::Result<JsonParser> {
    JsonParser::new(
        &parser_config(columns, JsonFramingMode::SingleDocument),
        &crate::parsers::SystemColumnsConfig::default(),
        "test".into(),
    )
}

fn parse_with_config(
    config: &JsonParserConfig,
    system: &crate::parsers::SystemColumnsConfig,
    payload: &'static [u8],
) -> anyhow::Result<(TableData, Option<TableData>)> {
    JsonParser::new(config, system, "test".into())?.parse_into(
        vec![Message::new(Bytes::from_static(payload))],
        &mut ParserWorkspace::new(),
    )
}

#[test]
fn conversion_policy_and_time_conversion_are_explicit() -> anyhow::Result<()> {
    let mut mapping = mapping("$.at", "at", "Timestamp(Millisecond, UTC)", false);
    mapping.json_data_type = JsonDataType::String;
    mapping.time_conversion = Some(TimeConversion::String {
        format: "[year]-[month]-[day]T[hour]:[minute]:[second]Z".into(),
    });
    let mut config = parser_config(vec![mapping], JsonFramingMode::SingleDocument);
    let (main, dlq) = parse_with_config(
        &config,
        &crate::parsers::SystemColumnsConfig::default(),
        b"{\"at\":\"1970-01-01T00:00:01Z\"}",
    )?;
    assert!(dlq.is_none());
    let values = main
        .batch
        .column(0)
        .as_primitive::<arrow::datatypes::TimestampMillisecondType>();
    assert_eq!(values.value(0), 1_000);

    config.conversion_error = ConversionErrorPolicy::Fail;
    let error = parse_with_config(
        &config,
        &crate::parsers::SystemColumnsConfig::default(),
        b"{\"at\":\"bad\"}",
    )
    .expect_err("conversion_error=fail must stop the delivery");
    assert!(error.to_string().contains("parse"));
    Ok(())
}

#[test]
fn parse_errors_can_be_dropped_without_dlq_output() -> anyhow::Result<()> {
    let mut config = parser_config(
        vec![mapping("$.id", "id", "Int64", false)],
        JsonFramingMode::JsonLines,
    );
    config.conversion_error = ConversionErrorPolicy::Drop;
    let (main, dlq) = parse_with_config(
        &config,
        &crate::parsers::SystemColumnsConfig::default(),
        b"{\"id\":1}\nnot-json\n{\"id\":\"wrong\"}\n{\"id\":2}",
    )?;
    assert_eq!(main.batch.num_rows(), 2);
    assert!(dlq.is_none());
    Ok(())
}

#[test]
fn dlq_extraction_error_names_the_failed_column() -> anyhow::Result<()> {
    let config = parser_config(
        vec![mapping("$.required_id", "id", "Int64", false)],
        JsonFramingMode::SingleDocument,
    );
    let (_main, dlq) = parse_with_config(
        &config,
        &crate::parsers::SystemColumnsConfig::default(),
        b"{}",
    )?;
    let dlq = dlq.expect("missing required column must reach DLQ");
    let error = string_col(&dlq.batch, 1)?.value(0);
    assert!(error.contains("JSONPath extraction failed"), "{error}");
    assert!(error.contains("'id'"), "{error}");
    Ok(())
}

#[test]
fn unknown_fields_can_be_rejected_or_sent_to_a_column() -> anyhow::Result<()> {
    let id_mapping = mapping("$.id", "id", "Int64", false);
    let fail = parser_config(vec![id_mapping.clone()], JsonFramingMode::SingleDocument);
    let (main, dlq) = parse_with_config(
        &fail,
        &crate::parsers::SystemColumnsConfig::default(),
        b"{\"id\":1,\"extra\":true}",
    )?;
    assert_eq!(main.batch.num_rows(), 0);
    assert_eq!(
        dlq.expect("unknown field must reach DLQ").batch.num_rows(),
        1
    );

    let mut captured = parser_config(vec![id_mapping], JsonFramingMode::SingleDocument);
    captured.unknown_fields = UnknownFieldPolicy::SendToColumn {
        column_name: "additional_properties".into(),
    };
    let (main, dlq) = parse_with_config(
        &captured,
        &crate::parsers::SystemColumnsConfig::default(),
        b"{\"id\":1,\"extra\":true}",
    )?;
    assert!(dlq.is_none());
    assert_eq!(string_col(&main.batch, 1)?.value(0), "{\"extra\":true}");

    let mut drop = parser_config(
        vec![mapping("$.id", "id", "Int64", false)],
        JsonFramingMode::SingleDocument,
    );
    drop.unknown_fields = UnknownFieldPolicy::Drop;
    let (main, dlq) = parse_with_config(
        &drop,
        &crate::parsers::SystemColumnsConfig::default(),
        b"{\"id\":1,\"extra\":true}",
    )?;
    assert_eq!(main.batch.num_rows(), 1);
    assert!(dlq.is_none());
    Ok(())
}

#[test]
fn renamed_system_columns_are_materialized_physically() -> anyhow::Result<()> {
    let config = parser_config(
        vec![mapping("$.id", "id", "Int64", false)],
        JsonFramingMode::SingleDocument,
    );
    let system = crate::parsers::SystemColumnsConfig {
        offset: Some("source_offset".into()),
        ..Default::default()
    };
    let parser = JsonParser::new(&config, &system, "test".into())?;
    let message = Message {
        value: Bytes::from_static(b"{\"id\":1}"),
        key: None,
        headers: Arc::from([]),
        meta: transferia_core::data::message::MessageMeta {
            offset: Some(42),
            ..Default::default()
        },
    };
    let (main, _) = parser.parse_into(vec![message], &mut ParserWorkspace::new())?;
    assert_eq!(main.batch.schema().field(1).name(), "source_offset");
    assert_eq!(
        main.system_columns
            .get(SystemColumnKind::Offset)
            .expect("offset metadata")
            .name
            .as_ref(),
        "source_offset"
    );
    Ok(())
}

#[test]
fn duplicate_root_path_populates_every_output_column() -> anyhow::Result<()> {
    let parser = parser_for(vec![
        mapping("$.id", "left", "Int64", false),
        mapping("$.id", "right", "Int64", true),
    ])?;
    anyhow::ensure!(matches!(parser.mode, ParseMode::Mixed));
    let (main, dlq) = parser.parse_into(
        vec![Message::new(Bytes::from_static(b"{\"id\":7}"))],
        &mut ParserWorkspace::new(),
    )?;
    anyhow::ensure!(dlq.is_none());
    anyhow::ensure!(int64_col(&main.batch, 0)?.value(0) == 7);
    anyhow::ensure!(int64_col(&main.batch, 1)?.value(0) == 7);
    Ok(())
}

#[test]
fn duplicate_mapped_root_key_reaches_dlq_in_fast_and_mixed_modes() -> anyhow::Result<()> {
    use crate::parsers::json_parser::ColumnMapping;

    let fast = parser_for(vec![ColumnMapping::new(
        "$.id".into(),
        "id".into(),
        "Int64".into(),
        false,
    )])?;
    anyhow::ensure!(matches!(fast.mode, ParseMode::AllRootField(_)));

    let mixed = parser_for(vec![
        mapping("$.id", "left", "Int64", false),
        mapping("$.id", "right", "Int64", true),
    ])?;
    anyhow::ensure!(matches!(mixed.mode, ParseMode::Mixed));

    for parser in [&fast, &mixed] {
        let (main, dlq) = parser.parse_into(
            vec![Message::new(Bytes::from_static(b"{\"id\":1,\"id\":null}"))],
            &mut ParserWorkspace::new(),
        )?;
        anyhow::ensure!(main.batch.num_rows() == 0);
        anyhow::ensure!(dlq.is_some_and(|batch| batch.batch.num_rows() == 1));
    }
    Ok(())
}

#[test]
fn invalid_complex_jsonpath_is_rejected_at_startup() {
    use crate::parsers::json_parser::ColumnMapping;

    let error = parser_for(vec![ColumnMapping::new(
        "$.items[".into(),
        "value".into(),
        "Utf8".into(),
        true,
    )])
    .err()
    .expect("invalid JSONPath must fail parser construction");
    assert!(error.to_string().contains("invalid JSONPath"));
}

#[test]
fn invalid_root_jsonpath_is_rejected_at_startup() {
    use crate::parsers::json_parser::ColumnMapping;

    let error = parser_for(vec![ColumnMapping::new(
        "$.".into(),
        "value".into(),
        "Utf8".into(),
        true,
    )])
    .err()
    .expect("invalid root JSONPath must fail parser construction");
    assert!(error.to_string().contains("invalid JSONPath"));
}

#[test]
fn empty_one_message_record_is_sent_to_dlq() -> anyhow::Result<()> {
    use crate::parsers::json_parser::ColumnMapping;

    let parser = parser_for(vec![ColumnMapping::new(
        "$.id".into(),
        "id".into(),
        "Int64".into(),
        false,
    )])?;
    let message = Message::new(Bytes::new());
    let bound = parser.output_memory_bound(core::slice::from_ref(&message));
    let (main, dlq) = parser.parse_into(vec![message], &mut ParserWorkspace::new())?;
    let dlq = dlq.expect("empty JSON must reach DLQ");
    assert_eq!(main.batch.num_rows(), 0);
    assert_eq!(dlq.batch.num_rows(), 1);
    assert!(dlq.batch.get_array_memory_size() <= bound);
    Ok(())
}

#[test]
fn dense_fixed_width_rows_have_a_type_aware_memory_bound() -> anyhow::Result<()> {
    use crate::parsers::json_parser::ColumnMapping;

    const ROWS: usize = 32_769;
    let parser = JsonParser::new(
        &JsonParserConfig {
            columns: (0..8)
                .map(|index| {
                    ColumnMapping::new(
                        format!("$.c{index}"),
                        format!("c{index}"),
                        "Int64".into(),
                        false,
                    )
                })
                .collect(),
            json_framing: JsonFramingMode::JsonLines,
            conversion_error: ConversionErrorPolicy::Dlq,
            unknown_fields: UnknownFieldPolicy::Fail,
            keys: Vec::new(),
        },
        &crate::parsers::SystemColumnsConfig::default(),
        "test".into(),
    )?;
    let row = r#"{"c0":0,"c1":1,"c2":2,"c3":3,"c4":4,"c5":5,"c6":6,"c7":7}"#;
    let payload = Bytes::from(
        core::iter::repeat_n(row, ROWS)
            .collect::<Vec<_>>()
            .join("\n"),
    );
    let message = Message::new(payload);
    let bound = parser.output_memory_bound(core::slice::from_ref(&message));

    assert!(
        bound < 256 * 1024 * 1024,
        "dense primitive rows must not be rejected by a per-cell 1KiB heuristic: {bound}"
    );
    let (main, dlq) = parser.parse_into(vec![message], &mut ParserWorkspace::new())?;
    assert_eq!(main.batch.num_rows(), ROWS);
    assert!(dlq.is_none());
    Ok(())
}

#[test]
fn dense_invalid_newline_rows_use_compact_dlq_descriptors() -> anyhow::Result<()> {
    use crate::parsers::json_parser::ColumnMapping;

    const ROWS: usize = 1_048_577;
    // Extraction failures retain a precise, user-visible column diagnostic.
    // Keep the descriptor bounded even though the enum therefore carries an
    // owned error string in its uncommon variant.
    assert!(core::mem::size_of::<DlqRecord>() <= 40);
    let parser = JsonParser::new(
        &JsonParserConfig {
            columns: vec![ColumnMapping::new(
                "$.id".into(),
                "id".into(),
                "Int64".into(),
                false,
            )],
            json_framing: JsonFramingMode::JsonLines,
            conversion_error: ConversionErrorPolicy::Dlq,
            unknown_fields: UnknownFieldPolicy::Fail,
            keys: Vec::new(),
        },
        &crate::parsers::SystemColumnsConfig {
            message_index: Some("_system_message_index".into()),
            ..crate::parsers::SystemColumnsConfig::default()
        },
        "test".into(),
    )?;
    let payload = Bytes::from(
        core::iter::repeat_n("x", ROWS)
            .collect::<Vec<_>>()
            .join("\n"),
    );
    let (main, dlq) =
        parser.parse_into(vec![Message::new(payload)], &mut ParserWorkspace::new())?;

    assert_eq!(main.batch.num_rows(), 0);
    let dlq = dlq.expect("invalid records must reach DLQ");
    assert_eq!(dlq.batch.num_rows(), ROWS);
    let raw = string_col(&dlq.batch, 0)?;
    assert_eq!(raw.value(0), "eA==");
    assert_eq!(raw.value(ROWS - 1), "eA==");
    let index = dlq
        .system_columns
        .get(transferia_core::data::system_columns::SystemColumnKind::MessageIndex)
        .expect("message index system column");
    let indexes = dlq
        .batch
        .column(index.index)
        .as_any()
        .downcast_ref::<arrow::array::UInt64Array>()
        .expect("message index values");
    assert_eq!(indexes.value(0), 0);
    assert_eq!(indexes.value(ROWS - 1), (ROWS - 1) as u64);
    Ok(())
}

#[test]
fn parser_session_has_no_hidden_limit_below_the_pipeline_budget() -> anyhow::Result<()> {
    let parser = Arc::new(parser_for(
        (0..8)
            .map(|index| {
                crate::parsers::json_parser::ColumnMapping::new(
                    format!("$.c{index}"),
                    format!("c{index}"),
                    "Utf8".into(),
                    false,
                )
            })
            .collect(),
    )?);
    let payload = Bytes::from(vec![b'x'; 32 * 1024 * 1024]);
    let estimated = parser.output_memory_bound(&[Message::new(payload.clone())]);
    assert!(estimated > 256 * 1024 * 1024);

    let mut allowed = parser.create_session(1024 * 1024 * 1024);
    let (_main, dlq) = allowed.parse_into(vec![Message::new(payload)])?;
    assert_eq!(
        dlq.expect("invalid JSON must reach DLQ").batch.num_rows(),
        1
    );
    Ok(())
}

#[test]
fn records_larger_than_four_mebibytes_have_no_hidden_limit() -> anyhow::Result<()> {
    let parser = JsonParser::new(
        &parser_config(
            vec![crate::parsers::json_parser::ColumnMapping::new(
                "$.value".into(),
                "value".into(),
                "Utf8".into(),
                false,
            )],
            JsonFramingMode::JsonLines,
        ),
        &crate::parsers::SystemColumnsConfig::default(),
        "test".into(),
    )?;
    let oversized_bytes = 4 * 1024 * 1024 + 1;
    let mut payload = b"{}\n".to_vec();
    payload.extend(core::iter::repeat_n(b'x', oversized_bytes));
    let (main, dlq) = parser.parse_into(
        vec![Message::new(Bytes::from(payload))],
        &mut ParserWorkspace::new(),
    )?;
    assert_eq!(main.batch.num_rows(), 0);
    assert_eq!(
        dlq.expect("invalid records must reach DLQ")
            .batch
            .num_rows(),
        2
    );
    Ok(())
}

#[test]
fn base64_is_streamed_directly_into_arrow_builder() -> anyhow::Result<()> {
    let raw = vec![0x5a; 2 * 1024 * 1024 + 1];
    let mut builder = StringBuilder::with_capacity(1, 0);
    append_base64(&mut builder, &raw)?;
    let array = builder.finish();
    assert_eq!(array.len(), 1);
    assert_eq!(
        base64::engine::general_purpose::STANDARD.decode(array.value(0))?,
        raw
    );
    Ok(())
}

#[test]
fn dlq_source_timestamp_is_deterministic() -> anyhow::Result<()> {
    use crate::parsers::json_parser::ColumnMapping;
    use arrow::array::Int64Array;

    let parser = parser_for(vec![ColumnMapping::new(
        "$.id".into(),
        "id".into(),
        "Int64".into(),
        false,
    )])?;
    let mut message = Message::new(Bytes::from_static(b"invalid"));
    message.meta.write_timestamp_ms = Some(1_234);
    let (_main, dlq) = parser.parse_into(vec![message], &mut ParserWorkspace::new())?;
    let dlq = dlq.expect("invalid JSON must reach DLQ");
    assert_eq!(
        dlq.batch.schema().field(2).name(),
        "source_write_timestamp_ms"
    );
    let timestamps = dlq
        .batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("DLQ source timestamp must be Int64");
    assert_eq!(timestamps.value(0), 1_234);
    Ok(())
}

#[test]
fn dlq_does_not_invent_a_missing_source_timestamp() -> anyhow::Result<()> {
    let parser = parser_for(vec![crate::parsers::json_parser::ColumnMapping::new(
        "$.id".into(),
        "id".into(),
        "Int64".into(),
        false,
    )])?;
    let (_main, dlq) = parser.parse_into(
        vec![Message::new(Bytes::from_static(b"invalid"))],
        &mut ParserWorkspace::new(),
    )?;
    let dlq = dlq.expect("invalid JSON must reach DLQ");
    let timestamps = dlq
        .batch
        .column(2)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("DLQ source timestamp must be Int64");
    assert!(timestamps.is_null(0));
    Ok(())
}

#[test]
fn dlq_preserves_non_utf8_payload_as_base64_and_releases_scratch() -> anyhow::Result<()> {
    use crate::parsers::json_parser::ColumnMapping;

    let parser = parser_for(vec![ColumnMapping::new(
        "$.id".into(),
        "id".into(),
        "Int64".into(),
        false,
    )])?;
    let mut workspace = ParserWorkspace::new();
    workspace
        .json_buf
        .reserve(ParserWorkspace::MAX_RETAINED_SCRATCH_BYTES + 1);
    let (_main, dlq) = parser.parse_into(
        vec![Message::new(Bytes::from_static(&[0xff, 0x00]))],
        &mut workspace,
    )?;
    let dlq = dlq.expect("invalid payload must reach DLQ");
    let raw = string_col(&dlq.batch, 0)?.value(0);
    anyhow::ensure!(base64::engine::general_purpose::STANDARD.decode(raw)? == [0xff, 0x00]);
    anyhow::ensure!(dlq.batch.schema().field(0).name() == "raw_base64");
    anyhow::ensure!(workspace.dlq_records.is_empty());
    anyhow::ensure!(workspace.json_buf.capacity() <= ParserWorkspace::MAX_RETAINED_SCRATCH_BYTES);
    Ok(())
}

/// Verifies the core invariant end-to-end: simd-json returns `&str`
/// values whose bytes exactly match `json_buf[start..end]`.
///
/// If this test fails, the safety comment on `str_val` is WRONG and
/// the unsafe block is producing garbage (or UB).
#[test]
fn str_val_matches_simd_json_output() -> anyhow::Result<()> {
    // "Moscow" and "🚀" as explicit UTF-8 byte sequences
    let json = b"{\"name\":\"Alice\",\"city\":\"Moscow\",\"flag\":\"\xF0\x9F\x9A\x80\"}";

    let kinds = vec![
        ColumnKind::Utf8, // name
        ColumnKind::Utf8, // city
        ColumnKind::Utf8, // flag
    ];

    let idx = ColumnIndex::Small(vec![
        ("name".into(), 0),
        ("city".into(), 1),
        ("flag".into(), 2),
    ]);

    let info = RootFieldInfo {
        index: idx,
        required: vec![true, true, true],
        required_total: 3,
        reject_unknown: false,
    };

    let mut scratch = vec![TypedScratch::Empty; kinds.len()];
    let mut seen = vec![false; kinds.len()];
    let mut buf = Vec::new();

    let ok = parse_root_fields_typed(json, &mut buf, &info, &mut scratch, &mut seen, &kinds)?;
    anyhow::ensure!(ok, "all fields should be found");

    // buf has been modified by simd-json in-situ parsing.
    // Now verify: json_buf[start..end] is valid UTF-8 AND matches the expected string.
    let expected = ["Alice", "Moscow", "\u{1F680}"];
    for (i, exp) in expected.iter().enumerate() {
        let TypedScratch::Str(range) = scratch[i] else {
            anyhow::bail!("Column {i}: expected Str, got {:?}", scratch[i]);
        };
        let reconstructed = str_val(&buf, range)?;
        let (start, end) = range.byte_range();
        anyhow::ensure!(
            reconstructed == *exp,
            "Column {i}: str_val({start}..{end}) = {reconstructed:?}, expected {exp:?}",
        );
    }
    Ok(())
}

/// Verifies that `str_val` correctly handles strings with escape sequences
/// (simd-json unescapes them in-situ, so the byte range should contain
/// the unescaped version).
#[test]
fn str_val_with_escapes() -> anyhow::Result<()> {
    // JSON with escape sequences that simd-json will process in-situ
    let json = br#"{"text":"Line1\nLine2\tTabbed"}"#;

    let kinds = vec![ColumnKind::Utf8];
    let idx = ColumnIndex::Small(vec![("text".into(), 0)]);
    let info = RootFieldInfo {
        index: idx,
        required: vec![true],
        required_total: 1,
        reject_unknown: false,
    };

    let mut scratch = vec![TypedScratch::Empty; 1];
    let mut seen = vec![false; 1];
    let mut buf = Vec::new();

    let ok = parse_root_fields_typed(json, &mut buf, &info, &mut scratch, &mut seen, &kinds)?;
    anyhow::ensure!(ok, "parse should succeed");

    let TypedScratch::Str(range) = scratch[0] else {
        anyhow::bail!("expected Str, got {:?}", scratch[0]);
    };
    let s = str_val(&buf, range)?;
    // After unescaping: \n -> newline, \t -> tab
    anyhow::ensure!(
        s.contains('\n'),
        "should contain unescaped newline, got {s:?}"
    );
    anyhow::ensure!(s.contains('\t'), "should contain unescaped tab, got {s:?}");
    anyhow::ensure!(!s.contains('\\'), "should not contain backslash, got {s:?}");
    Ok(())
}

#[test]
fn validated_string_rejects_a_different_buffer_with_identical_bytes() -> anyhow::Result<()> {
    let input = br#"{"value":"same bytes"}"#;
    let info = RootFieldInfo {
        index: ColumnIndex::Small(vec![("value".to_owned(), 0)]),
        required: vec![true],
        required_total: 1,
        reject_unknown: false,
    };
    let mut owner = Vec::new();
    let mut scratch = vec![TypedScratch::Empty];
    let mut seen = vec![false];
    anyhow::ensure!(parse_root_fields_typed(
        input,
        &mut owner,
        &info,
        &mut scratch,
        &mut seen,
        &[ColumnKind::Utf8],
    )?);
    let TypedScratch::Str(validated) = scratch[0] else {
        anyhow::bail!("string was not extracted")
    };
    let impostor = owner.clone();
    let error = str_val(&impostor, validated)
        .expect_err("validated string unexpectedly accepted a different allocation");
    assert!(error.to_string().contains("different source buffer"));
    assert_eq!(str_val(&owner, validated)?, "same bytes");
    Ok(())
}

/// Verifies that `json_framing: json_lines` correctly splits multi-line
/// messages and parses each line as a separate JSON row.
#[test]
fn newline_json_framing() -> anyhow::Result<()> {
    use crate::parsers::json_parser::{ColumnMapping, JsonFramingMode};

    let config = JsonParserConfig {
        columns: vec![
            ColumnMapping {
                jsonpath: "$.id".into(),
                column_name: "id".into(),
                arrow_type: "Utf8".into(),
                decimal_precision: None,
                decimal_scale: None,
                nullable: false,
                json_data_type: JsonDataType::String,
                time_conversion: None,
                low_cardinality: false,
                max_length: None,
            },
            ColumnMapping {
                jsonpath: "$.val".into(),
                column_name: "val".into(),
                arrow_type: "Int64".into(),
                decimal_precision: None,
                decimal_scale: None,
                nullable: true,
                json_data_type: JsonDataType::Number,
                time_conversion: None,
                low_cardinality: false,
                max_length: None,
            },
        ],
        json_framing: JsonFramingMode::JsonLines,
        conversion_error: ConversionErrorPolicy::Dlq,
        unknown_fields: UnknownFieldPolicy::Fail,
        keys: Vec::new(),
    };

    let parser = JsonParser::new(
        &config,
        &crate::parsers::SystemColumnsConfig::default(),
        "test".into(),
    )?;
    let mut ws = ParserWorkspace::new();

    // 3 JSONs separated by \n, one empty line
    let payload = b"{\"id\":\"a\",\"val\":1}\n{\"id\":\"b\",\"val\":2}\n\n{\"id\":\"c\"}";
    let msgs = vec![Message::new(Bytes::copy_from_slice(payload))];

    let (good, dlq) = parser.parse_into(msgs, &mut ws)?;

    anyhow::ensure!(
        good.batch.num_rows() == 3,
        "3 valid JSON lines \u{2192} 3 rows"
    );
    anyhow::ensure!(dlq.is_none(), "all 3 lines are valid JSON, no DLQ");

    // Check column values
    let id_col = string_col(&good.batch, 0)?;
    let val_col = int64_col(&good.batch, 1)?;
    anyhow::ensure!(id_col.value(0) == "a");
    anyhow::ensure!(id_col.value(1) == "b");
    anyhow::ensure!(id_col.value(2) == "c");
    anyhow::ensure!(val_col.value(0) == 1);
    anyhow::ensure!(val_col.value(1) == 2);
    anyhow::ensure!(good.batch.column(1).is_null(2));
    Ok(())
}

#[test]
fn json_array_framing_emits_one_row_per_element() -> anyhow::Result<()> {
    let config = parser_config(
        vec![mapping("$.id", "id", "Int64", false)],
        JsonFramingMode::JsonArray,
    );
    let (good, dlq) = parse_with_config(
        &config,
        &crate::parsers::SystemColumnsConfig::default(),
        br#"[{"id":1},{"id":2},{"id":3}]"#,
    )?;
    assert_eq!(good.batch.num_rows(), 3);
    assert_eq!(int64_col(&good.batch, 0)?.values(), &[1, 2, 3]);
    assert!(dlq.is_none());
    Ok(())
}

#[test]
fn json_array_framing_honors_parse_error_policy() -> anyhow::Result<()> {
    let mut config = parser_config(
        vec![mapping("$.id", "id", "Int64", false)],
        JsonFramingMode::JsonArray,
    );

    config.conversion_error = ConversionErrorPolicy::Dlq;
    let (main, dlq) = parse_with_config(
        &config,
        &crate::parsers::SystemColumnsConfig::default(),
        b"not-an-array",
    )?;
    assert_eq!(main.batch.num_rows(), 0);
    assert_eq!(
        dlq.expect("invalid array must reach the DLQ")
            .batch
            .num_rows(),
        1
    );

    config.conversion_error = ConversionErrorPolicy::Drop;
    let (main, dlq) = parse_with_config(
        &config,
        &crate::parsers::SystemColumnsConfig::default(),
        b"not-an-array",
    )?;
    assert_eq!(main.batch.num_rows(), 0);
    assert!(dlq.is_none());

    config.conversion_error = ConversionErrorPolicy::Fail;
    let error = parse_with_config(
        &config,
        &crate::parsers::SystemColumnsConfig::default(),
        b"not-an-array",
    )
    .expect_err("invalid array must fail the delivery");
    assert!(error.to_string().contains("invalid JSON array"));
    Ok(())
}

#[test]
fn materializes_system_columns_on_main_and_dlq() -> anyhow::Result<()> {
    use crate::parsers::json_parser::{ColumnMapping, JsonFramingMode};
    use crate::parsers::SystemColumnsConfig;
    use transferia_core::data::message::MessageMeta;
    use transferia_core::data::system_columns::SystemColumnKind;

    let config = JsonParserConfig {
        columns: vec![ColumnMapping {
            jsonpath: "$.id".into(),
            column_name: "id".into(),
            arrow_type: "Utf8".into(),
            decimal_precision: None,
            decimal_scale: None,
            nullable: false,
            json_data_type: JsonDataType::String,
            time_conversion: None,
            low_cardinality: false,
            max_length: None,
        }],
        json_framing: JsonFramingMode::JsonLines,
        conversion_error: ConversionErrorPolicy::Dlq,
        unknown_fields: UnknownFieldPolicy::Fail,
        keys: Vec::new(),
    };
    let system = SystemColumnsConfig {
        topic: Some("_system_topic".into()),
        partition: Some("_system_partition".into()),
        offset: Some("_system_offset".into()),
        message_index: Some("_system_message_index".into()),
        write_timestamp_ms: Some("_system_write_timestamp_ms".into()),
    };
    let parser = JsonParser::new(&config, &system, "test".into())?;
    let message = Message {
        value: Bytes::from_static(b"{\"id\":\"ok\"}\nnot-json"),
        key: None,
        headers: Arc::from([]),
        meta: MessageMeta {
            topic: Some(Arc::from("topic-a")),
            partition: Some(7),
            offset: Some(42),
            write_timestamp_ms: Some(1_234),
        },
    };
    let (good, dlq) = parser.parse_into(vec![message], &mut ParserWorkspace::new())?;
    let offset = good.system_columns.get(SystemColumnKind::Offset).unwrap();
    anyhow::ensure!(int64_col(&good.batch, offset.index)?.value(0) == 42);
    let dlq = dlq.ok_or_else(|| anyhow::anyhow!("invalid row must reach DLQ"))?;
    let index = dlq
        .system_columns
        .get(SystemColumnKind::MessageIndex)
        .unwrap();
    let values = dlq
        .batch
        .column(index.index)
        .as_any()
        .downcast_ref::<arrow::array::UInt64Array>()
        .ok_or_else(|| anyhow::anyhow!("message index has wrong type"))?;
    anyhow::ensure!(values.value(0) == 1);
    Ok(())
}

#[test]
fn missing_enabled_system_column_metadata_is_a_regular_error() -> anyhow::Result<()> {
    let config = parser_config(
        vec![mapping("$.id", "id", "Utf8", false)],
        JsonFramingMode::SingleDocument,
    );
    let system = crate::parsers::SystemColumnsConfig {
        topic: Some("_system_topic".into()),
        ..Default::default()
    };
    let parser = JsonParser::new(&config, &system, "test".into())?;

    let error = parser
        .parse_into(
            vec![Message::new(Bytes::from_static(b"{\"id\":\"value\"}"))],
            &mut ParserWorkspace::new(),
        )
        .expect_err("missing source metadata must not reach the append hot path");

    assert_eq!(
        error.to_string(),
        "source message is missing metadata required for system column '_system_topic'"
    );
    Ok(())
}

#[test]
fn null_in_non_nullable_partition_candidate_goes_to_dlq() -> anyhow::Result<()> {
    use crate::parsers::json_parser::{ColumnMapping, JsonFramingMode};

    let config = JsonParserConfig {
        columns: vec![ColumnMapping {
            jsonpath: "$.tenant".into(),
            column_name: "tenant".into(),
            arrow_type: "Utf8".into(),
            decimal_precision: None,
            decimal_scale: None,
            nullable: false,
            json_data_type: JsonDataType::String,
            time_conversion: None,
            low_cardinality: false,
            max_length: None,
        }],
        json_framing: JsonFramingMode::SingleDocument,
        conversion_error: ConversionErrorPolicy::Dlq,
        unknown_fields: UnknownFieldPolicy::Fail,
        keys: Vec::new(),
    };
    let parser = JsonParser::new(
        &config,
        &crate::parsers::SystemColumnsConfig::default(),
        "test".into(),
    )?;
    let (main, dlq) = parser.parse_into(
        vec![Message::new(Bytes::from_static(b"{\"tenant\":null}"))],
        &mut ParserWorkspace::new(),
    )?;
    anyhow::ensure!(main.batch.num_rows() == 0);
    anyhow::ensure!(dlq.is_some_and(|batch| batch.batch.num_rows() == 1));
    Ok(())
}

#[test]
fn invalid_types_and_ranges_go_to_dlq_in_root_and_mixed_modes() -> anyhow::Result<()> {
    use crate::parsers::json_parser::{ColumnMapping, JsonFramingMode};

    let cases = [
        ("Int8", "300"),
        ("UInt8", "-1"),
        ("UInt16", "70000"),
        ("Boolean", "\"true\""),
        ("Utf8", "42"),
        ("Float32", "1e39"),
    ];

    for (arrow_type, value) in cases {
        for (jsonpath, payload) in [
            ("$.value", format!("{{\"value\":{value}}}")),
            (
                "$.nested.value",
                format!("{{\"nested\":{{\"value\":{value}}}}}"),
            ),
        ] {
            let config = JsonParserConfig {
                columns: vec![ColumnMapping {
                    jsonpath: jsonpath.into(),
                    column_name: "value".into(),
                    arrow_type: arrow_type.into(),
                    decimal_precision: None,
                    decimal_scale: None,
                    nullable: false,
                    json_data_type: match arrow_type {
                        "Boolean" => JsonDataType::Boolean,
                        "Utf8" => JsonDataType::String,
                        _ => JsonDataType::Number,
                    },
                    time_conversion: None,
                    low_cardinality: false,
                    max_length: None,
                }],
                json_framing: JsonFramingMode::SingleDocument,
                conversion_error: ConversionErrorPolicy::Dlq,
                unknown_fields: UnknownFieldPolicy::Fail,
                keys: Vec::new(),
            };
            let parser = JsonParser::new(
                &config,
                &crate::parsers::SystemColumnsConfig::default(),
                "test".into(),
            )?;
            let (main, dlq) = parser.parse_into(
                vec![Message::new(Bytes::from(payload))],
                &mut ParserWorkspace::new(),
            )?;
            anyhow::ensure!(
                main.batch.num_rows() == 0,
                "{arrow_type} accepted invalid value in {jsonpath}"
            );
            anyhow::ensure!(
                dlq.is_some_and(|batch| batch.batch.num_rows() == 1),
                "{arrow_type} invalid value did not reach DLQ in {jsonpath}"
            );
        }
    }
    Ok(())
}

#[test]
fn nullable_root_and_mixed_values_accept_null_but_not_wrong_type() -> anyhow::Result<()> {
    use crate::parsers::json_parser::{ColumnMapping, JsonFramingMode};

    for (jsonpath, null_payload, invalid_payload) in [
        (
            "$.value",
            b"{\"value\":null}".as_slice(),
            b"{\"value\":\"bad\"}".as_slice(),
        ),
        (
            "$.nested.value",
            b"{\"nested\":{\"value\":null}}".as_slice(),
            b"{\"nested\":{\"value\":\"bad\"}}".as_slice(),
        ),
    ] {
        let config = JsonParserConfig {
            columns: vec![ColumnMapping {
                jsonpath: jsonpath.into(),
                column_name: "value".into(),
                arrow_type: "Int32".into(),
                decimal_precision: None,
                decimal_scale: None,
                nullable: true,
                json_data_type: JsonDataType::Number,
                time_conversion: None,
                low_cardinality: false,
                max_length: None,
            }],
            json_framing: JsonFramingMode::SingleDocument,
            conversion_error: ConversionErrorPolicy::Dlq,
            unknown_fields: UnknownFieldPolicy::Fail,
            keys: Vec::new(),
        };
        let parser = JsonParser::new(
            &config,
            &crate::parsers::SystemColumnsConfig::default(),
            "test".into(),
        )?;
        let messages = vec![
            Message::new(Bytes::copy_from_slice(null_payload)),
            Message::new(Bytes::copy_from_slice(invalid_payload)),
        ];
        let (main, dlq) = parser.parse_into(messages, &mut ParserWorkspace::new())?;
        anyhow::ensure!(main.batch.num_rows() == 1, "{jsonpath}");
        anyhow::ensure!(main.batch.column(0).is_null(0), "{jsonpath}");
        anyhow::ensure!(
            dlq.is_some_and(|batch| batch.batch.num_rows() == 1),
            "{jsonpath}"
        );
    }
    Ok(())
}

#[test]
fn timestamp_timezone_is_preserved_in_record_batch() -> anyhow::Result<()> {
    use crate::parsers::json_parser::{ColumnMapping, JsonFramingMode};

    let config = JsonParserConfig {
        columns: vec![ColumnMapping {
            jsonpath: "$.ts".into(),
            column_name: "ts".into(),
            arrow_type: "Timestamp(Millisecond, UTC)".into(),
            decimal_precision: None,
            decimal_scale: None,
            nullable: false,
            json_data_type: JsonDataType::Number,
            time_conversion: Some(TimeConversion::Epoch {
                unit: EpochUnit::Milliseconds,
            }),
            low_cardinality: false,
            max_length: None,
        }],
        json_framing: JsonFramingMode::SingleDocument,
        conversion_error: ConversionErrorPolicy::Dlq,
        unknown_fields: UnknownFieldPolicy::Fail,
        keys: Vec::new(),
    };
    let parser = JsonParser::new(
        &config,
        &crate::parsers::SystemColumnsConfig::default(),
        "test".into(),
    )?;
    let (main, dlq) = parser.parse_into(
        vec![Message::new(Bytes::from_static(b"{\"ts\":123}"))],
        &mut ParserWorkspace::new(),
    )?;
    anyhow::ensure!(dlq.is_none());
    anyhow::ensure!(
        main.batch.schema().field(0).data_type()
            == &DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
    );
    anyhow::ensure!(main.batch.column(0).data_type() == main.batch.schema().field(0).data_type());
    Ok(())
}

fn string_col(batch: &RecordBatch, idx: usize) -> anyhow::Result<&arrow::array::StringArray> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .ok_or_else(|| anyhow::anyhow!("column {idx} is not StringArray"))
}

fn int64_col(batch: &RecordBatch, idx: usize) -> anyhow::Result<&arrow::array::Int64Array> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .ok_or_else(|| anyhow::anyhow!("column {idx} is not Int64Array"))
}

#[test]
fn materializes_arrow_json_and_exact_decimal() -> anyhow::Result<()> {
    use transferia_core::data::schema::{ARROW_JSON_EXTENSION_NAME, META_ARROW_EXTENSION_NAME};

    let config: JsonParserConfig = serde_yaml::from_str(
        "columns:\n  - jsonpath: $.document\n    column_name: document\n    json_data_type: json\n    arrow_type: Json\n    nullable: false\n  - jsonpath: $.price\n    column_name: price\n    json_data_type: decimal\n    arrow_type: Decimal128\n    decimal_precision: 12\n    decimal_scale: 4\n    nullable: false\nconversion_error: fail\nunknown_fields: { action: fail }\n",
    )?;
    let (main, dlq) = parse_with_config(
        &config,
        &crate::parsers::SystemColumnsConfig::default(),
        br#"{"document":{"items":[true,2],"name":"demo"},"price":"12345678.9012"}"#,
    )?;
    assert!(dlq.is_none());
    assert_eq!(
        string_col(&main.batch, 0)?.value(0),
        r#"{"items":[true,2],"name":"demo"}"#
    );
    assert_eq!(
        main.batch
            .schema()
            .field(0)
            .metadata()
            .get(META_ARROW_EXTENSION_NAME),
        Some(&ARROW_JSON_EXTENSION_NAME.to_owned())
    );
    let decimals = main
        .batch
        .column(1)
        .as_primitive::<arrow::datatypes::Decimal128Type>();
    assert_eq!(decimals.value(0), 123_456_789_012);
    assert_eq!(decimals.data_type(), &DataType::Decimal128(12, 4));
    Ok(())
}

#[test]
fn decimal_never_rounds_or_overflows_silently() -> anyhow::Result<()> {
    let config: JsonParserConfig = serde_yaml::from_str(
        "columns:\n  - jsonpath: $.value\n    column_name: value\n    json_data_type: decimal\n    arrow_type: Decimal128\n    decimal_precision: 5\n    decimal_scale: 2\n    nullable: false\nconversion_error: fail\nunknown_fields: { action: fail }\n",
    )?;
    for payload in [
        br#"{"value":"1.234"}"#.as_slice(),
        br#"{"value":"1234.56"}"#.as_slice(),
    ] {
        let error = parse_with_config(
            &config,
            &crate::parsers::SystemColumnsConfig::default(),
            payload,
        )
        .expect_err("decimal precision loss must fail the delivery");
        assert!(
            error.to_string().contains("cannot be represented exactly"),
            "{error:#}"
        );
    }
    Ok(())
}
