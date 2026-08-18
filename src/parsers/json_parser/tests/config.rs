use super::*;

#[test]
fn parses_arrow_types() -> anyhow::Result<()> {
    anyhow::ensure!(parse_arrow_type("Utf8")? == DataType::Utf8);
    anyhow::ensure!(parse_arrow_type("Int64")? == DataType::Int64);
    anyhow::ensure!(parse_arrow_type("Float64")? == DataType::Float64);
    anyhow::ensure!(parse_arrow_type("Boolean")? == DataType::Boolean);
    anyhow::ensure!(
        parse_arrow_type("Timestamp(Millisecond)")?
            == DataType::Timestamp(TimeUnit::Millisecond, None)
    );
    anyhow::ensure!(
        parse_arrow_type("Timestamp(Microsecond, UTC)")?
            == DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    );
    Ok(())
}

#[test]
fn rejects_unknown_arrow_type() -> anyhow::Result<()> {
    let error = parse_arrow_type("Blob")
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected Blob to be rejected"))?;
    anyhow::ensure!(error.to_string().contains("unsupported arrow_type"));
    Ok(())
}

#[test]
fn rejects_malformed_timestamp_types() {
    for value in [
        "Timestamp(Millisecond",
        "Timestamp(Millisecond))",
        "Timestamp(Millisecond, UTC, extra)",
        "Timestamp(Millisecond,)",
        "Timestamp()",
    ] {
        let error = parse_arrow_type(value).expect_err("malformed Timestamp must fail");
        assert!(
            error.to_string().contains("Timestamp"),
            "unexpected error for {value}: {error:#}"
        );
    }
}

#[test]
fn produces_sink_neutral_schema() -> anyhow::Result<()> {
    let config: JsonParserConfig = serde_yaml::from_str(
        "columns:\n  - jsonpath: $.id\n    column_name: id\n    json_data_type: number\n    arrow_type: UInt64\n    nullable: false\nconversion_error: dlq\nunknown_fields: { action: fail }\n",
    )?;
    let schema = config.to_dataset_schema()?;
    anyhow::ensure!(schema.columns.len() == 1);
    anyhow::ensure!(schema.columns[0].name == "id");
    anyhow::ensure!(schema.columns[0].data_type == DataType::UInt64);
    anyhow::ensure!(!schema.columns[0].nullable);
    Ok(())
}

#[test]
fn decimal_and_json_types_have_explicit_arrow_contracts() -> anyhow::Result<()> {
    let config: JsonParserConfig = serde_yaml::from_str(
        "columns:\n  - jsonpath: $.price\n    column_name: price\n    json_data_type: decimal\n    arrow_type: Decimal128\n    decimal_precision: 12\n    decimal_scale: 4\n    nullable: false\n  - jsonpath: $.document\n    column_name: document\n    json_data_type: json\n    arrow_type: Json\n    nullable: false\nconversion_error: fail\nunknown_fields: { action: fail }\n",
    )?;
    let schema = config.to_dataset_schema()?;
    assert_eq!(schema.columns[0].data_type, DataType::Decimal128(12, 4));
    assert_eq!(schema.columns[1].data_type, DataType::Utf8);
    assert_eq!(
        schema.columns[1].arrow_extension_name,
        Some(ARROW_JSON_EXTENSION_NAME)
    );
    Ok(())
}

#[test]
fn decimal_requires_valid_explicit_precision_and_scale() {
    for (precision, scale) in [(None, Some(2)), (Some(0), Some(0)), (Some(4), Some(5))] {
        let mapping = ColumnMapping {
            jsonpath: "$.value".into(),
            column_name: "value".into(),
            json_data_type: JsonDataType::Decimal,
            arrow_type: "Decimal128".into(),
            decimal_precision: precision,
            decimal_scale: scale,
            nullable: false,
            time_conversion: None,
            low_cardinality: false,
            max_length: None,
        };
        assert!(mapping.to_schema_column(false).is_err());
    }
}

#[test]
fn defaults_unknown_fields_to_the_additional_properties_column() -> anyhow::Result<()> {
    let config: JsonParserConfig = serde_yaml::from_str(
        "columns:\n  - jsonpath: $.id\n    column_name: id\n    json_data_type: number\n    arrow_type: UInt64\n    nullable: false\nconversion_error: dlq\n",
    )?;
    assert_eq!(
        config.unknown_fields,
        UnknownFieldPolicy::SendToColumn {
            column_name: "additional_properties".to_owned(),
        }
    );

    let schema = serde_json::to_value(schemars::schema_for!(JsonParserConfig))?;
    assert_eq!(
        schema.pointer("/properties/unknown_fields/default/action"),
        Some(&serde_json::json!("send_to_column"))
    );
    assert_eq!(
        schema.pointer("/properties/unknown_fields/default/column_name"),
        Some(&serde_json::json!("additional_properties"))
    );
    Ok(())
}

#[test]
fn rejects_the_removed_rest_unknown_field_action() {
    let error = serde_yaml::from_str::<JsonParserConfig>(
        "columns: []\nconversion_error: fail\nunknown_fields: { action: rest, column_name: rest }\n",
    )
    .expect_err("the obsolete rest action must not be accepted");
    assert!(error.to_string().contains("rest"), "{error:#}");
}

#[test]
fn rejects_sink_specific_fields_in_parser_config() {
    let result =
        serde_yaml::from_str::<JsonParserConfig>("columns: []\nsink_specific_field: true\n");
    assert!(result.is_err());
}

#[test]
fn json_data_type_has_only_json_level_categories() {
    for removed in ["integer", "unsigned_integer"] {
        let yaml = format!(
            "columns:\n  - jsonpath: $.id\n    column_name: id\n    json_data_type: {removed}\n    arrow_type: Int64\n    nullable: false\nconversion_error: fail\nunknown_fields: {{ action: fail }}\n"
        );
        let error = serde_yaml::from_str::<JsonParserConfig>(&yaml)
            .expect_err("removed JSON type must be rejected");
        assert!(error.to_string().contains(removed), "{error:#}");
    }
}

#[test]
fn rejects_the_removed_chunk_splitter_name() {
    let error = serde_yaml::from_str::<JsonParserConfig>(
        "columns: []\nchunk_splitter: one-message-one-row\nconversion_error: fail\nunknown_fields: { action: fail }\n",
    )
    .expect_err("the obsolete field must not be accepted");
    assert!(error.to_string().contains("chunk_splitter"), "{error:#}");
}
