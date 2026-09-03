use mysql_async::consts::ColumnType;
use transferia_core::data::schema::DatasetSchema;

use super::super::decoder::{MySqlBinlogColumnIdentity, MySqlTableIdentity};
use super::super::source::{
    validate_replication_column_plan, validate_selected_table_map, verify_binlog_heartbeat,
};
use super::super::config::heartbeat_period_nanoseconds;
use crate::connectors::mysql::src_batch::{
    ColumnPlan, DiscoveredTable, MySqlColumnKind, TableConfig,
};

#[test]
fn binlog_heartbeat_is_exact_checked_and_verified_before_stream_handoff() {
    assert_eq!(heartbeat_period_nanoseconds(10).unwrap(), 10_000_000);
    assert!(heartbeat_period_nanoseconds(u64::MAX).is_err());

    verify_binlog_heartbeat(10_000_000, Some((10_000_000, 10_000_000))).unwrap();
    for observed in [None, Some((0, 10_000_000)), Some((10_000_000, 0))] {
        assert!(verify_binlog_heartbeat(10_000_000, observed).is_err());
    }
}

#[test]
fn table_map_rejects_same_count_column_rename_and_type_change() {
    let table = table(vec![column("id", "int", Some(1))]);
    let mut identity = table_identity(vec![binlog_column("id", ColumnType::MYSQL_TYPE_LONG, Some(1))]);
    validate_selected_table_map(&table, &identity).unwrap();

    identity.column_identities[0].name = b"renamed".to_vec();
    assert!(validate_selected_table_map(&table, &identity).is_err());

    identity.column_identities[0].name = b"id".to_vec();
    identity.column_identities[0].column_type = ColumnType::MYSQL_TYPE_SHORT;
    assert!(validate_selected_table_map(&table, &identity).is_err());
}

#[test]
fn table_map_rejects_primary_key_order_change_with_the_same_columns() {
    let table = table(vec![
        column("tenant_id", "int", Some(1)),
        column("item_id", "int", Some(2)),
    ]);
    let identity = table_identity(vec![
        binlog_column("tenant_id", ColumnType::MYSQL_TYPE_LONG, Some(2)),
        binlog_column("item_id", ColumnType::MYSQL_TYPE_LONG, Some(1)),
    ]);
    assert!(validate_selected_table_map(&table, &identity).is_err());
}

#[test]
fn table_map_rejects_collation_change_without_a_column_count_change() {
    let table = table(vec![text_column("body", 255)]);
    let mut column = binlog_column("body", ColumnType::MYSQL_TYPE_VARCHAR, None);
    column.collation_id = Some(45);
    column.metadata = 1_020_u16.to_le_bytes().to_vec();
    let identity = table_identity(vec![column]);
    assert!(validate_selected_table_map(&table, &identity).is_err());
}

#[test]
fn replication_discovery_rejects_lossy_binlog_value_encodings_and_charsets() {
    for data_type in [
        "json",
        "timestamp(6)",
        "time(6)",
        "enum('a','b')",
        "set('a','b')",
        "year",
    ] {
        let mut unsupported = column("value", data_type, None);
        unsupported.kind = MySqlColumnKind::Utf8;
        assert!(validate_replication_column_plan(&unsupported).is_err());
    }

    let mut text = text_column("body", 255);
    text.character_set = Some("latin1".to_owned());
    assert!(validate_replication_column_plan(&text).is_err());
    text.character_set = Some("utf8mb4".to_owned());
    validate_replication_column_plan(&text).unwrap();
}

fn table(columns: Vec<ColumnPlan>) -> DiscoveredTable {
    DiscoveredTable {
        config: TableConfig {
            name: "items".to_owned(),
        },
        schema: DatasetSchema::default(),
        columns,
        engine: "InnoDB".to_owned(),
    }
}

fn column(name: &str, column_type: &str, primary_key_ordinal: Option<u64>) -> ColumnPlan {
    ColumnPlan {
        name: name.to_owned(),
        kind: MySqlColumnKind::Int32,
        nullable: false,
        primary_key: primary_key_ordinal.is_some(),
        max_length: None,
        expression: format!("`{name}`"),
        column_type: column_type.to_owned(),
        character_set: None,
        collation: None,
        collation_id: None,
        extra: String::new(),
        generation_expression: Some(String::new()),
        primary_key_ordinal,
        primary_key_prefix_length: None,
        primary_key_direction: primary_key_ordinal.map(|_| "A".to_owned()),
    }
}

fn text_column(name: &str, collation_id: u16) -> ColumnPlan {
    ColumnPlan {
        name: name.to_owned(),
        kind: MySqlColumnKind::Utf8,
        nullable: false,
        primary_key: false,
        max_length: Some(255),
        expression: format!("`{name}`"),
        column_type: "varchar(255)".to_owned(),
        character_set: Some("utf8mb4".to_owned()),
        collation: Some("utf8mb4_0900_ai_ci".to_owned()),
        collation_id: Some(collation_id),
        extra: String::new(),
        generation_expression: Some(String::new()),
        primary_key_ordinal: None,
        primary_key_prefix_length: None,
        primary_key_direction: None,
    }
}

fn table_identity(column_identities: Vec<MySqlBinlogColumnIdentity>) -> MySqlTableIdentity {
    MySqlTableIdentity {
        table_id: 7,
        database: b"inventory".to_vec(),
        table: b"items".to_vec(),
        columns: column_identities.len() as u64,
        column_identities,
    }
}

fn binlog_column(
    name: &str,
    column_type: ColumnType,
    primary_key_ordinal: Option<u64>,
) -> MySqlBinlogColumnIdentity {
    MySqlBinlogColumnIdentity {
        name: name.as_bytes().to_vec(),
        column_type,
        metadata: Vec::new(),
        nullable: false,
        unsigned: column_type.is_numeric_type().then_some(false),
        collation_id: None,
        primary_key_ordinal,
        primary_key_prefix_length: None,
    }
}
