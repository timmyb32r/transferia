use arrow::datatypes::DataType;
use transferia_core::data::schema::{DatasetSchema, SchemaColumn};

use super::{
    relation_lock_sql, validate_relation_identity_contract, CurrentRelationIdentity,
    RELATION_IDENTITY_SQL,
};
use crate::connectors::postgres::source::{DiscoveredTable, TableConfig};
use crate::connectors::postgres::src_stream::publication::{
    is_replication_contract_violation, replication_contract_violation,
};

fn discovered(schema: &str, name: &str, relation_oid: u32) -> DiscoveredTable {
    DiscoveredTable {
        config: TableConfig {
            schema: schema.to_owned(),
            name: name.to_owned(),
        },
        schema: DatasetSchema::new(vec![SchemaColumn::new(
            "id".to_owned(),
            DataType::Int64,
            false,
        )
        .with_constraints(true, false, None)]),
        type_oids: vec![20],
        replica_identity_full: false,
        replica_identity: "d".to_owned(),
        relation_oid,
    }
}

fn current(schema: &str, name: &str, relation_oid: Option<u32>) -> CurrentRelationIdentity {
    CurrentRelationIdentity {
        schema: schema.to_owned(),
        name: name.to_owned(),
        relation_oid,
        replica_identity: relation_oid.map(|_| "d".to_owned()),
        column_names: relation_oid.map_or_else(Vec::new, |_| vec!["id".to_owned()]),
        type_oids: relation_oid.map_or_else(Vec::new, |_| vec![20]),
        nullable: relation_oid.map_or_else(Vec::new, |_| vec![false]),
        primary_key: relation_oid.map_or_else(Vec::new, |_| vec![true]),
    }
}

#[test]
fn exact_relation_identities_are_accepted() {
    let expected = [
        discovered("public", "accounts", 42),
        discovered("audit", "events", 84),
    ];
    let actual = [
        current("public", "accounts", Some(42)),
        current("audit", "events", Some(84)),
    ];
    validate_relation_identity_contract(&expected, &actual).unwrap();
}

#[test]
fn removed_and_recreated_relations_are_rejected() {
    let expected = [discovered("public", "accounts", 42)];

    let removed = [current("public", "accounts", None)];
    let error = validate_relation_identity_contract(&expected, &removed)
        .unwrap_err()
        .to_string();
    assert!(error.contains("removed or replaced"), "{error}");

    let recreated = [current("public", "accounts", Some(43))];
    let error = validate_relation_identity_contract(&expected, &recreated)
        .unwrap_err()
        .to_string();
    assert!(error.contains("expected relation OID 42"), "{error}");
}

#[test]
fn deterministic_relation_drift_is_a_fatal_replication_contract_violation() {
    let expected = [discovered("public", "accounts", 42)];
    let recreated = [current("public", "accounts", Some(43))];
    let error = validate_relation_identity_contract(&expected, &recreated).unwrap_err();
    let error = replication_contract_violation(error);
    assert!(is_replication_contract_violation(&error));
}

#[test]
fn missing_duplicate_and_unexpected_relation_rows_fail_closed() {
    let expected = [discovered("public", "accounts", 42)];
    assert!(validate_relation_identity_contract(&expected, &[]).is_err());

    let duplicate = current("public", "accounts", Some(42));
    assert!(
        validate_relation_identity_contract(&expected, &[duplicate.clone(), duplicate]).is_err()
    );

    let wrong_name = [current("public", "other", Some(42))];
    assert!(validate_relation_identity_contract(&expected, &wrong_name).is_err());
}

#[test]
fn replica_identity_and_every_authoritative_column_attribute_are_exact() {
    let expected = [discovered("public", "accounts", 42)];
    let exact = current("public", "accounts", Some(42));

    let mut replica_identity = exact.clone();
    replica_identity.replica_identity = Some("f".to_owned());
    let error = validate_relation_identity_contract(&expected, &[replica_identity])
        .unwrap_err()
        .to_string();
    assert!(error.contains("replica identity changed"), "{error}");

    let mut column_name = exact.clone();
    column_name.column_names[0] = "other".to_owned();
    let error = validate_relation_identity_contract(&expected, &[column_name])
        .unwrap_err()
        .to_string();
    assert!(error.contains("column 0 name changed"), "{error}");

    let mut type_oid = exact.clone();
    type_oid.type_oids[0] = 23;
    let error = validate_relation_identity_contract(&expected, &[type_oid])
        .unwrap_err()
        .to_string();
    assert!(error.contains("type OID changed"), "{error}");

    let mut nullability = exact.clone();
    nullability.nullable[0] = true;
    let error = validate_relation_identity_contract(&expected, &[nullability])
        .unwrap_err()
        .to_string();
    assert!(error.contains("nullability changed"), "{error}");

    let mut primary_key = exact.clone();
    primary_key.primary_key[0] = false;
    let error = validate_relation_identity_contract(&expected, &[primary_key])
        .unwrap_err()
        .to_string();
    assert!(error.contains("primary-key membership changed"), "{error}");

    let mut removed_column = exact;
    removed_column.column_names.clear();
    removed_column.type_oids.clear();
    removed_column.nullable.clear();
    removed_column.primary_key.clear();
    let error = validate_relation_identity_contract(&expected, &[removed_column])
        .unwrap_err()
        .to_string();
    assert!(error.contains("expected 1 columns"), "{error}");
}

#[test]
fn relation_identity_query_is_parameterized_and_reads_full_authoritative_schema() {
    assert!(RELATION_IDENTITY_SQL
        .contains("ROWS FROM (pg_catalog.unnest($1::text[]), pg_catalog.unnest($2::text[]))"));
    assert!(RELATION_IDENTITY_SQL.contains("current_table.oid"));
    assert!(RELATION_IDENTITY_SQL.contains("current_table.relreplident"));
    assert!(RELATION_IDENTITY_SQL.contains("attribute.atttypid"));
    assert!(RELATION_IDENTITY_SQL.contains("column_metadata.is_nullable"));
    assert!(RELATION_IDENTITY_SQL.contains("index_metadata.indisprimary"));
    assert!(RELATION_IDENTITY_SQL.contains("ORDER BY attribute.attnum"));
    assert!(RELATION_IDENTITY_SQL.contains("LEFT JOIN pg_catalog.pg_class"));
    assert!(!RELATION_IDENTITY_SQL.contains("accounts"));
}

#[test]
fn relation_lock_covers_every_authoritative_table_and_quotes_identifiers() {
    let tables = [
        discovered("public", "accounts", 42),
        discovered("odd\"schema", "odd\"table", 84),
    ];
    assert_eq!(
        relation_lock_sql(&tables).unwrap(),
        "LOCK TABLE \"public\".\"accounts\", \"odd\"\"schema\".\"odd\"\"table\" IN ACCESS SHARE MODE"
    );
    assert!(relation_lock_sql(&[]).is_err());
}
