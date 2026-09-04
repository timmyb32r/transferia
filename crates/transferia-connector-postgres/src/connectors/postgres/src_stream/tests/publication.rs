use arrow::datatypes::DataType;
use transferia_core::data::schema::{DatasetSchema, SchemaColumn};

use super::{
    validate_publication_contract, PublicationActions, PublicationTable, PUBLICATION_CONTRACT_SQL,
};
use crate::connectors::postgres::source::{DiscoveredTable, TableConfig};

const PUBLICATION: &str = "transferia_publication";

fn discovered(schema: &str, name: &str, relation_oid: u32) -> DiscoveredTable {
    DiscoveredTable {
        config: TableConfig {
            schema: schema.to_owned(),
            name: name.to_owned(),
        },
        schema: DatasetSchema::new(vec![
            SchemaColumn::new("id".to_owned(), DataType::Int64, false)
                .with_constraints(true, false, None),
            SchemaColumn::new("value".to_owned(), DataType::Utf8, true),
        ]),
        type_oids: vec![20, 25],
        replica_identity_full: false,
        replica_identity: "d".to_owned(),
        relation_oid,
    }
}

const fn actions() -> PublicationActions {
    PublicationActions {
        insert: Some(true),
        update: Some(true),
        delete: Some(true),
        truncate: Some(false),
        via_partition_root: Some(false),
        has_attnames: true,
        has_rowfilter: true,
    }
}

fn published(schema: &str, name: &str, relation_oid: u32) -> PublicationTable {
    PublicationTable {
        schema: schema.to_owned(),
        name: name.to_owned(),
        current_oid: Some(relation_oid),
        published_oid: Some(relation_oid),
        publishes_all_columns: Some(true),
        has_no_row_filter: true,
    }
}

#[test]
fn exact_row_dml_publication_is_accepted() {
    let expected = [discovered("public", "events", 42)];
    let actual = [published("public", "events", 42)];
    validate_publication_contract(PUBLICATION, actions(), &expected, &actual).unwrap();
}

#[test]
fn legacy_catalog_without_filter_or_projection_columns_is_accepted() {
    let expected = [discovered("public", "events", 42)];
    let mut settings = actions();
    settings.truncate = None;
    settings.via_partition_root = None;
    settings.has_attnames = false;
    settings.has_rowfilter = false;
    let mut actual = published("public", "events", 42);
    actual.publishes_all_columns = None;
    validate_publication_contract(PUBLICATION, settings, &expected, &[actual]).unwrap();
}

#[test]
fn every_row_dml_action_is_required() {
    let expected = [discovered("public", "events", 42)];
    let actual = [published("public", "events", 42)];
    for missing in [0, 1, 2] {
        let mut settings = actions();
        match missing {
            0 => settings.insert = Some(false),
            1 => settings.update = Some(false),
            2 => settings.delete = Some(false),
            _ => panic!("test enumerates exactly the three row DML actions"),
        }
        let error = validate_publication_contract(PUBLICATION, settings, &expected, &actual)
            .unwrap_err()
            .to_string();
        assert!(error.contains("INSERT, UPDATE, and DELETE"), "{error}");
    }

    let mut unknown = actions();
    unknown.update = None;
    assert!(validate_publication_contract(PUBLICATION, unknown, &expected, &actual).is_err());
}

#[test]
fn truncate_and_partition_root_are_rejected() {
    let expected = [discovered("public", "events", 42)];
    let actual = [published("public", "events", 42)];

    let mut truncate = actions();
    truncate.truncate = Some(true);
    let error = validate_publication_contract(PUBLICATION, truncate, &expected, &actual)
        .unwrap_err()
        .to_string();
    assert!(error.contains("must not publish TRUNCATE"), "{error}");

    let mut root = actions();
    root.via_partition_root = Some(true);
    let error = validate_publication_contract(PUBLICATION, root, &expected, &actual)
        .unwrap_err()
        .to_string();
    assert!(error.contains("publish_via_partition_root"), "{error}");
}

#[test]
fn missing_repeated_and_replaced_relations_are_rejected() {
    let expected = [discovered("public", "events", 42)];
    assert!(validate_publication_contract(PUBLICATION, actions(), &expected, &[]).is_err());

    let duplicate = published("public", "events", 42);
    assert!(validate_publication_contract(
        PUBLICATION,
        actions(),
        &expected,
        &[duplicate.clone(), duplicate]
    )
    .is_err());

    let recreated = [published("public", "events", 43)];
    let error = validate_publication_contract(PUBLICATION, actions(), &expected, &recreated)
        .unwrap_err()
        .to_string();
    assert!(error.contains("replaced after discovery"), "{error}");

    let mut not_a_member = published("public", "events", 42);
    not_a_member.published_oid = None;
    assert!(
        validate_publication_contract(PUBLICATION, actions(), &expected, &[not_a_member]).is_err()
    );
}

#[test]
fn row_filters_and_column_projections_are_rejected() {
    let expected = [discovered("public", "events", 42)];

    let mut projected = published("public", "events", 42);
    projected.publishes_all_columns = Some(false);
    let error = validate_publication_contract(PUBLICATION, actions(), &expected, &[projected])
        .unwrap_err()
        .to_string();
    assert!(error.contains("column projections"), "{error}");

    let mut filtered = published("public", "events", 42);
    filtered.has_no_row_filter = false;
    let error = validate_publication_contract(PUBLICATION, actions(), &expected, &[filtered])
        .unwrap_err()
        .to_string();
    assert!(error.contains("row filters"), "{error}");
}

#[test]
fn partially_upgraded_publication_catalog_is_rejected() {
    let expected = [discovered("public", "events", 42)];
    let actual = [published("public", "events", 42)];
    let mut settings = actions();
    settings.has_rowfilter = false;
    let error = validate_publication_contract(PUBLICATION, settings, &expected, &actual)
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("unsupported pg_publication_tables shape"),
        "{error}"
    );
}

#[test]
fn catalog_query_is_parameterized_and_checks_exact_relation_identity() {
    assert!(PUBLICATION_CONTRACT_SQL
        .contains("ROWS FROM (pg_catalog.unnest($2::text[]), pg_catalog.unnest($3::text[]))"));
    assert!(PUBLICATION_CONTRACT_SQL.contains("current_table.oid"));
    assert!(PUBLICATION_CONTRACT_SQL.contains("published_table.oid"));
    assert!(
        PUBLICATION_CONTRACT_SQL.contains("pg_catalog.to_jsonb(publication_table) -> 'attnames'")
    );
    assert!(
        PUBLICATION_CONTRACT_SQL.contains("pg_catalog.to_jsonb(publication_table) ->> 'rowfilter'")
    );
    assert!(!PUBLICATION_CONTRACT_SQL.contains(PUBLICATION));
}
