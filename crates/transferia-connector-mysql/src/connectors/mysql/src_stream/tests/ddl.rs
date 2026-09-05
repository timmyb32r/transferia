use super::*;
use transferia_registry::table_selection::{EmptyMatches, PatternMode, TableRule, TableSelection};

fn selection() -> CompiledSelection {
    TableSelection {
        rules: vec![TableRule { include: "prod.reports_*".into(), exclude: None, mode: PatternMode::Glob }],
        empty_matches: EmptyMatches::FailValidation,
    }.compile().unwrap()
}

#[test]
fn rename_diagnostics_cover_mysql_forms_and_multi_rename() {
    for query in [
        "RENAME TABLE staging.old TO prod.reports_new",
        "ALTER TABLE staging.old RENAME TO prod.reports_new",
        "RENAME TABLE unrelated TO ignored, staging.old TO prod.reports_new",
        "RENAME TABLE `staging`.`old` TO `prod`.`reports_new`",
    ] {
        let message = rename_error(query.as_bytes(), b"prod", &selection()).unwrap();
        for expected in ["staging.old", "prod.reports_new", "rule 1", "No rename progress"] {
            assert!(message.contains(expected), "{message}");
        }
    }
}

#[test]
fn unqualified_alter_rename_preserves_original_database() {
    let message = rename_error(b"ALTER TABLE prod.old RENAME TO reports_new", b"other", &selection()).unwrap();
    assert!(message.contains("prod.reports_new"));
}

#[test]
fn unsupported_or_unselected_ddl_is_not_authorized_by_diagnostics() {
    for query in ["CREATE TABLE prod.reports_new (id INT)", "RENAME TABLE a TO b", "/*! RENAME TABLE old TO prod.reports_new */"] {
        assert!(rename_error(query.as_bytes(), b"prod", &selection()).is_none());
    }
}

#[test]
fn empty_table_creation_preserves_qualified_identifiers() {
    for (query, namespace, name) in [
        ("CREATE TABLE reports_new (id BIGINT PRIMARY KEY)", "prod", "reports_new"),
        ("CREATE TABLE other.reports_new LIKE prod.template", "other", "reports_new"),
        ("CREATE TABLE `other.db`.`reports.new` (id INT)", "other.db", "reports.new"),
    ] {
        assert_eq!(created_table(query.as_bytes(), b"prod"), Some(TableIdentity {
            namespace: namespace.into(), name: name.into(),
        }));
    }
}

#[test]
fn ambiguous_or_populated_creation_is_never_admitted() {
    for query in [
        "CREATE TABLE IF NOT EXISTS reports_new (id INT)",
        "CREATE TEMPORARY TABLE reports_new (id INT)",
        "CREATE TABLE reports_new AS SELECT * FROM existing",
        "CREATE TABLE reports_new SELECT * FROM existing",
        "CREATE TABLE reports_new (id INT); DROP TABLE existing",
        "/*! CREATE TABLE reports_new (id INT) */",
        "CREATE TABLE",
    ] {
        assert!(created_table(query.as_bytes(), b"prod").is_none(), "{query}");
    }
    assert!(created_table(b"CREATE TABLE reports_new (id INT)", b"").is_none());
    assert!(created_table(b"\xff", b"prod").is_none());
}
