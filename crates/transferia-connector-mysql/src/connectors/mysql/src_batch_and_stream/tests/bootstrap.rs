#![allow(
    clippy::expect_used,
    clippy::panic,
    clippy::unwrap_used,
    reason = "bootstrap contract tests intentionally fail fast"
)]

use std::time::Duration;

use super::*;
use crate::connectors::mysql::common::MySqlConnectionConfig;
use crate::connectors::mysql::src_batch::TableConfig;
use crate::connectors::mysql::src_batch_and_stream::{
    MySqlColumnGeneration, MySqlColumnVisibility,
};

const SERVER_UUID: &str = "24bc7856-9a41-11ee-b9d1-0242ac120002";

fn config() -> MySqlConnectionConfig {
    MySqlConnectionConfig {
        host: "127.0.0.1".to_owned(),
        port: 3306,
        database: "inventory".to_owned(),
        username: "replicator".to_owned(),
        password: "secret".to_owned(),
        trusted_plaintext: true,
        tls_ca_file: None,
    }
}

fn preflight() -> MySqlReplicationPreflight {
    MySqlReplicationPreflight {
        source: MySqlSourceIdentity {
            server_uuid: SERVER_UUID.to_owned(),
            database: "inventory".to_owned(),
        },
        server_version: "8.4.2".to_owned(),
        binary_log_status_query: "SHOW BINARY LOG STATUS",
    }
}

fn table() -> TableConfig {
    TableConfig {
        name: "accounts".to_owned(),
    }
}

fn authoritative_table() -> AuthoritativeTableIdentity {
    AuthoritativeTableIdentity {
        database: "inventory".to_owned(),
        table: "accounts".to_owned(),
        engine: "InnoDB".to_owned(),
        columns: vec![AuthoritativeColumnIdentity {
            name: "id".to_owned(),
            data_type: "bigint".to_owned(),
            column_type: "bigint unsigned".to_owned(),
            unsigned: true,
            zerofill: false,
            auto_increment: true,
            nullable: false,
            character_maximum_length: None,
            character_octet_length: None,
            numeric_precision: Some(20),
            numeric_scale: Some(0),
            datetime_precision: None,
            character_set: None,
            collation: None,
            collation_id: None,
            collation_padding: None,
            enum_set_values: None,
            srs_id: None,
            visibility: MySqlColumnVisibility::Visible,
            generation: MySqlColumnGeneration::None,
            extra: "auto_increment".to_owned(),
            generation_expression: Some(String::new()),
            primary_key_ordinal: Some(1),
            primary_key_prefix_length: None,
            primary_key_direction: Some("A".to_owned()),
        }],
    }
}

fn valid_preflight(version: &str) -> anyhow::Result<&'static str> {
    validate_replication_preflight(
        version,
        "ON",
        "ON",
        "ON",
        "ROW",
        "FULL",
        "FULL",
        "",
        "OFF",
        "CRC32",
        SERVER_UUID,
    )
}

#[test]
fn exact_execution_lock_name_is_human_readable_and_never_hashed() {
    let name = replication_lock_name(SERVER_UUID, u32::MAX).unwrap();
    assert_eq!(
        name,
        "transferia:mysql:24bc7856-9a41-11ee-b9d1-0242ac120002:4294967295"
    );
    assert_eq!(name.len(), MYSQL_LOCK_NAME_MAX_BYTES);
    assert!(replication_lock_name(SERVER_UUID, 0).is_err());
    assert!(replication_lock_name("not-a-uuid", 7).is_err());
}

#[test]
fn mysql_8_minor_version_selects_its_exact_status_statement() {
    assert_eq!(valid_preflight("8.0.36").unwrap(), "SHOW MASTER STATUS");
    assert_eq!(
        valid_preflight("8.4.2-commercial").unwrap(),
        "SHOW BINARY LOG STATUS"
    );
}

#[test]
fn mariadb_and_non_mysql_8_versions_are_rejected_before_coordination() {
    for version in ["10.11.9-MariaDB", "5.7.44", "9.0.1", "invalid"] {
        let message = valid_preflight(version).unwrap_err().to_string();
        assert!(
            message.contains("MariaDB")
                || message.contains("requires MySQL 8.x")
                || message.contains("invalid digit"),
            "unexpected diagnostic for {version:?}: {message}"
        );
    }
}

#[test]
fn every_lossless_binlog_prerequisite_fails_closed() {
    let cases = [
        (
            ["OFF", "ON", "ON", "ROW", "FULL", "FULL", "", "OFF", "CRC32"],
            "gtid_mode=ON",
        ),
        (
            ["ON", "OFF", "ON", "ROW", "FULL", "FULL", "", "OFF", "CRC32"],
            "enforce_gtid_consistency=ON",
        ),
        (
            ["ON", "ON", "OFF", "ROW", "FULL", "FULL", "", "OFF", "CRC32"],
            "@@GLOBAL.log_bin=ON",
        ),
        (
            [
                "ON",
                "ON",
                "ON",
                "STATEMENT",
                "FULL",
                "FULL",
                "",
                "OFF",
                "CRC32",
            ],
            "binlog_format=ROW",
        ),
        (
            [
                "ON", "ON", "ON", "ROW", "MINIMAL", "FULL", "", "OFF", "CRC32",
            ],
            "binlog_row_image=FULL",
        ),
        (
            [
                "ON", "ON", "ON", "ROW", "FULL", "MINIMAL", "", "OFF", "CRC32",
            ],
            "binlog_row_metadata=FULL",
        ),
        (
            [
                "ON",
                "ON",
                "ON",
                "ROW",
                "FULL",
                "FULL",
                "PARTIAL_JSON",
                "OFF",
                "CRC32",
            ],
            "binlog_row_value_options",
        ),
        (
            ["ON", "ON", "ON", "ROW", "FULL", "FULL", "", "ON", "CRC32"],
            "binlog_transaction_compression=OFF",
        ),
        (
            ["ON", "ON", "ON", "ROW", "FULL", "FULL", "", "OFF", "NONE"],
            "binlog_checksum=CRC32",
        ),
    ];
    for (settings, expected) in cases {
        let error = validate_replication_preflight(
            "8.4.2",
            settings[0],
            settings[1],
            settings[2],
            settings[3],
            settings[4],
            settings[5],
            settings[6],
            settings[7],
            settings[8],
            SERVER_UUID,
        )
        .unwrap_err();
        assert!(
            error.to_string().contains(expected),
            "expected {expected:?} in {error}"
        );
    }
}

#[test]
fn bootstrap_inputs_bind_database_tables_server_id_and_all_timeouts() {
    let timeout = Duration::from_secs(1);
    let max_row_bytes = crate::connectors::mysql::src_batch::DEFAULT_MYSQL_MAX_ROW_BYTES;
    validate_bootstrap_inputs(
        &config(),
        &[table()],
        7,
        &preflight(),
        max_row_bytes,
        timeout,
        timeout,
        timeout,
    )
    .unwrap();

    let mut wrong_database = preflight();
    wrong_database.source.database = "other".to_owned();
    assert!(validate_bootstrap_inputs(
        &config(),
        &[table()],
        7,
        &wrong_database,
        max_row_bytes,
        timeout,
        timeout,
        timeout,
    )
    .is_err());
    assert!(validate_bootstrap_inputs(
        &config(),
        &[],
        7,
        &preflight(),
        max_row_bytes,
        timeout,
        timeout,
        timeout,
    )
    .is_err());
    assert!(validate_bootstrap_inputs(
        &config(),
        &[table(), table()],
        7,
        &preflight(),
        max_row_bytes,
        timeout,
        timeout,
        timeout,
    )
    .is_err());
    for timeouts in [
        (Duration::ZERO, timeout, timeout),
        (timeout, Duration::ZERO, timeout),
        (timeout, timeout, Duration::ZERO),
    ] {
        assert!(validate_bootstrap_inputs(
            &config(),
            &[table()],
            7,
            &preflight(),
            max_row_bytes,
            timeouts.0,
            timeouts.1,
            timeouts.2,
        )
        .is_err());
    }
    for invalid in [1_023, 1_073_741_825] {
        assert!(validate_bootstrap_inputs(
            &config(),
            &[table()],
            7,
            &preflight(),
            invalid,
            timeout,
            timeout,
            timeout,
        )
        .is_err());
    }
}

#[test]
fn boundary_validation_preserves_exact_gtid_and_server_timestamp() {
    let boundary = MySqlBinlogBoundary {
        filename: "mysql-bin.000009".to_owned(),
        position: 4,
        gtid_executed: format!("{SERVER_UUID}:1-87"),
        source_timestamp_micros: 1_731_234_567_890_123,
    };
    super::super::phase::validate_boundary(&boundary).unwrap();

    let mut invalid = boundary;
    invalid.source_timestamp_micros = -1;
    assert!(super::super::phase::validate_boundary(&invalid).is_err());

    invalid.source_timestamp_micros = 0;
    invalid.gtid_executed = format!("{SERVER_UUID}:0");
    assert!(super::super::phase::validate_boundary(&invalid).is_err());
}

#[test]
fn locked_gtid_state_requires_the_fence_and_exact_canonical_text() {
    let canonical = format!("{SERVER_UUID}:1-7:10-12");
    let state =
        validate_locked_gtid_state("transferia:mysql:test", Some(1), &canonical, "").unwrap();
    assert_eq!(state.executed.0.len(), 1);
    assert_eq!(state.executed.0[0].to_mysql_text(), canonical);
    assert_eq!(state.purged, GtidSet::default());
    assert_eq!(
        validate_locked_gtid_state("transferia:mysql:test", Some(1), "", "").unwrap(),
        MySqlGtidState {
            executed: GtidSet::default(),
            purged: GtidSet::default(),
        }
    );

    for held in [None, Some(0), Some(2)] {
        assert!(
            validate_locked_gtid_state("transferia:mysql:test", held, &canonical, "",)
                .unwrap_err()
                .to_string()
                .contains("no longer owned")
        );
    }
    for invalid in [
        format!(" {SERVER_UUID}:1"),
        format!("{SERVER_UUID}:1-2:3"),
        format!("{SERVER_UUID}:0"),
        format!("{SERVER_UUID}:1,"),
    ] {
        assert!(
            validate_locked_gtid_state("transferia:mysql:test", Some(1), &invalid, "",)
                .unwrap_err()
                .to_string()
                .contains("invalid executed GTID set")
        );

        assert!(
            validate_locked_gtid_state("transferia:mysql:test", Some(1), &canonical, &invalid,)
                .unwrap_err()
                .to_string()
                .contains("invalid purged GTID set")
        );
    }
}

#[test]
fn stream_boundary_rejects_any_authoritative_identity_drift() {
    let expected = vec![authoritative_table()];
    validate_authoritative_table_selection("inventory", &[table()], &expected).unwrap();
    validate_authoritative_tables_unchanged(&expected, &expected).unwrap();

    let mut changed = expected.clone();
    changed[0].columns[0].extra.clear();
    assert!(validate_authoritative_tables_unchanged(&expected, &changed)
        .unwrap_err()
        .to_string()
        .contains("changed after discovery"));

    changed = expected.clone();
    changed[0].table = "ACCOUNTS".to_owned();
    assert!(
        validate_authoritative_table_selection("inventory", &[table()], &changed)
            .unwrap_err()
            .to_string()
            .contains("does not exactly match")
    );
    assert!(
        validate_authoritative_table_selection("inventory", &[table(), table()], &expected)
            .is_err()
    );
}
