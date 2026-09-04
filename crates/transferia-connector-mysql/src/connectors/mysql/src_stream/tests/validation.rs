use super::super::{validate_replication_prerequisites, MySqlReplicationPrerequisites};

#[test]
fn exact_supported_server_settings_are_accepted_case_insensitively() {
    validate_replication_prerequisites(&MySqlReplicationPrerequisites {
        log_bin: "on".to_owned(),
        gtid_mode: "on".to_owned(),
        enforce_gtid_consistency: "on".to_owned(),
        binlog_format: "row".to_owned(),
        binlog_checksum: "crc32".to_owned(),
        binlog_row_image: "full".to_owned(),
        binlog_row_metadata: "full".to_owned(),
        binlog_row_value_options: String::new(),
        binlog_transaction_compression: "off".to_owned(),
    })
    .unwrap();

    validate_replication_prerequisites(&MySqlReplicationPrerequisites {
        log_bin: "1".to_owned(),
        gtid_mode: "ON".to_owned(),
        enforce_gtid_consistency: "ON".to_owned(),
        binlog_format: "ROW".to_owned(),
        binlog_checksum: "CRC32".to_owned(),
        binlog_row_image: "FULL".to_owned(),
        binlog_row_metadata: "FULL".to_owned(),
        binlog_row_value_options: String::new(),
        binlog_transaction_compression: "0".to_owned(),
    })
    .unwrap();
}

#[test]
fn every_lossy_or_silently_dropped_server_mode_is_rejected() {
    let supported = MySqlReplicationPrerequisites {
        log_bin: "ON".to_owned(),
        gtid_mode: "ON".to_owned(),
        enforce_gtid_consistency: "ON".to_owned(),
        binlog_format: "ROW".to_owned(),
        binlog_checksum: "CRC32".to_owned(),
        binlog_row_image: "FULL".to_owned(),
        binlog_row_metadata: "FULL".to_owned(),
        binlog_row_value_options: String::new(),
        binlog_transaction_compression: "OFF".to_owned(),
    };
    for invalid in [
        MySqlReplicationPrerequisites {
            log_bin: "OFF".to_owned(),
            ..supported.clone()
        },
        MySqlReplicationPrerequisites {
            log_bin: "0".to_owned(),
            ..supported.clone()
        },
        MySqlReplicationPrerequisites {
            gtid_mode: "OFF_PERMISSIVE".to_owned(),
            ..supported.clone()
        },
        MySqlReplicationPrerequisites {
            enforce_gtid_consistency: "WARN".to_owned(),
            ..supported.clone()
        },
        MySqlReplicationPrerequisites {
            binlog_format: "STATEMENT".to_owned(),
            ..supported.clone()
        },
        MySqlReplicationPrerequisites {
            binlog_checksum: "NONE".to_owned(),
            ..supported.clone()
        },
        MySqlReplicationPrerequisites {
            binlog_row_image: "MINIMAL".to_owned(),
            ..supported.clone()
        },
        MySqlReplicationPrerequisites {
            binlog_row_metadata: "MINIMAL".to_owned(),
            ..supported.clone()
        },
        MySqlReplicationPrerequisites {
            binlog_row_value_options: "PARTIAL_JSON".to_owned(),
            ..supported.clone()
        },
        MySqlReplicationPrerequisites {
            binlog_transaction_compression: "ON".to_owned(),
            ..supported
        },
    ] {
        assert!(validate_replication_prerequisites(&invalid).is_err());
    }
}
