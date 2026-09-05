use super::super::config::{replication_slot, PostgresReplicationConfig};
use super::*;

#[test]
fn plugin_detection_does_not_hide_permissions_or_missing_wal() {
    assert!(plugin_is_missing(
        &SqlState::UNDEFINED_FILE,
        "could not access file \"wal2json\": No such file or directory",
        "wal2json"
    ));
    assert!(!plugin_is_missing(
        &SqlState::UNDEFINED_FILE,
        "could not open file pg_wal/0001",
        "wal2json"
    ));
    assert!(!plugin_is_missing(
        &SqlState::INSUFFICIENT_PRIVILEGE,
        "permission denied",
        "pgoutput"
    ));
    assert!(!plugin_is_missing(
        &SqlState::UNDEFINED_FILE,
        "could not access file \"wal2json\": Permission denied",
        "wal2json"
    ));
    assert!(!plugin_is_missing(
        &SqlState::UNDEFINED_FILE,
        "could not access file \"pgoutput\": No such file or directory",
        "wal2json"
    ));
}

#[test]
fn automatic_selection_checks_every_availability_combination() {
    assert_eq!(select_auto_plugin(true, true, None).unwrap(), "pgoutput");
    assert_eq!(select_auto_plugin(true, false, None).unwrap(), "pgoutput");
    assert_eq!(select_auto_plugin(false, true, None).unwrap(), "wal2json");
    assert!(select_auto_plugin(false, false, None).is_err());
}

#[test]
fn resuming_auto_never_changes_the_existing_slots_plugin() {
    assert_eq!(
        select_auto_plugin(true, true, Some("wal2json")).unwrap(),
        "wal2json"
    );
    assert_eq!(
        select_auto_plugin(true, true, Some("pgoutput")).unwrap(),
        "pgoutput"
    );
    assert!(select_auto_plugin(false, true, Some("pgoutput")).is_err());
    assert!(select_auto_plugin(true, false, Some("wal2json")).is_err());
    assert!(select_auto_plugin(true, true, Some("unknown")).is_err());
}

#[test]
fn plugin_is_advanced_defaults_to_auto_and_legacy_settings_are_rejected() {
    let config: PostgresReplicationConfig = serde_json::from_str("{}").unwrap();
    assert!(matches!(config.plugin, ReplicationPlugin::Auto));
    config.validate().unwrap();
    for invalid in [
        r#"{"slot":"dtt123"}"#,
        r#"{"decoder":{"type":"wal2_json"}}"#,
        r#"{"plugin":{"type":"wal2_json"}}"#,
    ] {
        assert!(serde_json::from_str::<PostgresReplicationConfig>(invalid).is_err());
    }
    let schema = serde_json::to_value(schemars::schema_for!(PostgresReplicationConfig)).unwrap();
    let properties = &schema["properties"];
    assert!(properties.get("slot").is_none());
    assert!(properties.get("decoder").is_none());
    assert_eq!(properties["plugin"]["title"], "Plugin");
    assert_eq!(
        properties["plugin"]["default"],
        serde_json::json!({"type": "auto"})
    );
    // The source's inline replication object owns the common Advanced section.
    assert!(properties["plugin"]["x-ui"]["section"].is_null());
    let branches = schema["$defs"]["ReplicationPlugin"]["oneOf"]
        .as_array()
        .unwrap();
    let tags = branches
        .iter()
        .map(|branch| branch["properties"]["type"]["const"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(tags, ["auto", "pgoutput", "wal2json"]);
}

#[test]
fn slot_name_is_the_exact_generated_transfer_id_without_normalization() {
    for id in ["dttabc123", "dtt000xyz"] {
        assert_eq!(replication_slot(id).unwrap(), id);
    }
    for id in ["", "dtt-with-dashes", "DTTabc", "dtt/abc", "дтт123"] {
        assert!(replication_slot(id).is_err());
    }
    assert!(replication_slot(&"a".repeat(63)).is_ok());
    assert!(replication_slot(&"a".repeat(64)).is_err());
}
