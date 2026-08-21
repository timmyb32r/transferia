use super::*;
use crate::connectors::s3::sink::object_key::ObjectKey;

#[test]
fn dynamic_empty_record_time_segment_is_rejected_without_panicking() {
    let error = record_time_path(0, "fraction/%.f/end", chrono_tz::UTC)
        .expect_err("whole-second timestamp must not create an empty path segment");
    assert!(error.to_string().contains("invalid partition path"));
}

#[test]
fn default_record_time_path_is_valid() -> anyhow::Result<()> {
    let path = record_time_path(0, "dt=%Y-%m-%d/hour=%H", chrono_tz::UTC)?;
    assert_eq!(path.as_ref(), "dt=1970-01-01/hour=00");
    Ok(())
}

#[test]
fn overlong_record_time_path_is_preserved_for_final_key_validation() {
    let literal = "x".repeat(1_100);
    let error = record_time_path(0, &format!("dt=%Y/value={literal}"), chrono_tz::UTC)
        .and_then(|path| ObjectKey::for_json_object("", "events", &path, "topic", 0, 0))
        .expect_err("an overlong record-time path must not be shortened");
    assert!(error.to_string().contains("1024-byte limit"));
}

#[test]
fn invalid_partition_value_is_rejected_instead_of_encoded() {
    let error = validate_path_component("partition column value", "tenant/a")
        .expect_err("slash must not be silently percent encoded");
    assert!(error.to_string().contains("partition column value"));
}
