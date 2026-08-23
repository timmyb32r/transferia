use super::*;

#[test]
fn operational_tuning_is_hidden_from_the_sink_form() {
    let schema = serde_json::to_value(schemars::schema_for!(S3SinkConfig))
        .expect("S3 sink schema must serialize");

    for field in ["partitioning", "rotation", "buffering", "upload", "retry"] {
        assert_eq!(
            schema
                .pointer(&format!("/properties/{field}/x-ui/widget"))
                .and_then(serde_json::Value::as_str),
            Some("hidden"),
            "{field} must not appear in the S3 sink form",
        );
    }
}

#[test]
fn rejects_zero_parallel_object_uploads() -> anyhow::Result<()> {
    let config: S3SinkConfig =
        serde_yaml::from_str("bucket: test\nupload: { max_in_flight_objects: 0 }\n")?;
    let error = config.validate().expect_err("zero concurrency must fail");
    assert!(error.to_string().contains("max_in_flight_objects"));
    Ok(())
}

#[test]
fn rejects_zero_operation_timeout() -> anyhow::Result<()> {
    let config: S3SinkConfig =
        serde_yaml::from_str("bucket: test\nupload: { operation_timeout: 0ms }\n")?;
    let error = config.validate().expect_err("zero timeout must fail");
    assert!(error.to_string().contains("operation_timeout"));
    Ok(())
}

#[test]
fn rejects_zero_rotation_intervals() -> anyhow::Result<()> {
    for field in ["record_time_interval", "wall_clock_interval"] {
        let yaml = format!("bucket: test\nrotation: {{ {field}: 0ms }}\n");
        let config: S3SinkConfig = serde_yaml::from_str(&yaml)?;
        let error = config
            .validate()
            .expect_err("a zero rotation interval must fail during startup");
        assert!(error.to_string().contains(field));
    }
    Ok(())
}

#[test]
fn rejects_unknown_object_layout_version() -> anyhow::Result<()> {
    let config: S3SinkConfig = serde_yaml::from_str("bucket: test\nobject_layout_version: 1\n")?;
    let error = config
        .validate()
        .expect_err("unknown layout must not silently change replay semantics");
    assert!(error.to_string().contains("object_layout_version"));
    Ok(())
}

#[test]
fn credentials_are_redacted_from_debug_output() -> anyhow::Result<()> {
    let config: S3SinkConfig = serde_yaml::from_str(
        "bucket: test\ncredentials: { access_key: visible-key, secret_key: secret-value }\n",
    )?;
    let debug = format!("{config:?}");
    assert!(!debug.contains("visible-key"));
    assert!(!debug.contains("secret-value"));
    assert!(debug.contains("[REDACTED]"));
    Ok(())
}

#[test]
fn validates_explicit_epoch_boundary() -> anyhow::Result<()> {
    let config: S3SinkConfig = serde_yaml::from_str(
        "bucket: test\nbuffering: { max_buffered_bytes: 64MiB, max_epoch_bytes: 65MiB }\n",
    )?;
    let error = config
        .validate()
        .expect_err("epoch boundary beyond the total buffer must fail");
    assert!(error.to_string().contains("max_epoch_bytes"));
    Ok(())
}

#[test]
fn rejects_zero_pending_upload_objects_and_retry_attempts() -> anyhow::Result<()> {
    let no_pending: S3SinkConfig =
        serde_yaml::from_str("bucket: test\nbuffering: { max_pending_upload_objects: 0 }\n")?;
    assert!(no_pending
        .validate()
        .expect_err("zero pending objects must fail")
        .to_string()
        .contains("max_pending_upload_objects"));

    let no_attempts: S3SinkConfig =
        serde_yaml::from_str("bucket: test\nretry: { max_attempts: 0 }\n")?;
    assert!(no_attempts
        .validate()
        .expect_err("zero retry attempts must fail")
        .to_string()
        .contains("max_attempts"));
    Ok(())
}

#[test]
fn default_epoch_boundary_is_independent_of_operational_buffering() -> anyhow::Result<()> {
    let defaults: S3SinkConfig = serde_yaml::from_str("bucket: test\n")?;
    assert_eq!(defaults.epoch_byte_limit(), 128 * MIB);

    let config: S3SinkConfig =
        serde_yaml::from_str("bucket: test\nbuffering: { max_buffered_bytes: 32MiB }\n")?;
    assert_eq!(config.epoch_byte_limit(), 128 * MIB);
    assert!(config.validate().is_err());

    let explicit: S3SinkConfig = serde_yaml::from_str(
        "bucket: test\nbuffering: { max_buffered_bytes: 32MiB, max_epoch_bytes: 7MiB }\n",
    )?;
    assert_eq!(explicit.epoch_byte_limit(), 7 * MIB);
    explicit.validate()?;
    Ok(())
}

#[test]
fn rejects_invalid_record_time_format_and_partition_path() -> anyhow::Result<()> {
    for path in ["%Q", "year=%Y//month=%m", "year=%Y/../month=%m"] {
        let yaml = format!(
            "bucket: test\npartitioning: {{ type: record_time, window: 1h, path: '{path}', timezone: UTC }}\n"
        );
        let config: S3SinkConfig = serde_yaml::from_str(&yaml)?;
        assert!(
            config.validate().is_err(),
            "unsafe record-time path {path:?} must fail during startup validation"
        );
    }
    Ok(())
}
