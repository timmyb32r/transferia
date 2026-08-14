use super::*;

#[test]
fn object_key_layout_preserves_valid_components_verbatim() -> anyhow::Result<()> {
    let key = ObjectKey::for_json_object("streams", "events", "tenant=alpha", "topic-a", 3, 77)?;
    assert_eq!(
        key.as_str(),
        "streams/events/tenant=alpha/topic-a+3+77.json"
    );
    Ok(())
}

#[test]
fn rejects_static_layout_overhead_beyond_the_s3_utf8_byte_limit() {
    let table = "x".repeat(MAX_OBJECT_KEY_BYTES);
    let error = ObjectKey::for_json_object("", &table, "partition=0", "topic", 0, 0)
        .expect_err("an overlong key must fail before object-store I/O");
    assert!(error.to_string().contains("1024-byte limit"));
}

#[test]
fn accepts_the_exact_s3_object_key_byte_limit() -> anyhow::Result<()> {
    let suffix = "/partition=0/topic+0+0.json";
    let table = "x".repeat(MAX_OBJECT_KEY_BYTES - suffix.len());
    let key = ObjectKey::for_json_object("", &table, "partition=0", "topic", 0, 0)?;
    assert_eq!(key.as_str().len(), MAX_OBJECT_KEY_BYTES);
    Ok(())
}

#[test]
fn overlong_dynamic_topic_fails_instead_of_being_rewritten() {
    let topic = "a".repeat(MAX_OBJECT_KEY_BYTES);
    let error = ObjectKey::for_json_object("", "events", "partition=0", &topic, 0, 0)
        .expect_err("an overlong topic must not be hashed or truncated");
    assert!(error.to_string().contains("1024-byte limit"));
    assert!(!error.to_string().contains("sha256"));
}

#[test]
fn invalid_dynamic_component_fails_instead_of_being_percent_encoded() {
    let error = ObjectKey::for_json_object("", "events", "partition=0", "topic/a", 0, 0)
        .expect_err("a slash in a component must not be silently encoded");
    assert!(error.to_string().contains("source topic"));
}

#[test]
fn path_validation_is_part_of_final_key_construction() {
    let error = ObjectKey::for_json_object("", "events", "../escape", "topic", 0, 0)
        .expect_err("relative path segments must not reach object-store I/O");
    assert!(error.to_string().contains("invalid S3 object key"));
}
