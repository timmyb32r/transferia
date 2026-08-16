use super::*;

#[test]
fn commit_marker_returns_its_exact_value_type() -> anyhow::Result<()> {
    let marker = CommitMarker::new(42_i64);

    assert_eq!(*marker.value::<i64>()?, 42);
    Ok(())
}

#[test]
fn commit_marker_type_mismatch_is_an_explicit_error() {
    let marker = CommitMarker::new(42_i64);

    let error = marker
        .value::<String>()
        .expect_err("a mismatched marker type must fail");
    assert_eq!(
        error.to_string(),
        "commit marker type mismatch: expected 'alloc::string::String', received 'i64'"
    );
}
