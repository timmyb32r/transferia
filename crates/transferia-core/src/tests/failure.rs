use super::*;

#[test]
fn disposition_is_explicit_and_survives_context() {
    let failure = DataPlaneFailure::retryable(anyhow::anyhow!("network unavailable"))
        .context("source read failed");
    assert_eq!(failure.disposition(), FailureDisposition::Retryable);
    assert!(failure.to_string().contains("source read failed"));

    let failure = DataPlaneFailure::fatal(anyhow::anyhow!("schema mismatch"));
    assert_eq!(failure.disposition(), FailureDisposition::Fatal);
    assert!(!failure.is_retryable());
}
