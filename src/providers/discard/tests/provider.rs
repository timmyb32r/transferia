use super::*;

#[test]
fn rejects_unknown_discard_sink_settings() -> anyhow::Result<()> {
    assert!(DiscardSinkProvider::from_config(serde_yaml::from_str("unexpected: true")?).is_err());
    assert!(DiscardSinkProvider::from_config(serde_yaml::from_str("{}")?).is_ok());
    Ok(())
}
