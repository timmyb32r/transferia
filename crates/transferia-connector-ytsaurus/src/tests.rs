use transferia_registry::ConnectionCheckStatus;

use super::{configured_source_paths, incomplete_entities_result, ytsaurus};

#[test]
fn empty_source_paths_cannot_be_reported_as_verified_entities() -> anyhow::Result<()> {
    let config = serde_yaml::from_str::<ytsaurus::YTsaurusSourceConfig>(
        "auth: { type: token, token: test }\nhost: localhost\nport: 8000\ntrusted_plaintext: true\ntables:\n  - path: ''\n",
    )?;

    assert!(configured_source_paths(&config).is_none());
    Ok(())
}

#[test]
fn incomplete_entity_result_is_explicitly_partial() {
    let result = incomplete_entities_result("entity access was not checked");

    assert!(matches!(
        result.status,
        ConnectionCheckStatus::NetworkReachable
    ));
    assert_eq!(result.message.as_deref(), Some("entity access was not checked"));
}
