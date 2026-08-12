use super::*;

#[test]
fn inline_token_is_trimmed_and_redacted() {
    let auth = PqV1AuthConfig {
        auth_type: "access_token".to_string(),
        token: Some("  secret  ".to_string()),
        token_file: None,
    };
    assert_eq!(load_access_token(&auth).unwrap(), "secret");
    let debug = format!("{auth:?}");
    assert!(!debug.contains("secret"));
    assert!(debug.contains("[REDACTED]"));
}
