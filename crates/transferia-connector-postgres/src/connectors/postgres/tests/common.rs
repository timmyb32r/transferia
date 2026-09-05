use super::authentication_check_message;

#[test]
fn empty_password_authentication_failure_explains_how_to_retry() {
    let message = authentication_check_message(Some("28P01"), true).unwrap();
    assert!(message.contains("password field is empty"));
    assert!(message.contains("Enter the password"));
}

#[test]
fn authentication_diagnostics_do_not_misclassify_other_failures() {
    let message = authentication_check_message(Some("28P01"), false).unwrap();
    assert!(message.contains("Check the username and password"));
    assert!(!message.contains("empty"));
    assert!(authentication_check_message(Some("28000"), true)
        .unwrap()
        .contains("pg_hba.conf"));
    assert_eq!(authentication_check_message(Some("08001"), true), None);
    assert_eq!(authentication_check_message(None, true), None);
}
