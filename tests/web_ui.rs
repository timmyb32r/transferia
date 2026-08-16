use std::process::Command;

#[test]
fn web_ui_contract_tests_pass() {
    let status = Command::new("npm")
        .arg("test")
        .current_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/web"))
        .status()
        .expect("npm must be available because build.rs uses it to bundle the web UI");

    assert!(status.success(), "web UI tests failed with status {status}");
}
