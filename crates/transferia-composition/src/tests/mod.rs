use super::*;

#[test]
fn rejects_invalid_worker_assignment_before_partitioning() {
    let mut cli = Cli {
        config: Some("unused".into()),
        server: false,
        bind: "127.0.0.1:8080"
            .parse()
            .expect("loopback test address is valid"),
        state_dir: ".transferia-server".into(),
        total_workers: 0,
        worker_index: 0,
        parent_control: None,
        parent_token: None,
        resolved_config: false,
        composition_fingerprint: None,
    };
    assert!(validate_worker_assignment(&cli).is_err());
    cli.total_workers = 2;
    cli.worker_index = 2;
    assert!(validate_worker_assignment(&cli).is_err());
    cli.worker_index = 1;
    assert!(validate_worker_assignment(&cli).is_ok());
    cli.resolved_config = true;
    assert!(validate_worker_assignment(&cli).is_err());
    cli.composition_fingerprint = Some("test-composition".to_owned());
    assert!(validate_worker_assignment(&cli).is_ok());
}
