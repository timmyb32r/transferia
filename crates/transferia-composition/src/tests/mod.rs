use super::*;

#[test]
fn rejects_invalid_worker_assignment_before_partitioning() {
    let mut cli = Cli {
        config: Some("unused".into()),
        server: false,
        bind: "127.0.0.1:8080"
            .parse()
            .expect("loopback test address is valid"),
        listen_all: None,
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

#[test]
fn listen_all_selects_every_ipv4_interface() {
    let cli = Cli::try_parse_from(["transferia", "--server", "--listen-all", "18080"])
        .expect("listen-all command line must parse");

    assert_eq!(cli.listen_all, Some(18_080));
    assert_eq!(
        cli.listen_all
            .map(|port| std::net::SocketAddr::from(([0, 0, 0, 0], port))),
        Some("0.0.0.0:18080".parse().unwrap())
    );
}
