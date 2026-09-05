use super::*;

fn state() -> State {
    State { version: 1, delivery_id: "dtt123".into(), replay_identity: "config".into(),
        start_offsets: HashMap::from([("/db/t/feed".into(), 0)]), streaming: false }
}

#[test]
fn interrupted_snapshot_requires_manual_recovery() {
    let state = state();
    let reopened: State = serde_json::from_slice(&serde_json::to_vec(&state).unwrap()).unwrap();
    let error = require_streaming(&reopened).unwrap_err().to_string();
    assert!(error.contains("clean destination"));
    assert!(error.contains("No destination data has been automatically deleted"));
}

#[test]
fn phase_transition_requires_snapshot_completion_and_preserves_positions() {
    let state = state();
    assert!(streaming_state(&state, false).is_err());
    let next = streaming_state(&state, true).unwrap();
    assert!(next.streaming);
    assert_eq!(next.start_offsets, state.start_offsets);
    assert!(!state.streaming);
}

#[test]
fn only_completed_snapshot_can_resume_and_offsets_are_exact() {
    let mut state = state();
    state.streaming = true;
    let reopened: State = serde_json::from_slice(&serde_json::to_vec(&state).unwrap()).unwrap();
    require_streaming(&reopened).unwrap();
    validate_state(&reopened, "dtt123", "config", &["/db/t/feed".into()]).unwrap();
    assert_eq!(reopened.start_offsets["/db/t/feed"], 0);
    assert!(validate_state(&reopened, "other", "config", &["/db/t/feed".into()]).is_err());
    assert!(validate_state(&reopened, "dtt123", "changed", &["/db/t/feed".into()]).is_err());
    assert!(validate_state(&reopened, "dtt123", "config", &["/db/other/feed".into()]).is_err());
    state.start_offsets.insert("/db/t/feed".into(), -1);
    assert!(validate_state(&state, "dtt123", "config", &["/db/t/feed".into()]).is_err());
}
