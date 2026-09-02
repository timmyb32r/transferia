use std::time::{Duration, Instant};

use super::{elapsed_millis, observe_external_request};

#[tokio::test]
async fn observer_preserves_success_and_failure_without_transforming_them() {
    assert_eq!(
        observe_external_request("test", "success", async { Ok::<_, u8>(42) }).await,
        Ok(42)
    );
    assert_eq!(
        observe_external_request("test", "failure", async { Err::<u8, _>(17) }).await,
        Err(17)
    );
}

#[test]
fn elapsed_time_is_reported_in_milliseconds() {
    let started = Instant::now() - Duration::from_millis(5);
    assert!(elapsed_millis(started) >= 5);
}
