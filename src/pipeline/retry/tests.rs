use super::*;

#[test]
fn seed_and_jitter_are_deterministic() {
    let seed = stable_retry_seed(b"partition/7/table/events");

    assert_eq!(seed, stable_retry_seed(b"partition/7/table/events"));
    assert_ne!(seed, stable_retry_seed(b"partition/8/table/events"));
    assert_eq!(
        jittered_retry_delay(Duration::from_secs(10), 3, seed),
        jittered_retry_delay(Duration::from_secs(10), 3, seed)
    );
}

#[test]
fn jitter_stays_below_the_configured_delay_and_within_twenty_percent() {
    let base = Duration::from_secs(10);
    for seed in 0..32 {
        for attempt in 0..32 {
            let delay = jittered_retry_delay(base, attempt, seed);
            assert!(delay >= Duration::from_secs(8));
            assert!(delay <= base);
        }
    }
    assert_eq!(jittered_retry_delay(Duration::ZERO, 7, 11), Duration::ZERO);
}

#[test]
fn attempts_and_seeds_desynchronize_retry_series() {
    let base = Duration::from_secs(10);
    let first: Vec<_> = (0..8)
        .map(|attempt| jittered_retry_delay(base, attempt, stable_retry_seed(b"first")))
        .collect();
    let second: Vec<_> = (0..8)
        .map(|attempt| jittered_retry_delay(base, attempt, stable_retry_seed(b"second")))
        .collect();

    assert!(first.windows(2).any(|pair| pair[0] != pair[1]));
    assert_ne!(first, second);
}
