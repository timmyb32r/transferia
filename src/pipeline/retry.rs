use std::time::Duration;

const FNV_OFFSET_BASIS: u64 = 14_695_981_039_346_656_037;
const FNV_PRIME: u64 = 1_099_511_628_211;
const RETRY_MIX: u64 = 0x9e37_79b9_7f4a_7c15;
const MIN_JITTER_PERCENT: u64 = 80;
const JITTER_PERCENT_COUNT: u64 = 21;

/// Returns a stable, process-independent seed for retry jitter.
#[must_use]
pub fn stable_retry_seed(value: &[u8]) -> u64 {
    value.iter().fold(FNV_OFFSET_BASIS, |state, byte| {
        (state ^ u64::from(*byte)).wrapping_mul(FNV_PRIME)
    })
}

/// Applies deterministic 0–20% downward jitter without exceeding the configured delay cap.
#[must_use]
pub fn jittered_retry_delay(delay: Duration, attempt: u32, seed: u64) -> Duration {
    let mixed = seed
        .wrapping_add(u64::from(attempt).wrapping_mul(RETRY_MIX))
        .wrapping_add(RETRY_MIX);
    let mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    let mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    let mixed = mixed ^ (mixed >> 31);
    let jitter_percent = MIN_JITTER_PERCENT + mixed % JITTER_PERCENT_COUNT;
    let nanos = delay.as_nanos().saturating_mul(u128::from(jitter_percent)) / 100;
    if nanos >= Duration::MAX.as_nanos() {
        return Duration::MAX;
    }
    Duration::new(
        u64::try_from(nanos / 1_000_000_000).unwrap_or(u64::MAX),
        u32::try_from(nanos % 1_000_000_000).unwrap_or(999_999_999),
    )
}

#[cfg(test)]
mod tests;
