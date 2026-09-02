use std::future::Future;
use std::time::Instant;

/// Observe one request made through an SDK whose transport cannot use
/// [`crate::outbound_http::OutboundHttpRequest`] directly.
///
/// Operation and system names must be static, credential-free identifiers.
/// Errors are deliberately not formatted because third-party SDK errors may
/// contain credentials or request payloads.
pub async fn observe_external_request<T, E>(
    system: &'static str,
    operation: &'static str,
    request: impl Future<Output = Result<T, E>>,
) -> Result<T, E> {
    let started = Instant::now();
    let result = request.await;
    tracing::info!(
        target: "transferia.external_request",
        external_system = system,
        operation,
        elapsed_ms = elapsed_millis(started),
        success = result.is_ok(),
        "external request completed"
    );
    result
}

#[must_use]
pub fn elapsed_millis(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
#[path = "external_request_tests.rs"]
mod tests;
