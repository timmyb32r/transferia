use core::pin::Pin;
use core::task::{Context, Poll};

use anyhow::anyhow;
use http::Uri;
use hyper::client::conn::http2;
use tokio_util::sync::CancellationToken;

/// Bridges Hyper's HTTP/2 sender to the `tower::Service` expected by tonic.
pub struct H2Service {
    inner: http2::SendRequest<tonic::body::Body>,
}

impl tower::Service<http::Request<tonic::body::Body>> for H2Service {
    type Response = http::Response<hyper::body::Incoming>;
    type Error = hyper::Error;
    type Future =
        Pin<Box<dyn core::future::Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, request: http::Request<tonic::body::Body>) -> Self::Future {
        Box::pin(self.inner.send_request(request))
    }
}

/// Open plaintext HTTP/2 with prior knowledge, as required by YDB's native gRPC port.
pub async fn connect_http2_prior_knowledge(
    uri: &Uri,
    timeout: core::time::Duration,
    cancellation: &CancellationToken,
) -> anyhow::Result<H2Service> {
    let address = socket_address(uri);
    let operation = async {
        let stream = tokio::net::TcpStream::connect(&address)
            .await
            .map_err(|error| anyhow!("TCP connect to {address}: {error}"))?;
        stream.set_nodelay(true)?;
        let io = hyper_util::rt::TokioIo::new(stream);
        let mut builder = http2::Builder::new(hyper_util::rt::TokioExecutor::new());
        builder
            .timer(hyper_util::rt::TokioTimer::new())
            .keep_alive_interval(timeout)
            .keep_alive_timeout(timeout)
            .keep_alive_while_idle(true);
        builder
            .handshake(io)
            .await
            .map_err(|error| anyhow!("HTTP/2 handshake failed: {error}"))
    };
    let (send_request, connection) = tokio::select! {
        biased;
        () = cancellation.cancelled() => anyhow::bail!("HTTP/2 connection cancelled"),
        result = tokio::time::timeout(timeout, operation) => {
            result.map_err(|_| anyhow!("HTTP/2 connection timed out after {} ms", timeout.as_millis()))??
        }
    };
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            tracing::error!("HTTP/2 connection error: {error}");
        }
    });
    tracing::debug!(%address, "HTTP/2 prior-knowledge connection established");
    Ok(H2Service {
        inner: send_request,
    })
}

fn socket_address(uri: &Uri) -> String {
    crate::connectors::address::host_port(
        uri.host().unwrap_or("localhost"),
        uri.port_u16().unwrap_or(2135),
    )
}
