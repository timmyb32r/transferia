// ---------------------------------------------------------------------------
// HTTP/2 prior-knowledge transport (Go-compatible)
// ---------------------------------------------------------------------------

use anyhow::anyhow;
use http::Uri;
use tokio_util::sync::CancellationToken;
use tonic::metadata::{AsciiMetadataValue, MetadataMap};
use tonic::Request;

use super::{
    status_failure_kind, surface_session_failure, tonic_failure, PqV1Client, SessionFailure,
    MAX_GRPC_MESSAGE_SIZE, YDB_STATUS_SUCCESS, YDB_STATUS_UNSPECIFIED,
};
use crate::delivery::execution::retry::stable_retry_seed;
use crate::delivery::execution::PipelineFailure;
pub use crate::providers::ydb_transport::{connect_http2_prior_knowledge, H2Service};
use crate::Ydb::status_ids::StatusCode;

/// YDB cluster database used for discovery/routing metadata (`x-ydb-database`).
/// Always `/Root` in our deployment — hardcoded rather than configured.
const YDB_DATABASE: &str = "/Root";

pub(super) async fn network_stage<T>(
    name: &str,
    timeout: core::time::Duration,
    cancellation: &CancellationToken,
    operation: impl core::future::Future<Output = anyhow::Result<T>>,
) -> anyhow::Result<T> {
    tokio::select! {
        biased;
        () = cancellation.cancelled() => anyhow::bail!("{name} cancelled"),
        result = tokio::time::timeout(timeout, operation) => {
            result.map_err(|_| anyhow!("{name} timed out after {} ms", timeout.as_millis()))?
        }
    }
}

/// Attach the YDB auth/routing headers that Logbroker expects on every call.
pub(super) fn auth_metadata_value(token: &str) -> anyhow::Result<AsciiMetadataValue> {
    anyhow::ensure!(!token.is_empty(), "PQv1 access token must not be empty");
    AsciiMetadataValue::try_from(token)
        .map_err(|_| anyhow!("PQv1 access token is not valid ASCII metadata"))
}

pub fn set_ydb_headers(md: &mut MetadataMap, token: &str) -> anyhow::Result<()> {
    md.insert("x-ydb-auth-ticket", auth_metadata_value(token)?);
    md.insert(
        "x-ydb-database",
        AsciiMetadataValue::from_static(YDB_DATABASE),
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Parse a plaintext `PQv1` discovery endpoint into its authority. This client currently
/// targets the fixed `/Root` database, so accepting a database path would be misleading.
pub fn parse_endpoint(endpoint: &str) -> anyhow::Result<String> {
    let uri: Uri = endpoint
        .parse()
        .map_err(|e| anyhow!("Invalid PQv1 discovery endpoint '{endpoint}': {e}"))?;
    let scheme = uri
        .scheme_str()
        .ok_or_else(|| anyhow!("PQv1 discovery endpoint must include the grpc:// scheme"))?;
    anyhow::ensure!(
        scheme == "grpc",
        "PQv1 scheme '{scheme}' is not supported: the custom transport requires grpc:// and uses a raw HTTP/2 TCP stream without TLS"
    );
    anyhow::ensure!(
        (uri.path().is_empty() || uri.path() == "/") && uri.query().is_none(),
        "PQv1 discovery endpoint must not contain a database path or query; the database is fixed to {YDB_DATABASE}"
    );
    let host = uri
        .authority()
        .map(http::uri::Authority::as_str)
        .ok_or_else(|| anyhow!("PQv1 discovery endpoint must include a host authority"))?
        .to_string();
    Ok(host)
}

pub fn http_uri(host: &str) -> anyhow::Result<Uri> {
    format!("http://{host}")
        .parse()
        .map_err(|e| anyhow!("bad uri http://{host}: {e}"))
}

fn format_host_port(host: &str, port: u16) -> String {
    if host.parse::<std::net::Ipv6Addr>().is_ok() {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

#[cfg(test)]
pub(super) fn socket_address(uri: &Uri) -> String {
    crate::providers::address::host_port(
        uri.host().unwrap_or("localhost"),
        uri.port_u16().unwrap_or(2135),
    )
}

/// Discover a proxy endpoint via `ListEndpoints` over HTTP/2 prior knowledge.
/// The gRPC response type is `GetOperationResponse` (matching Go's `conn.Invoke`).
async fn discover_proxies(
    main_uri: &Uri,
    token: &str,
    timeout: core::time::Duration,
    cancellation: &CancellationToken,
) -> anyhow::Result<Vec<crate::Ydb::discovery::EndpointInfo>> {
    use crate::Ydb::discovery::{ListEndpointsRequest, ListEndpointsResult};
    use crate::Ydb::operations::GetOperationResponse;
    use prost::Message as _;

    let h2 = connect_http2_prior_knowledge(main_uri, timeout, cancellation).await?;
    let mut grpc = tonic::client::Grpc::<H2Service>::with_origin(h2, main_uri.clone());

    let mut req = Request::new(ListEndpointsRequest {
        database: YDB_DATABASE.to_string(),
        service: vec![],
    });
    set_ydb_headers(req.metadata_mut(), token)?;

    let path =
        http::uri::PathAndQuery::from_static("/Ydb.Discovery.V1.DiscoveryService/ListEndpoints");
    let resp: GetOperationResponse =
        network_stage("PQv1 proxy discovery", timeout, cancellation, async {
            grpc.ready()
                .await
                .map_err(|e| anyhow!("ListEndpoints ready: {e}"))?;
            grpc.unary(
                req,
                path,
                tonic_prost::ProstCodec::<ListEndpointsRequest, GetOperationResponse>::default(),
            )
            .await
            .map(tonic::Response::into_inner)
            .map_err(|status| surface_session_failure(tonic_failure("proxy discovery", &status)))
        })
        .await?;

    let op = resp.operation.ok_or_else(|| anyhow!("no operation"))?;
    if !op.ready {
        anyhow::bail!("ListEndpoints not ready");
    }
    // SUCCESS is 400000, not 0 (0 == UNSPECIFIED also acceptable for forward-compat).
    if op.status != YDB_STATUS_UNSPECIFIED && op.status != YDB_STATUS_SUCCESS {
        let status_name =
            StatusCode::try_from(op.status).map_or("UNKNOWN", |status| status.as_str_name());
        let error = anyhow!(
            "PQv1 ListEndpoints failed: status={} ({status_name}), issues={:?}",
            op.status,
            op.issues
        );
        return Err(surface_session_failure(SessionFailure {
            error,
            kind: status_failure_kind(op.status),
        }));
    }
    let result = op.result.ok_or_else(|| anyhow!("no result"))?;
    let eps = ListEndpointsResult::decode(result.value.as_slice())?;
    anyhow::ensure!(!eps.endpoints.is_empty(), "no endpoints");
    Ok(eps.endpoints)
}

fn describe_topic_protocol_error(message: impl core::fmt::Display) -> anyhow::Error {
    PipelineFailure::fatal(anyhow!("PQv1 DescribeTopic protocol violation: {message}")).into()
}

pub(super) fn decode_describe_topic_response(
    response: crate::Ydb::pers_queue::v1::DescribeTopicResponse,
) -> anyhow::Result<crate::Ydb::pers_queue::v1::TopicSettings> {
    use crate::Ydb::pers_queue::v1::DescribeTopicResult;
    use prost::Message as _;

    let operation = response
        .operation
        .ok_or_else(|| describe_topic_protocol_error("response is missing operation"))?;
    if !operation.ready {
        return Err(describe_topic_protocol_error("SYNC operation is not ready"));
    }
    if operation.status != YDB_STATUS_SUCCESS {
        let status_name =
            StatusCode::try_from(operation.status).map_or("UNKNOWN", |status| status.as_str_name());
        let error = anyhow!(
            "PQv1 DescribeTopic failed: status={} ({status_name}), issues={:?}",
            operation.status,
            operation.issues
        );
        return Err(surface_session_failure(SessionFailure {
            error,
            kind: status_failure_kind(operation.status),
        }));
    }
    let result = operation
        .result
        .ok_or_else(|| describe_topic_protocol_error("successful operation is missing result"))?;
    let result = DescribeTopicResult::decode(result.value.as_slice()).map_err(|error| {
        describe_topic_protocol_error(format_args!("cannot decode result: {error}"))
    })?;
    result
        .settings
        .ok_or_else(|| describe_topic_protocol_error("successful result is missing topic settings"))
}

pub(super) fn describe_topic_request(
    topic_path: &str,
) -> crate::Ydb::pers_queue::v1::DescribeTopicRequest {
    use crate::Ydb::operations::{operation_params::OperationMode, OperationParams};

    crate::Ydb::pers_queue::v1::DescribeTopicRequest {
        operation_params: Some(OperationParams {
            operation_mode: OperationMode::Sync as i32,
            ..Default::default()
        }),
        path: topic_path.to_owned(),
    }
}

async fn describe_topic_metadata(
    main_uri: &Uri,
    topic_path: &str,
    token: &str,
    timeout: core::time::Duration,
    cancellation: &CancellationToken,
) -> anyhow::Result<crate::Ydb::pers_queue::v1::TopicSettings> {
    use crate::Ydb::pers_queue::v1::{DescribeTopicRequest, DescribeTopicResponse};

    let h2 = connect_http2_prior_knowledge(main_uri, timeout, cancellation).await?;
    let mut grpc = tonic::client::Grpc::<H2Service>::with_origin(h2, main_uri.clone())
        .max_decoding_message_size(MAX_GRPC_MESSAGE_SIZE)
        .max_encoding_message_size(MAX_GRPC_MESSAGE_SIZE);
    let mut request = Request::new(describe_topic_request(topic_path));
    set_ydb_headers(request.metadata_mut(), token)?;

    let path =
        http::uri::PathAndQuery::from_static("/Ydb.PersQueue.V1.PersQueueService/DescribeTopic");
    let response: DescribeTopicResponse = network_stage(
        "PQv1 topic metadata discovery",
        timeout,
        cancellation,
        async {
            grpc.ready()
                .await
                .map_err(|error| anyhow!("DescribeTopic ready: {error}"))?;
            grpc.unary(
                request,
                path,
                tonic_prost::ProstCodec::<DescribeTopicRequest, DescribeTopicResponse>::default(),
            )
            .await
            .map(tonic::Response::into_inner)
            .map_err(|status| {
                surface_session_failure(tonic_failure("topic metadata discovery", &status))
            })
        },
    )
    .await?;
    decode_describe_topic_response(response)
}

pub(super) fn ordered_plaintext_proxies(
    endpoints: Vec<crate::Ydb::discovery::EndpointInfo>,
    partition_id: i64,
) -> anyhow::Result<Vec<String>> {
    let mut proxies: Vec<_> = endpoints
        .into_iter()
        .filter(|endpoint| !endpoint.ssl)
        .filter_map(|endpoint| {
            let port = u16::try_from(endpoint.port).ok().filter(|port| *port > 0)?;
            let address = endpoint.address.trim();
            if address.is_empty() || !endpoint.load_factor.is_finite() || endpoint.load_factor < 0.0
            {
                return None;
            }
            Some((format_host_port(address, port), endpoint.load_factor))
        })
        .collect();
    proxies.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| left.1.total_cmp(&right.1))
    });
    proxies.dedup_by(|left, right| left.0 == right.0);
    anyhow::ensure!(
        !proxies.is_empty(),
        "discovery returned no usable plaintext endpoints"
    );

    // Weighted rendezvous ordering: every partition gets a stable primary and failover order,
    // while a larger discovery load factor makes an endpoint proportionally less likely to lead.
    let partition_bytes = partition_id.to_le_bytes();
    let mut scored: Vec<_> = proxies
        .into_iter()
        .map(|(address, load_factor)| {
            let mut key = Vec::with_capacity(partition_bytes.len() + address.len());
            key.extend_from_slice(&partition_bytes);
            key.extend_from_slice(address.as_bytes());
            let hash = stable_retry_seed(&key);
            let unit = (hash as f64 + 1.0) / (u64::MAX as f64 + 1.0);
            let score = -unit.ln() * (1.0 + f64::from(load_factor));
            (address, score)
        })
        .collect();
    scored.sort_by(|left, right| {
        left.1
            .total_cmp(&right.1)
            .then_with(|| left.0.cmp(&right.0))
    });
    Ok(scored.into_iter().map(|(address, _)| address).collect())
}

impl PqV1Client {
    pub async fn discover_endpoints(
        endpoint: &str,
        token: &str,
        network_timeout: core::time::Duration,
        cancellation: &CancellationToken,
    ) -> anyhow::Result<(String, Vec<crate::Ydb::discovery::EndpointInfo>)> {
        auth_metadata_value(token)?;
        let main_host = parse_endpoint(endpoint)?;
        let main_uri = http_uri(&main_host)?;
        let endpoints = discover_proxies(&main_uri, token, network_timeout, cancellation).await?;
        Ok((main_host, endpoints))
    }

    pub async fn describe_topic(
        endpoint: &str,
        topic_path: &str,
        token: &str,
        network_timeout: core::time::Duration,
        cancellation: &CancellationToken,
    ) -> anyhow::Result<crate::Ydb::pers_queue::v1::TopicSettings> {
        auth_metadata_value(token)?;
        let main_host = parse_endpoint(endpoint)?;
        let main_uri = http_uri(&main_host)?;
        describe_topic_metadata(&main_uri, topic_path, token, network_timeout, cancellation).await
    }

    pub fn order_proxies(
        main_host: String,
        endpoints: Vec<crate::Ydb::discovery::EndpointInfo>,
        partition_id: i64,
    ) -> Vec<String> {
        match ordered_plaintext_proxies(endpoints, partition_id) {
            Ok(mut proxies) => {
                if !proxies.iter().any(|proxy| proxy == &main_host) {
                    proxies.push(main_host);
                }
                proxies
            }
            Err(error) => {
                tracing::warn!(
                    "Proxy discovery returned no compatible endpoint: {error}. Using main endpoint."
                );
                vec![main_host]
            }
        }
    }
}
