use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::BytesMut;
use futures_util::StreamExt as _;
use reqwest::{Method, StatusCode, Url};
use thiserror::Error;
use transferia_connector_support::outbound_http::{OutboundHttpClient, OutboundHttpError};

use super::connection::OpenSearchConnectionConfig;

#[derive(Debug, Error)]
pub enum OpenSearchHttpError {
    #[error("OpenSearch request was rejected by the outbound HTTP boundary")]
    Boundary,

    #[error("OpenSearch request failed before a response was received")]
    Transport,

    #[error("OpenSearch returned HTTP {status}")]
    Status { status: StatusCode },

    #[error("OpenSearch response exceeds configured max_response_bytes={limit}")]
    ResponseTooLarge { limit: usize },

    #[error("OpenSearch returned invalid JSON")]
    InvalidJson,
}

impl OpenSearchHttpError {
    #[must_use]
    pub const fn retryable(&self) -> bool {
        match self {
            Self::Transport => true,
            Self::Status { status } => {
                matches!(status.as_u16(), 408 | 425 | 429 | 500 | 502 | 503 | 504)
            }
            Self::Boundary | Self::ResponseTooLarge { .. } | Self::InvalidJson => false,
        }
    }
}

#[derive(Debug)]
pub struct OpenSearchResponse {
    pub body: Vec<u8>,
}

#[derive(Clone)]
pub struct OpenSearchClient {
    http: OutboundHttpClient,

    bases: Arc<[Url]>,

    auth: super::OpenSearchAuth,

    max_response_bytes: usize,

    next_node: Arc<AtomicUsize>,
}

impl OpenSearchClient {
    pub fn new(config: &OpenSearchConnectionConfig) -> anyhow::Result<Self> {
        config.validate()?;
        let scheme = if config.trusted_plaintext {
            "http"
        } else {
            "https"
        };
        let bases = config
            .hosts
            .iter()
            .map(|host| {
                let mut url = Url::parse(&format!("{scheme}://localhost"))?;
                url.set_host(Some(host))
                    .map_err(|_| anyhow::anyhow!("invalid OpenSearch host"))?;
                url.set_port(Some(config.port))
                    .map_err(|()| anyhow::anyhow!("invalid OpenSearch port"))?;
                Ok(url)
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        Ok(Self {
            http: config.http_client()?,
            bases: Arc::from(bases),
            auth: config.auth.clone(),
            max_response_bytes: config.max_response_bytes,
            next_node: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub async fn json<T: serde::de::DeserializeOwned>(
        &self,
        method: Method,
        path: &[&str],
        query: &[(&str, String)],
        body: Option<&serde_json::Value>,
    ) -> Result<T, OpenSearchHttpError> {
        let response = self
            .request(
                method,
                path,
                query,
                "application/json",
                body.map(serde_json::to_vec)
                    .transpose()
                    .map_err(|_| OpenSearchHttpError::InvalidJson)?,
            )
            .await?;
        let value =
            serde_json::from_slice(&response.body).map_err(|_| OpenSearchHttpError::InvalidJson)?;
        Ok(value)
    }

    pub async fn request(
        &self,
        method: Method,
        path: &[&str],
        query: &[(&str, String)],
        content_type: &'static str,
        body: Option<Vec<u8>>,
    ) -> Result<OpenSearchResponse, OpenSearchHttpError> {
        let mut url = self.next_base();
        {
            let mut segments = url
                .path_segments_mut()
                .map_err(|()| OpenSearchHttpError::Boundary)?;
            segments.pop_if_empty();
            for segment in path {
                segments.push(segment);
            }
        }
        let auth = self.auth.basic();
        let max_response_bytes = self.max_response_bytes;
        let request = self.http.request(method, url).configure(move |request| {
            let request = request.query(query).header("Accept", "application/json");
            let request = if let Some((username, password)) = auth {
                request.basic_auth(username, Some(password))
            } else {
                request
            };
            match body {
                Some(body) => request.header("Content-Type", content_type).body(body),
                None => request,
            }
        });
        let response = request
            .send()
            .await
            .map_err(|error| map_boundary_error(&error))?;
        let status = response.status();
        if !status.is_success() {
            return Err(OpenSearchHttpError::Status { status });
        }
        if response
            .content_length()
            .is_some_and(|length| length > max_response_bytes as u64)
        {
            return Err(OpenSearchHttpError::ResponseTooLarge {
                limit: max_response_bytes,
            });
        }
        let mut output = BytesMut::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|_| OpenSearchHttpError::Transport)?;
            if output.len().saturating_add(chunk.len()) > max_response_bytes {
                return Err(OpenSearchHttpError::ResponseTooLarge {
                    limit: max_response_bytes,
                });
            }
            output.extend_from_slice(&chunk);
        }
        Ok(OpenSearchResponse {
            body: output.to_vec(),
        })
    }

    fn next_base(&self) -> Url {
        let index = self.next_node.fetch_add(1, Ordering::Relaxed) % self.bases.len();
        self.bases[index].clone()
    }
}

const fn map_boundary_error(error: &OutboundHttpError) -> OpenSearchHttpError {
    match error {
        OutboundHttpError::InvalidUrl { .. } | OutboundHttpError::RedirectRejected { .. } => {
            OpenSearchHttpError::Boundary
        }
        OutboundHttpError::Request(_) => OpenSearchHttpError::Transport,
    }
}
