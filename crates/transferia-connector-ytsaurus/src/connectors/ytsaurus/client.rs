use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use serde::Deserialize;
use serde_json::Value;
use tokio::sync::OnceCell;
use transferia_connector_support::outbound_http::{
    NetworkPolicy, OutboundHttpClient, OutboundHttpRequest,
};

use super::config::{
    YTsaurusConnectionConfig, YTsaurusTableReaderConfig,
};

#[derive(Debug)]
pub struct YTsaurusHttpError {
    pub status: reqwest::StatusCode,
    body: String,
}

impl core::fmt::Display for YTsaurusHttpError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "YTsaurus HTTP {}: {}", self.status, self.body)
    }
}

impl std::error::Error for YTsaurusHttpError {}

#[derive(Clone)]
pub struct YTsaurusClient {
    endpoint: reqwest::Url,
    token: String,
    client: OutboundHttpClient,
    heavy_endpoints: Arc<OnceCell<Vec<reqwest::Url>>>,
    next_heavy_endpoint: Arc<AtomicUsize>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum DiscoverProxiesResponse {
    List(Vec<String>),
    Object { proxies: Vec<String> },
}

#[derive(Deserialize)]
#[serde(untagged)]
pub(super) enum ListedNode {
    Name(String),
    WithAttributes {
        #[serde(rename = "$value")]
        name: String,
        #[serde(rename = "$attributes")]
        attributes: ListedNodeAttributes,
    },
}

#[derive(Deserialize)]
pub(super) struct ListedNodeAttributes {
    #[serde(rename = "type")]
    node_type: String,
}

impl DiscoverProxiesResponse {
    fn into_proxies(self) -> Vec<String> {
        match self {
            Self::List(proxies) | Self::Object { proxies } => proxies,
        }
    }
}

impl YTsaurusClient {
    pub fn new(config: &YTsaurusConnectionConfig) -> anyhow::Result<Self> {
        config.validate()?;
        Ok(Self {
            endpoint: config.endpoint().parse()?,
            token: config.auth.load_token()?,
            client: OutboundHttpClient::new(
                config.timeout(),
                [],
                NetworkPolicy::AllowPrivateNetworks,
            )?,
            heavy_endpoints: Arc::new(OnceCell::new()),
            next_heavy_endpoint: Arc::new(AtomicUsize::new(0)),
        })
    }

    fn request_at(
        &self,
        endpoint: &reqwest::Url,
        method: reqwest::Method,
        command: &str,
    ) -> anyhow::Result<OutboundHttpRequest> {
        let mut url = endpoint.clone();
        let mut segments = url
            .path_segments_mut()
            .map_err(|()| anyhow::anyhow!("YTsaurus endpoint cannot be a base URL"))?;
        segments.pop_if_empty().extend(["api", "v3", command]);
        drop(segments);
        let request = self.client.request(method, url);
        Ok(request.configure(|request| {
            request.header(
                reqwest::header::AUTHORIZATION,
                format!("OAuth {}", self.token),
            )
        }))
    }

    fn request(
        &self,
        method: reqwest::Method,
        command: &str,
    ) -> anyhow::Result<OutboundHttpRequest> {
        self.request_at(&self.endpoint, method, command)
    }

    async fn discover_heavy_endpoints(&self) -> anyhow::Result<Vec<reqwest::Url>> {
        let mut url = self.endpoint.clone();
        url.set_path("/api/v4/discover_proxies");
        url.query_pairs_mut()
            .clear()
            .append_pair("type", "http")
            .append_pair(
                "address_type",
                if self.endpoint.scheme() == "https" {
                    "https"
                } else {
                    "http"
                },
            );
        let response = self
            .client
            .request(reqwest::Method::GET, url)
            .configure(|request| {
                request.header(
                    reqwest::header::AUTHORIZATION,
                    format!("OAuth {}", self.token),
                )
            })
            .send()
            .await?;
        let response = Self::checked(response).await?;
        let proxies = response
            .json::<DiscoverProxiesResponse>()
            .await?
            .into_proxies();
        anyhow::ensure!(!proxies.is_empty(), "YTsaurus data proxy discovery returned no proxies");

        proxies
            .into_iter()
            .map(|proxy| {
                let mut endpoint = if proxy.contains("://") {
                    proxy.parse::<reqwest::Url>()?
                } else {
                    format!("{}://{proxy}", self.endpoint.scheme()).parse::<reqwest::Url>()?
                };
                endpoint.set_path("");
                endpoint.set_query(None);
                endpoint.set_fragment(None);
                Ok(endpoint)
            })
            .collect()
    }

    async fn heavy_request(
        &self,
        method: reqwest::Method,
        command: &str,
    ) -> anyhow::Result<OutboundHttpRequest> {
        let endpoints = self
            .heavy_endpoints
            .get_or_try_init(|| self.discover_heavy_endpoints())
            .await?;
        let index = self.next_heavy_endpoint.fetch_add(1, Ordering::Relaxed) % endpoints.len();
        self.request_at(&endpoints[index], method, command)
    }

    async fn checked(response: reqwest::Response) -> anyhow::Result<reqwest::Response> {
        if response.status().is_success() {
            return Ok(response);
        }
        let status = response.status();
        let bytes = response.bytes().await?;
        let body = String::from_utf8_lossy(&bytes[..bytes.len().min(64 * 1024)]).into_owned();
        Err(YTsaurusHttpError { status, body }.into())
    }

    pub async fn get_json(&self, path: &str) -> anyhow::Result<Value> {
        let parameters = serde_json::json!({ "path": path });
        let parameters = serde_json::to_string(&parameters)?;
        let response = self
            .request(reqwest::Method::GET, "get")?
            .configure(|request| {
                request
                    .header("X-YT-Parameters", parameters)
                    .header(reqwest::header::ACCEPT, "application/json")
            })
            .send()
            .await?;
        Ok(Self::checked(response).await?.json().await?)
    }

    pub async fn list_table_paths(&self, query: &str) -> anyhow::Result<Vec<String>> {
        let directory = suggestion_directory(query)?;
        let parameters = serde_json::json!({
            "path": directory,
            "attributes": ["type"],
        });
        let response = self
            .request(reqwest::Method::GET, "list")?
            .configure(|request| {
                request
                    .header("X-YT-Parameters", parameters.to_string())
                    .header(reqwest::header::ACCEPT, "application/json")
            })
            .send()
            .await?;
        let nodes = Self::checked(response)
            .await?
            .json::<Vec<ListedNode>>()
            .await?;
        Ok(table_path_suggestions(&directory, nodes))
    }

    pub async fn read_table(
        &self,
        path: &str,
        start_row_index: i64,
        output_format: &str,
        unordered: bool,
        table_reader: &YTsaurusTableReaderConfig,
    ) -> anyhow::Result<reqwest::Response> {
        anyhow::ensure!(
            start_row_index >= 0,
            "YTsaurus start row index must not be negative"
        );
        let path = rich_read_path(path, start_row_index);
        let parameters = serde_json::json!({
            "path": path,
            "unordered": unordered,
            "table_reader": table_reader,
        });
        let parameters = serde_json::to_string(&parameters)?;
        let response = self
            .heavy_request(reqwest::Method::GET, "read_table")
            .await?
            .configure(|request| {
                request
                    .header("X-YT-Parameters", parameters)
                    .header("X-YT-Output-Format", output_format)
            })
            .send()
            .await?;
        Self::checked(response).await
    }

    pub async fn read_arrow(
        &self,
        path: &str,
        start_row_index: i64,
        unordered: bool,
    ) -> anyhow::Result<reqwest::Response> {
        self.read_table(
            path,
            start_row_index,
            "\"arrow\"",
            unordered,
            &YTsaurusTableReaderConfig::default(),
        )
        .await
    }

    pub async fn discover_rpc_endpoints(&self) -> anyhow::Result<Vec<String>> {
        let mut url = self.endpoint.clone();
        url.set_path("/api/v4/discover_proxies");
        url.query_pairs_mut().clear().append_pair("type", "rpc");
        let response = self
            .client
            .request(reqwest::Method::GET, url)
            .configure(|request| {
                request.header(
                    reqwest::header::AUTHORIZATION,
                    format!("OAuth {}", self.token),
                )
            })
            .send()
            .await?;
        let endpoints = Self::checked(response)
            .await?
            .json::<DiscoverProxiesResponse>()
            .await?
            .into_proxies();
        anyhow::ensure!(
            !endpoints.is_empty(),
            "YTsaurus RPC proxy discovery returned no proxies"
        );
        Ok(endpoints)
    }

    pub(crate) fn token(&self) -> &str {
        &self.token
    }

    pub async fn write_table(
        &self,
        path: &str,
        format: &str,
        payload: Vec<u8>,
    ) -> anyhow::Result<()> {
        let parameters = serde_json::json!({ "path": format!("<append=%true>{path}") });
        let parameters = serde_json::to_string(&parameters)?;
        let response = self
            .heavy_request(reqwest::Method::PUT, "write_table")
            .await?
            .configure(|request| {
                request
                    .header("X-YT-Parameters", parameters)
                    .header("X-YT-Input-Format", format!("\"{format}\""))
                    .body(payload)
            })
            .send()
            .await?;
        Self::checked(response).await?;
        Ok(())
    }

    pub async fn remove_table(&self, path: &str) -> anyhow::Result<()> {
        let parameters = serde_json::json!({ "path": path, "force": true });
        let parameters = serde_json::to_string(&parameters)?;
        let response = self
            .request(reqwest::Method::POST, "remove")?
            .configure(|request| request.header("X-YT-Parameters", parameters))
            .send()
            .await?;
        Self::checked(response).await?;
        Ok(())
    }

    pub async fn create_table(&self, path: &str, schema: Value) -> anyhow::Result<()> {
        let parameters = serde_json::json!({
            "type": "table",
            "path": path,
            "attributes": { "schema": schema, "optimize_for": "scan" }
        });
        let parameters = serde_json::to_string(&parameters)?;
        let response = self
            .request(reqwest::Method::POST, "create")?
            .configure(|request| request.header("X-YT-Parameters", parameters))
            .send()
            .await?;
        Self::checked(response).await?;
        Ok(())
    }

    pub async fn create_directory(&self, path: &str) -> anyhow::Result<()> {
        let parameters = serde_json::json!({
            "type": "map_node",
            "path": path,
            "recursive": true,
            "ignore_existing": true
        });
        let parameters = serde_json::to_string(&parameters)?;
        let response = self
            .request(reqwest::Method::POST, "create")?
            .configure(|request| request.header("X-YT-Parameters", parameters))
            .send()
            .await?;
        Self::checked(response).await?;
        Ok(())
    }
}

pub(super) fn suggestion_directory(query: &str) -> anyhow::Result<String> {
    let query = query.trim();
    let directory = if matches!(query, "" | "/" | "//") {
        "//"
    } else {
        query.trim_end_matches('/')
    };
    anyhow::ensure!(
        directory == "//" || (directory.starts_with("//") && directory.len() > 2),
        "YTsaurus path suggestion query must be empty or start with '//'"
    );
    anyhow::ensure!(
        !directory.contains('<') && !directory.contains('>') && !directory.contains('\0'),
        "YTsaurus path suggestion query must not contain rich-path attributes or NUL"
    );
    Ok(directory.to_owned())
}

pub(super) fn table_path_suggestions(directory: &str, nodes: Vec<ListedNode>) -> Vec<String> {
    let prefix = if directory == "//" {
        "//".to_owned()
    } else {
        format!("{directory}/")
    };
    let mut suggestions = nodes
        .into_iter()
        .filter_map(|node| match node {
            ListedNode::WithAttributes { name, attributes }
                if attributes.node_type == "table" => Some(format!("{prefix}{name}")),
            ListedNode::WithAttributes { name, attributes }
                if matches!(attributes.node_type.as_str(), "map_node" | "portal_entrance") =>
            {
                Some(format!("{prefix}{name}/"))
            }
            ListedNode::Name(_name) => None,
            ListedNode::WithAttributes { .. } => None,
        })
        .collect::<Vec<_>>();
    suggestions.sort_unstable();
    suggestions
}

pub(super) fn rich_read_path(path: &str, start_row_index: i64) -> String {
    if start_row_index == 0 {
        return path.to_owned();
    }
    format!("<ranges=[{{lower_limit={{row_index={start_row_index}}}}}]>{path}")
}

pub fn classify_http_failure(error: anyhow::Error) -> anyhow::Error {
    let permanent = error
        .downcast_ref::<YTsaurusHttpError>()
        .is_some_and(|http| {
            http.status.is_client_error()
                && http.status != reqwest::StatusCode::REQUEST_TIMEOUT
                && http.status != reqwest::StatusCode::TOO_MANY_REQUESTS
                && http.status != reqwest::StatusCode::CONFLICT
                && http.status != reqwest::StatusCode::LOCKED
        });
    if permanent {
        transferia_core::failure::DataPlaneFailure::fatal(error).into()
    } else {
        error
    }
}
