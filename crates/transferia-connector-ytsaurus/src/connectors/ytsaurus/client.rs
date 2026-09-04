use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::stream::{self, StreamExt as _};
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::OnceCell;
use transferia_connector_support::outbound_http::{
    NetworkPolicy, OutboundHttpClient, OutboundHttpRequest,
};

use super::config::{
    YTsaurusAtomicity, YTsaurusConnectionConfig, YTsaurusOptimizeFor, YTsaurusTableReaderConfig,
    YTsaurusTableWriterConfig,
};

#[derive(Debug)]
pub struct YTsaurusHttpError {
    pub status: reqwest::StatusCode,
    body: String,
}

impl core::fmt::Display for YTsaurusHttpError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let message = serde_json::from_str::<Value>(&self.body)
            .ok()
            .and_then(|body| body.get("message")?.as_str().map(str::to_owned))
            .unwrap_or_else(|| "the server rejected the request".to_owned());
        write!(f, "YTsaurus request failed ({}): {message}", self.status)
    }
}

impl std::error::Error for YTsaurusHttpError {}

#[derive(Clone)]
pub struct YTsaurusClient {
    endpoint: reqwest::Url,
    token: String,
    rpc_proxy_role: Option<String>,
    client: OutboundHttpClient,
    heavy_endpoints: Arc<OnceCell<Vec<reqwest::Url>>>,
    next_heavy_endpoint: Arc<AtomicUsize>,
}

pub struct DistributedWriteSession {
    session: Value,
    cookies: Vec<Value>,
}

pub(super) fn static_table_attributes(
    schema: &Value,
    optimize_for: YTsaurusOptimizeFor,
    primary_medium: &str,
    custom_attributes: &BTreeMap<String, Value>,
) -> anyhow::Result<Value> {
    let mut attributes = serde_json::json!({
        "schema": schema,
        "optimize_for": optimize_for.as_str(),
        "chunk_format": optimize_for.chunk_format(),
        "primary_medium": primary_medium,
    });
    extend_attributes(&mut attributes, custom_attributes)?;
    Ok(attributes)
}

fn extend_attributes(
    attributes: &mut Value,
    custom_attributes: &BTreeMap<String, Value>,
) -> anyhow::Result<()> {
    let object = attributes
        .as_object_mut()
        .ok_or_else(|| anyhow::anyhow!("table attributes must be an object"))?;
    for (name, value) in custom_attributes {
        object.entry(name.clone()).or_insert_with(|| value.clone());
    }
    Ok(())
}

pub(super) fn table_writer_spec(
    table_writer: &YTsaurusTableWriterConfig,
    custom_spec: &BTreeMap<String, Value>,
) -> anyhow::Result<Value> {
    let mut value = serde_json::to_value(table_writer)?;
    let object = value
        .as_object_mut()
        .ok_or_else(|| anyhow::anyhow!("YT table_writer config must be an object"))?;
    for (name, custom_value) in custom_spec {
        object.insert(name.clone(), custom_value.clone());
    }
    Ok(value)
}

impl DistributedWriteSession {
    pub fn into_parts(self) -> (Value, Vec<Value>) {
        (self.session, self.cookies)
    }
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
        Self::new_with_proxy_role(config, None)
    }

    pub fn new_with_proxy_role(
        config: &YTsaurusConnectionConfig,
        proxy_role: Option<&str>,
    ) -> anyhow::Result<Self> {
        config.validate()?;
        if let Some(proxy_role) = proxy_role {
            anyhow::ensure!(
                !proxy_role.is_empty() && proxy_role.trim() == proxy_role,
                "YTsaurus RPC proxy role must be non-empty and contain no surrounding whitespace"
            );
        }
        Ok(Self {
            endpoint: config.endpoint().parse()?,
            token: config.auth.load_token()?,
            rpc_proxy_role: proxy_role.map(str::to_owned),
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
        self.request_at_version(endpoint, "v3", method, command)
    }

    fn request_at_version(
        &self,
        endpoint: &reqwest::Url,
        version: &str,
        method: reqwest::Method,
        command: &str,
    ) -> anyhow::Result<OutboundHttpRequest> {
        let mut url = endpoint.clone();
        let mut segments = url
            .path_segments_mut()
            .map_err(|()| anyhow::anyhow!("YTsaurus endpoint cannot be a base URL"))?;
        segments.pop_if_empty().extend(["api", version, command]);
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

    fn request_v4(
        &self,
        method: reqwest::Method,
        command: &str,
    ) -> anyhow::Result<OutboundHttpRequest> {
        self.request_at_version(&self.endpoint, "v4", method, command)
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
        anyhow::ensure!(
            !proxies.is_empty(),
            "YTsaurus data proxy discovery returned no proxies"
        );

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
                if endpoint.host_str().is_some_and(is_loopback_host)
                    && self.endpoint.host_str().is_some_and(is_loopback_host)
                {
                    endpoint = self.endpoint.clone();
                    endpoint.set_path("");
                    endpoint.set_query(None);
                    endpoint.set_fragment(None);
                }
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

    async fn heavy_request_v4(
        &self,
        method: reqwest::Method,
        command: &str,
    ) -> anyhow::Result<OutboundHttpRequest> {
        let endpoints = self
            .heavy_endpoints
            .get_or_try_init(|| self.discover_heavy_endpoints())
            .await?;
        let index = self.next_heavy_endpoint.fetch_add(1, Ordering::Relaxed) % endpoints.len();
        self.request_at_version(&endpoints[index], "v4", method, command)
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

    pub async fn node_exists(&self, path: &str) -> anyhow::Result<bool> {
        let parameters = serde_json::json!({ "path": path });
        let response = self
            .request(reqwest::Method::GET, "exists")?
            .configure(|request| {
                request
                    .header("X-YT-Parameters", parameters.to_string())
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
        let prefix = suggestion_prefix(&directory);
        let links = nodes
            .iter()
            .filter_map(|node| match node {
                ListedNode::WithAttributes { name, attributes }
                    if attributes.node_type == "link" =>
                {
                    Some(format!("{prefix}{name}"))
                }
                ListedNode::Name(_) | ListedNode::WithAttributes { .. } => None,
            })
            .collect::<Vec<_>>();
        let mut suggestions = table_path_suggestions(&directory, nodes);
        suggestions.extend(
            stream::iter(links)
                .map(|path| async move {
                    let node_type = self
                        .get_json(&super::attribute_path(&path, "type"))
                        .await
                        .ok()?
                        .as_str()?
                        .to_owned();
                    resolved_link_suggestion(path, &node_type)
                })
                .buffer_unordered(8)
                .filter_map(std::future::ready)
                .collect::<Vec<_>>()
                .await,
        );
        suggestions.sort_unstable();
        suggestions.dedup();
        Ok(suggestions)
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
        let url = rpc_proxy_discovery_url(&self.endpoint, self.rpc_proxy_role.as_deref());
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

    pub async fn list_rpc_proxy_roles(&self) -> anyhow::Result<Vec<String>> {
        let parameters = serde_json::json!({ "path": "//sys/rpc_proxy_roles" });
        let response = self
            .request(reqwest::Method::GET, "list")?
            .configure(|request| {
                request
                    .header("X-YT-Parameters", parameters.to_string())
                    .header(reqwest::header::ACCEPT, "application/json")
            })
            .send()
            .await?;
        let roles = Self::checked(response).await?.json::<Vec<String>>().await?;
        Ok(normalize_rpc_proxy_roles(roles))
    }

    pub(crate) fn token(&self) -> &str {
        &self.token
    }

    pub async fn write_table(
        &self,
        path: &str,
        format: &str,
        payload: Vec<u8>,
        row_buffer_bytes: u64,
        table_writer: &YTsaurusTableWriterConfig,
        custom_spec: &BTreeMap<String, Value>,
    ) -> anyhow::Result<()> {
        let parameters = serde_json::json!({
            "path": format!("<append=%true>{path}"),
            "max_row_buffer_size": row_buffer_bytes,
            "table_writer": table_writer_spec(table_writer, custom_spec)?,
        });
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

    pub async fn start_distributed_write(
        &self,
        destination_path: &str,
        cookie_count: usize,
        session_timeout_ms: u64,
    ) -> anyhow::Result<DistributedWriteSession> {
        anyhow::ensure!(
            cookie_count > 0,
            "distributed write requires at least one cookie"
        );
        let parameters = serde_json::json!({
            "path": format!("<append=%true>{destination_path}"),
            "cookie_count": cookie_count,
            "session_timeout": session_timeout_ms,
        });
        let parameters = json_header_value(&parameters)?;
        let response = self
            .heavy_request_v4(reqwest::Method::POST, "start_distributed_write_session")
            .await?
            .configure(|request| {
                request
                    .header("X-YT-Parameters", parameters)
                    .header(reqwest::header::ACCEPT, "application/json")
            })
            .send()
            .await?;
        let value = Self::checked(response).await?.json::<Value>().await?;
        let session = value
            .get("session")
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("distributed write response has no session"))?;
        let cookies = value
            .get("cookies")
            .and_then(Value::as_array)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("distributed write response has no cookies"))?;
        anyhow::ensure!(
            cookies.len() == cookie_count,
            "distributed write returned {} cookies instead of {cookie_count}",
            cookies.len()
        );
        Ok(DistributedWriteSession { session, cookies })
    }

    pub async fn write_table_fragment(
        &self,
        cookie: Value,
        format: &str,
        payload: Vec<u8>,
        row_buffer_bytes: u64,
        table_writer: &YTsaurusTableWriterConfig,
        custom_spec: &BTreeMap<String, Value>,
    ) -> anyhow::Result<Value> {
        let parameters = serde_json::json!({
            "cookie": cookie,
            "max_row_buffer_size": row_buffer_bytes,
            "table_writer": table_writer_spec(table_writer, custom_spec)?,
        });
        let parameters = json_header_value(&parameters)?;
        let response = self
            .heavy_request_v4(reqwest::Method::PUT, "write_table_fragment")
            .await?
            .configure(|request| {
                request
                    .header("X-YT-Parameters", parameters)
                    .header("X-YT-Input-Format", format!("\"{format}\""))
                    .header(reqwest::header::ACCEPT, "application/json")
                    .body(payload)
            })
            .send()
            .await?;
        Ok(Self::checked(response).await?.json::<Value>().await?)
    }

    pub async fn finish_distributed_write(
        &self,
        session: Value,
        results: Vec<Value>,
    ) -> anyhow::Result<()> {
        let parameters = serde_json::json!({ "session": session, "results": results });
        let parameters = json_header_value(&parameters)?;
        let response = self
            .heavy_request_v4(reqwest::Method::POST, "finish_distributed_write_session")
            .await?
            .configure(|request| request.header("X-YT-Parameters", parameters))
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

    pub async fn remove_speedtest_root(
        &self,
        path: &str,
        revision: u64,
        mutation_id: &str,
    ) -> anyhow::Result<()> {
        let mut last_error = None;
        for retry in [false, true] {
            let parameters = recursive_removal_parameters(path, revision, mutation_id, retry);
            let response = self
                .request(reqwest::Method::POST, "remove")?
                .configure(|request| request.header("X-YT-Parameters", parameters.to_string()))
                .send()
                .await;
            match response {
                Ok(response) => match Self::checked(response).await {
                    Ok(_) => return Ok(()),
                    Err(error) => last_error = Some(error),
                },
                Err(error) => last_error = Some(error.into()),
            }
        }
        Err(last_error.unwrap_or_else(|| anyhow::anyhow!("YTsaurus remove returned no result")))
    }

    pub async fn create_table(
        &self,
        path: &str,
        schema: &Value,
        optimize_for: YTsaurusOptimizeFor,
        primary_medium: &str,
        custom_attributes: &BTreeMap<String, Value>,
    ) -> anyhow::Result<()> {
        let parameters = serde_json::json!({
            "type": "table",
            "path": path,
            "attributes": static_table_attributes(
                schema,
                optimize_for,
                primary_medium,
                custom_attributes,
            )?,
        });
        let parameters = yson_header_value(&parameters)?;
        let response = self
            .request(reqwest::Method::POST, "create")?
            .configure(|request| {
                request
                    .header("X-YT-Header-Format", "<format=text>yson")
                    .header("X-YT-Parameters", parameters)
            })
            .send()
            .await?;
        Self::checked(response).await?;
        Ok(())
    }

    pub async fn create_dynamic_table(
        &self,
        path: &str,
        schema: &Value,
        atomicity: YTsaurusAtomicity,
        primary_medium: &str,
        custom_attributes: &BTreeMap<String, Value>,
        tablet_cell_bundle: Option<&str>,
        dynamic_store_overflow_threshold: f64,
        table_ttl_ms: Option<u64>,
    ) -> anyhow::Result<()> {
        let attributes = dynamic_table_attributes(
            schema,
            atomicity,
            primary_medium,
            custom_attributes,
            tablet_cell_bundle,
            dynamic_store_overflow_threshold,
            table_ttl_ms,
        )?;
        let parameters = serde_json::json!({
            "type": "table",
            "path": path,
            "attributes": attributes,
        });
        let parameters = yson_header_value(&parameters)?;
        let response = self
            .request(reqwest::Method::POST, "create")?
            .configure(|request| {
                request
                    .header("X-YT-Header-Format", "<format=text>yson")
                    .header("X-YT-Parameters", parameters)
            })
            .send()
            .await?;
        Self::checked(response).await?;
        Ok(())
    }

    pub async fn mount_table(&self, path: &str, timeout: Duration) -> anyhow::Result<()> {
        let parameters = serde_json::json!({ "path": path });
        let response = self
            .request_v4(reqwest::Method::POST, "mount_table")?
            .configure(|request| request.header("X-YT-Parameters", parameters.to_string()))
            .send()
            .await?;
        Self::checked(response).await?;

        let deadline = Instant::now() + timeout;
        loop {
            let state = self
                .get_json(&super::attribute_path(path, "tablet_state"))
                .await?;
            match state.as_str() {
                Some("mounted") => return Ok(()),
                Some("mounting" | "unmounted" | "transient") => {}
                Some(other) => anyhow::bail!(
                    "YTsaurus dynamic table '{path}' entered unexpected tablet state '{other}'"
                ),
                None => anyhow::bail!(
                    "YTsaurus dynamic table '{path}' returned a non-string tablet state"
                ),
            }
            anyhow::ensure!(
                Instant::now() < deadline,
                "YTsaurus dynamic table '{path}' did not mount within {} ms",
                timeout.as_millis()
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    pub async fn reshard_table_uniform(
        &self,
        path: &str,
        tablet_count: usize,
    ) -> anyhow::Result<()> {
        let parameters = uniform_reshard_parameters(path, tablet_count)?;
        let response = self
            .request_v4(reqwest::Method::POST, "reshard_table")?
            .configure(|request| request.header("X-YT-Parameters", parameters.to_string()))
            .send()
            .await?;
        Self::checked(response).await?;
        let actual = self
            .get_json(&super::attribute_path(path, "tablet_count"))
            .await?;
        anyhow::ensure!(
            actual.as_u64() == Some(u64::try_from(tablet_count)?),
            "YTsaurus dynamic table '{path}' has {actual} tablets after reshard, expected {tablet_count}"
        );
        Ok(())
    }

    pub async fn convert_static_table_to_dynamic(
        &self,
        path: &str,
        atomicity: YTsaurusAtomicity,
        primary_medium: &str,
        custom_attributes: &BTreeMap<String, Value>,
        tablet_cell_bundle: Option<&str>,
        dynamic_store_overflow_threshold: f64,
        table_ttl_ms: Option<u64>,
    ) -> anyhow::Result<()> {
        let parameters = serde_json::json!({
            "path": path,
            "dynamic": true,
        });
        let response = self
            .request_v4(reqwest::Method::POST, "alter_table")?
            .configure(|request| request.header("X-YT-Parameters", parameters.to_string()))
            .send()
            .await?;
        Self::checked(response).await?;

        let attributes = dynamic_conversion_attributes(
            atomicity,
            primary_medium,
            custom_attributes,
            tablet_cell_bundle,
            dynamic_store_overflow_threshold,
            table_ttl_ms,
        )?;
        let parameters = serde_json::json!({
            "path": format!("{}/@", path.trim_end_matches('/')),
        });
        let response = self
            .request_v4(reqwest::Method::POST, "multiset_attributes")?
            .configure(|request| {
                request
                    .header("X-YT-Parameters", parameters.to_string())
                    .header("X-YT-Input-Format", "\"json\"")
                    .body(attributes.to_string())
            })
            .send()
            .await?;
        Self::checked(response).await?;
        Ok(())
    }

    pub async fn move_table(
        &self,
        source_path: &str,
        destination_path: &str,
    ) -> anyhow::Result<()> {
        let parameters = serde_json::json!({
            "source_path": source_path,
            "destination_path": destination_path,
            "recursive": true,
            "force": true,
        });
        let response = self
            .request(reqwest::Method::POST, "move")?
            .configure(|request| request.header("X-YT-Parameters", parameters.to_string()))
            .send()
            .await?;
        Self::checked(response).await?;
        Ok(())
    }

    pub async fn sort_table_unique(
        &self,
        source_path: &str,
        destination_path: &str,
        primary_keys: &[String],
        mutation_id: &str,
        timeout: Duration,
        operation_pool: Option<&str>,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            !primary_keys.is_empty(),
            "YTsaurus unique sort requires at least one primary-key column"
        );
        let sort_by = primary_keys
            .iter()
            .map(|name| serde_json::json!({ "name": name, "sort_order": "ascending" }))
            .collect::<Vec<_>>();
        let parameters = sort_operation_parameters(
            source_path,
            destination_path,
            &sort_by,
            mutation_id,
            operation_pool,
        );
        let response = self
            .request_v4(reqwest::Method::POST, "start_operation")?
            .configure(|request| {
                request
                    .header("X-YT-Parameters", parameters.to_string())
                    .header(reqwest::header::ACCEPT, "application/json")
            })
            .send()
            .await?;
        let operation = Self::checked(response)
            .await?
            .json::<Value>()
            .await?
            .get("operation_id")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                anyhow::anyhow!("YTsaurus start_operation response has no operation_id")
            })?
            .to_owned();
        let deadline = Instant::now() + timeout;
        loop {
            let parameters = serde_json::json!({
                "operation_id": operation,
                "attributes": ["state", "result"],
            });
            let response = self
                .request_v4(reqwest::Method::GET, "get_operation")?
                .configure(|request| {
                    request
                        .header("X-YT-Parameters", parameters.to_string())
                        .header(reqwest::header::ACCEPT, "application/json")
                })
                .send()
                .await?;
            let status = Self::checked(response).await?.json::<Value>().await?;
            match status.get("state").and_then(Value::as_str) {
                Some("completed") => return Ok(()),
                Some("failed" | "aborted") => {
                    let error = status
                        .pointer("/result/error/message")
                        .and_then(Value::as_str)
                        .unwrap_or("the operation did not complete successfully");
                    anyhow::bail!("YTsaurus unique sort operation failed: {error}");
                }
                Some(_) => {}
                None => anyhow::bail!("YTsaurus get_operation response has no state"),
            }
            anyhow::ensure!(
                Instant::now() < deadline,
                "YTsaurus unique sort operation timed out after {} ms",
                timeout.as_millis()
            );
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
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

    pub async fn create_speedtest_directory(
        &self,
        path: &str,
        owner: &str,
        mutation_id: &str,
    ) -> anyhow::Result<()> {
        let mut last_error = None;
        for retry in [false, true] {
            let parameters = speedtest_directory_parameters(path, owner, mutation_id, retry);
            let response = self
                .request(reqwest::Method::POST, "create")?
                .configure(|request| request.header("X-YT-Parameters", parameters.to_string()))
                .send()
                .await;
            match response {
                Ok(response) => match Self::checked(response).await {
                    Ok(_) => return Ok(()),
                    Err(error) => last_error = Some(error),
                },
                Err(error) => last_error = Some(error.into()),
            }
        }
        Err(last_error.unwrap_or_else(|| anyhow::anyhow!("YTsaurus create returned no result")))
    }
}

pub(super) fn speedtest_directory_parameters(
    path: &str,
    owner: &str,
    mutation_id: &str,
    retry: bool,
) -> Value {
    serde_json::json!({
        "type": "map_node",
        "path": path,
        "recursive": false,
        "ignore_existing": false,
        "attributes": { "transferia_speedtest_owner": owner },
        "mutation_id": mutation_id,
        "retry": retry
    })
}

pub(super) fn recursive_removal_parameters(
    path: &str,
    revision: u64,
    mutation_id: &str,
    retry: bool,
) -> Value {
    serde_json::json!({
        "path": path,
        "force": true,
        "recursive": true,
        "prerequisite_revisions": [{ "path": path, "revision": revision }],
        "mutation_id": mutation_id,
        "retry": retry
    })
}

pub(super) fn rpc_proxy_discovery_url(
    endpoint: &reqwest::Url,
    proxy_role: Option<&str>,
) -> reqwest::Url {
    let mut url = endpoint.clone();
    url.set_path("/api/v4/discover_proxies");
    let mut query = url.query_pairs_mut();
    query.clear().append_pair("type", "rpc");
    if let Some(proxy_role) = proxy_role {
        query.append_pair("role", proxy_role);
    }
    drop(query);
    url
}

pub(super) fn normalize_rpc_proxy_roles(mut roles: Vec<String>) -> Vec<String> {
    roles.retain(|role| !role.is_empty());
    roles.sort_unstable();
    roles.dedup();
    roles
}

pub(super) fn dynamic_table_attributes(
    schema: &Value,
    atomicity: YTsaurusAtomicity,
    primary_medium: &str,
    custom_attributes: &BTreeMap<String, Value>,
    tablet_cell_bundle: Option<&str>,
    dynamic_store_overflow_threshold: f64,
    table_ttl_ms: Option<u64>,
) -> anyhow::Result<Value> {
    let mut attributes = serde_json::json!({
        "schema": schema,
        "dynamic": true,
        "optimize_for": "lookup",
        "atomicity": atomicity.as_str(),
        "primary_medium": primary_medium,
        "mount_config": {
            "dynamic_store_overflow_threshold": dynamic_store_overflow_threshold,
        },
    });
    if let Some(bundle) = tablet_cell_bundle {
        attributes["tablet_cell_bundle"] = Value::String(bundle.to_owned());
    }
    if let Some(ttl_ms) = table_ttl_ms {
        attributes["min_data_versions"] = Value::from(0);
        attributes["max_data_versions"] = Value::from(1);
        attributes["min_data_ttl"] = Value::from(0);
        attributes["max_data_ttl"] = Value::from(ttl_ms);
        attributes["auto_compaction_period"] = Value::from(ttl_ms);
        attributes["merge_rows_on_flush"] = Value::Bool(true);
    }
    extend_attributes(&mut attributes, custom_attributes)?;
    Ok(attributes)
}

pub(super) fn uniform_reshard_parameters(path: &str, tablet_count: usize) -> anyhow::Result<Value> {
    anyhow::ensure!(!path.is_empty(), "YTsaurus reshard path must not be empty");
    anyhow::ensure!(
        (1..=10_000).contains(&tablet_count),
        "YTsaurus tablet count must be between 1 and 10000"
    );
    Ok(serde_json::json!({
        "path": path,
        "tablet_count": tablet_count,
        "uniform": true,
    }))
}

pub(super) fn dynamic_conversion_attributes(
    atomicity: YTsaurusAtomicity,
    primary_medium: &str,
    custom_attributes: &BTreeMap<String, Value>,
    tablet_cell_bundle: Option<&str>,
    dynamic_store_overflow_threshold: f64,
    table_ttl_ms: Option<u64>,
) -> anyhow::Result<Value> {
    let mut attributes = dynamic_table_attributes(
        &Value::Null,
        atomicity,
        primary_medium,
        custom_attributes,
        tablet_cell_bundle,
        dynamic_store_overflow_threshold,
        table_ttl_ms,
    )?;
    let object = attributes
        .as_object_mut()
        .ok_or_else(|| anyhow::anyhow!("dynamic table attributes must be an object"))?;
    object.remove("schema");
    object.remove("dynamic");
    Ok(attributes)
}

pub(super) fn sort_operation_parameters(
    source_path: &str,
    destination_path: &str,
    sort_by: &[Value],
    mutation_id: &str,
    operation_pool: Option<&str>,
) -> Value {
    let mut spec = serde_json::json!({
        "input_table_paths": [source_path],
        "output_table_path": destination_path,
        "sort_by": sort_by,
        "schema_inference_mode": "from_output",
        "max_failed_job_count": 1,
    });
    if let Some(pool) = operation_pool {
        spec["pool"] = Value::String(pool.to_owned());
    }
    serde_json::json!({
        "operation_type": "sort",
        "mutation_id": mutation_id,
        "spec": spec,
    })
}

pub(super) fn json_header_value(value: &Value) -> anyhow::Result<String> {
    use std::fmt::Write as _;

    let json = serde_json::to_string(value)?;
    let mut header = String::with_capacity(json.len());
    for character in json.chars() {
        if matches!(character, ' '..='~') {
            header.push(character);
            continue;
        }
        let codepoint = character as u32;
        if codepoint <= 0xffff {
            write!(header, "\\u{codepoint:04x}")?;
            continue;
        }
        let adjusted = codepoint - 0x1_0000;
        let high = 0xd800 + (adjusted >> 10);
        let low = 0xdc00 + (adjusted & 0x3ff);
        write!(header, "\\u{high:04x}\\u{low:04x}")?;
    }
    Ok(header)
}

pub(super) fn yson_header_value(value: &Value) -> anyhow::Result<String> {
    fn write_value(output: &mut String, value: &Value) -> anyhow::Result<()> {
        match value {
            Value::Null => output.push('#'),
            Value::Bool(value) => output.push_str(if *value { "%true" } else { "%false" }),
            Value::Number(value) => output.push_str(&value.to_string()),
            Value::String(value) => output.push_str(&serde_json::to_string(value)?),
            Value::Array(values) => {
                output.push('[');
                for value in values {
                    write_value(output, value)?;
                    output.push(';');
                }
                output.push(']');
            }
            Value::Object(object)
                if object.contains_key("$value") && object.contains_key("$attributes") =>
            {
                let attributes = object
                    .get("$attributes")
                    .and_then(Value::as_object)
                    .ok_or_else(|| anyhow::anyhow!("YSON $attributes must be an object"))?;
                let mut attributes = attributes.iter().collect::<Vec<_>>();
                attributes.sort_unstable_by_key(|(key, _)| key.as_str());
                output.push('<');
                for (key, value) in attributes {
                    output.push_str(&serde_json::to_string(key)?);
                    output.push('=');
                    write_value(output, value)?;
                    output.push(';');
                }
                output.push('>');
                write_value(
                    output,
                    object
                        .get("$value")
                        .ok_or_else(|| anyhow::anyhow!("YSON envelope has no $value"))?,
                )?;
            }
            Value::Object(object) if object.len() == 1 && object.contains_key("$uint64") => {
                let value = object
                    .get("$uint64")
                    .and_then(Value::as_str)
                    .ok_or_else(|| anyhow::anyhow!("YSON $uint64 value must be a string"))?;
                value
                    .parse::<u64>()
                    .map_err(|error| anyhow::anyhow!("invalid YSON uint64: {error}"))?;
                output.push_str(value);
                output.push('u');
            }
            Value::Object(object) if object.len() == 1 && object.contains_key("$double") => {
                let value = object
                    .get("$double")
                    .and_then(Value::as_str)
                    .filter(|value| matches!(*value, "%nan" | "%inf" | "%-inf"))
                    .ok_or_else(|| anyhow::anyhow!("invalid YSON $double value"))?;
                output.push_str(value);
            }
            Value::Object(object) => {
                let mut entries = object.iter().collect::<Vec<_>>();
                entries.sort_unstable_by_key(|(key, _)| key.as_str());
                output.push('{');
                for (key, value) in entries {
                    output.push_str(&serde_json::to_string(key)?);
                    output.push('=');
                    write_value(output, value)?;
                    output.push(';');
                }
                output.push('}');
            }
        }
        Ok(())
    }

    let mut output = String::new();
    write_value(&mut output, value)?;
    Ok(output)
}

pub(super) fn is_loopback_host(host: &str) -> bool {
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|address| address.is_loopback())
}

pub(super) fn suggestion_directory(query: &str) -> anyhow::Result<String> {
    let query = query.trim();
    let directory = if matches!(query, "" | "/" | "//") {
        "/"
    } else {
        query.trim_end_matches('/')
    };
    anyhow::ensure!(
        directory == "/" || (directory.starts_with("//") && directory.len() > 2),
        "YTsaurus path suggestion query must be empty or start with '//'"
    );
    anyhow::ensure!(
        !directory.contains('<') && !directory.contains('>') && !directory.contains('\0'),
        "YTsaurus path suggestion query must not contain rich-path attributes or NUL"
    );
    Ok(directory.to_owned())
}

pub(super) fn table_path_suggestions(directory: &str, nodes: Vec<ListedNode>) -> Vec<String> {
    let prefix = suggestion_prefix(directory);
    let mut suggestions = nodes
        .into_iter()
        .filter_map(|node| match node {
            ListedNode::WithAttributes { name, attributes } if attributes.node_type == "table" => {
                Some(format!("{prefix}{name}"))
            }
            ListedNode::WithAttributes { name, attributes }
                if matches!(
                    attributes.node_type.as_str(),
                    "map_node" | "portal_entrance" | "rootstock"
                ) =>
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

fn suggestion_prefix(directory: &str) -> String {
    if directory == "/" {
        "//".to_owned()
    } else {
        format!("{directory}/")
    }
}

pub(super) fn resolved_link_suggestion(path: String, node_type: &str) -> Option<String> {
    match node_type {
        "table" => Some(path),
        "map_node" | "portal_entrance" | "rootstock" => Some(format!("{path}/")),
        _ => None,
    }
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
