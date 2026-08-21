use serde_json::Value;
use transferia_connector_support::outbound_http::{OutboundHttpClient, OutboundHttpRequest};

use super::config::YTsaurusConnectionConfig;

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
}

impl YTsaurusClient {
    pub fn new(config: &YTsaurusConnectionConfig) -> anyhow::Result<Self> {
        config.validate()?;
        Ok(Self {
            endpoint: config.endpoint().parse()?,
            token: config.auth.load_token()?,
            client: OutboundHttpClient::new(config.timeout(), [])?,
        })
    }

    fn request(
        &self,
        method: reqwest::Method,
        command: &str,
    ) -> anyhow::Result<OutboundHttpRequest> {
        let mut url = self.endpoint.clone();
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

    pub async fn read_arrow(&self, path: &str) -> anyhow::Result<reqwest::Response> {
        let parameters = serde_json::json!({ "path": path });
        let parameters = serde_json::to_string(&parameters)?;
        let response = self
            .request(reqwest::Method::GET, "read_table")?
            .configure(|request| {
                request
                    .header("X-YT-Parameters", parameters)
                    .header("X-YT-Output-Format", "\"arrow\"")
            })
            .send()
            .await?;
        Self::checked(response).await
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
            .request(reqwest::Method::PUT, "write_table")?
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
