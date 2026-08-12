use serde_json::Value;

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
    endpoint: String,
    token: Option<String>,
    client: reqwest::Client,
}

impl YTsaurusClient {
    pub fn new(config: &YTsaurusConnectionConfig) -> anyhow::Result<Self> {
        config.validate()?;
        Ok(Self {
            endpoint: config.endpoint.trim_end_matches('/').to_owned(),
            token: config.token.clone(),
            client: reqwest::Client::builder()
                .timeout(config.timeout())
                .redirect(reqwest::redirect::Policy::limited(5))
                .build()?,
        })
    }

    fn request(&self, method: reqwest::Method, command: &str) -> reqwest::RequestBuilder {
        let request = self
            .client
            .request(method, format!("{}/api/v3/{command}", self.endpoint));
        if let Some(token) = &self.token {
            request.header(reqwest::header::AUTHORIZATION, format!("OAuth {token}"))
        } else {
            request
        }
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
        let response = self
            .request(reqwest::Method::GET, "get")
            .header("X-YT-Parameters", serde_json::to_string(&parameters)?)
            .header(reqwest::header::ACCEPT, "application/json")
            .send()
            .await?;
        Ok(Self::checked(response).await?.json().await?)
    }

    pub async fn read_arrow(&self, path: &str) -> anyhow::Result<reqwest::Response> {
        let parameters = serde_json::json!({ "path": path });
        let response = self
            .request(reqwest::Method::GET, "read_table")
            .header("X-YT-Parameters", serde_json::to_string(&parameters)?)
            .header("X-YT-Output-Format", "\"arrow\"")
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
        let response = self
            .request(reqwest::Method::PUT, "write_table")
            .header("X-YT-Parameters", serde_json::to_string(&parameters)?)
            .header("X-YT-Input-Format", format!("\"{format}\""))
            .body(payload)
            .send()
            .await?;
        Self::checked(response).await?;
        Ok(())
    }

    pub async fn remove_table(&self, path: &str) -> anyhow::Result<()> {
        let parameters = serde_json::json!({ "path": path, "force": true });
        let response = self
            .request(reqwest::Method::POST, "remove")
            .header("X-YT-Parameters", serde_json::to_string(&parameters)?)
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
        let response = self
            .request(reqwest::Method::POST, "create")
            .header("X-YT-Parameters", serde_json::to_string(&parameters)?)
            .send()
            .await?;
        Self::checked(response).await?;
        Ok(())
    }
}

pub fn runtime_http_failure(error: anyhow::Error) -> anyhow::Error {
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
        crate::pipeline::PipelineFailure::fatal(error).into()
    } else {
        error
    }
}
