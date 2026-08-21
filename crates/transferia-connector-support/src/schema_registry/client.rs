use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::Context as _;
use reqwest::Url;
use serde::Deserialize;
use tokio::sync::RwLock;

use super::{SchemaFormat, SchemaRegistryAuth, SchemaRegistryConnection};
use crate::outbound_http::{OutboundHttpClient, OutboundHttpRequest};

#[derive(Clone)]
pub struct RegistryClient {
    client: OutboundHttpClient,
    urls: Arc<[Url]>,
    auth: SchemaRegistryAuth,
    schemas: Arc<RwLock<HashMap<i32, RegistrySchema>>>,
}

#[derive(Clone)]
pub struct RegistrySchema {
    pub id: i32,
    pub definition: String,
    pub format: SchemaFormat,
}

#[derive(Deserialize)]
struct RegistryResponse {
    id: Option<i32>,
    schema: String,
    #[serde(rename = "schemaType")]
    schema_type: Option<String>,
}

impl RegistryClient {
    pub fn new(config: &SchemaRegistryConnection) -> anyhow::Result<Self> {
        config.validate()?;
        let urls = vec![Url::parse(&config.url)?];
        let certificates = config
            .ca_certificate
            .as_deref()
            .map(|certificate| reqwest::Certificate::from_pem(certificate.as_bytes()))
            .transpose()?
            .into_iter();
        let client = OutboundHttpClient::new(
            Duration::from_millis(config.request_timeout_ms),
            certificates,
        )
        .context("failed to build Schema Registry HTTP client")?;
        Ok(Self {
            client,
            urls: urls.into(),
            auth: config.auth.clone(),
            schemas: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    pub async fn schema_by_id(&self, id: i32) -> anyhow::Result<RegistrySchema> {
        anyhow::ensure!(id >= 0, "Schema Registry schema id must be nonnegative");
        let cached = self.schemas.read().await.get(&id).cloned();
        if let Some(schema) = cached {
            return Ok(schema);
        }
        let mut response = self
            .get(&["schemas", "ids", &id.to_string()], &[])
            .await
            .with_context(|| format!("failed to fetch Schema Registry schema id {id}"))?;
        let format = response_format(response.schema_type.as_deref())?;
        if format == SchemaFormat::Protobuf {
            response = self
                .get(
                    &["schemas", "ids", &id.to_string()],
                    &[("format", "serialized")],
                )
                .await
                .with_context(|| {
                    format!("failed to fetch serialized Schema Registry protobuf schema id {id}")
                })?;
            validate_format(format, response.schema_type.as_deref())?;
        }
        let schema = RegistrySchema {
            id,
            definition: response.schema,
            format,
        };
        self.schemas.write().await.insert(id, schema.clone());
        Ok(schema)
    }

    pub async fn latest_schema(
        &self,
        subject: &str,
        format: SchemaFormat,
    ) -> anyhow::Result<RegistrySchema> {
        let response = self
            .get(&["subjects", subject, "versions", "latest"], &[])
            .await
            .with_context(|| {
                format!("failed to fetch latest Schema Registry schema for subject '{subject}'")
            })?;
        validate_format(format, response.schema_type.as_deref())?;
        Ok(RegistrySchema {
            id: response
                .id
                .ok_or_else(|| anyhow::anyhow!("Schema Registry response has no schema id"))?,
            definition: response.schema,
            format,
        })
    }

    async fn get(&self, path: &[&str], query: &[(&str, &str)]) -> anyhow::Result<RegistryResponse> {
        let mut failures = Vec::with_capacity(self.urls.len());
        for base_url in self.urls.iter() {
            let mut url = base_url.clone();
            {
                let mut segments = url.path_segments_mut().map_err(|()| {
                    anyhow::anyhow!("Schema Registry base URL cannot be a base URL")
                })?;
                segments.pop_if_empty();
                for segment in path {
                    segments.push(segment);
                }
            }
            for (key, value) in query {
                url.query_pairs_mut().append_pair(key, value);
            }
            match self.send(self.client.get(url)).await {
                Ok(response) => return Ok(response),
                Err(error) => failures.push(error.to_string()),
            }
        }
        anyhow::bail!(
            "every configured Schema Registry endpoint failed: {}",
            failures.join("; ")
        )
    }

    async fn send(&self, request: OutboundHttpRequest) -> anyhow::Result<RegistryResponse> {
        let request = match &self.auth {
            SchemaRegistryAuth::None => request,
            SchemaRegistryAuth::Basic { username, password } => {
                request.configure(|request| request.basic_auth(username, Some(password)))
            }
            SchemaRegistryAuth::Bearer { token } => {
                request.configure(|request| request.bearer_auth(token))
            }
        };
        request
            .send()
            .await
            .context("Schema Registry request failed")?
            .error_for_status()
            .context("Schema Registry returned an error status")?
            .json()
            .await
            .context("Schema Registry returned an invalid JSON response")
    }
}

fn validate_format(expected: SchemaFormat, actual: Option<&str>) -> anyhow::Result<()> {
    let actual = actual.unwrap_or("AVRO");
    anyhow::ensure!(
        actual.eq_ignore_ascii_case(expected.registry_name()),
        "Schema Registry returned schema type {actual}, expected {}",
        expected.registry_name()
    );
    Ok(())
}

fn response_format(actual: Option<&str>) -> anyhow::Result<SchemaFormat> {
    match actual.unwrap_or("AVRO").to_ascii_uppercase().as_str() {
        "AVRO" => Ok(SchemaFormat::Avro),
        "JSON" | "JSONSCHEMA" => Ok(SchemaFormat::JsonSchema),
        "PROTOBUF" => Ok(SchemaFormat::Protobuf),
        actual => anyhow::bail!("Schema Registry returned unsupported schema type {actual}"),
    }
}
