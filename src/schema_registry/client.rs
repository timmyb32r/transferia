use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::Context as _;
use reqwest::{Client, RequestBuilder, Url};
use serde::Deserialize;
use tokio::sync::RwLock;

use super::{SchemaFormat, SchemaRegistryAuth, SchemaRegistryConnection};

#[derive(Clone)]
pub struct RegistryClient {
    client: Client,
    urls: Arc<[Url]>,
    subject: String,
    format: SchemaFormat,
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
        let urls = config
            .urls
            .iter()
            .map(|url| Url::parse(url))
            .collect::<Result<Vec<_>, _>>()?;
        let client = Client::builder()
            .timeout(Duration::from_millis(config.request_timeout_ms))
            .build()
            .context("failed to build Schema Registry HTTP client")?;
        Ok(Self {
            client,
            urls: urls.into(),
            subject: config.subject.clone(),
            format: config.format,
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
        let response = self
            .get(&["schemas", "ids", &id.to_string()], true)
            .await
            .with_context(|| {
                format!(
                    "failed to fetch Schema Registry schema id {id} from subject '{}'",
                    self.subject
                )
            })?;
        validate_format(self.format, response.schema_type.as_deref())?;
        let schema = RegistrySchema {
            id,
            definition: response.schema,
            format: self.format,
        };
        self.schemas.write().await.insert(id, schema.clone());
        Ok(schema)
    }

    pub async fn latest_schema(&self) -> anyhow::Result<RegistrySchema> {
        let response = self
            .get(&["subjects", &self.subject, "versions", "latest"], false)
            .await
            .with_context(|| {
                format!(
                    "failed to fetch latest Schema Registry schema for subject '{}'",
                    self.subject
                )
            })?;
        validate_format(self.format, response.schema_type.as_deref())?;
        Ok(RegistrySchema {
            id: response
                .id
                .ok_or_else(|| anyhow::anyhow!("Schema Registry response has no schema id"))?,
            definition: response.schema,
            format: self.format,
        })
    }

    async fn get(&self, path: &[&str], include_subject: bool) -> anyhow::Result<RegistryResponse> {
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
            if include_subject {
                url.query_pairs_mut().append_pair("subject", &self.subject);
            }
            if self.format == SchemaFormat::Protobuf {
                url.query_pairs_mut().append_pair("format", "serialized");
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

    async fn send(&self, request: RequestBuilder) -> anyhow::Result<RegistryResponse> {
        let request = match &self.auth {
            SchemaRegistryAuth::None => request,
            SchemaRegistryAuth::Basic { username, password } => {
                request.basic_auth(username, Some(password))
            }
            SchemaRegistryAuth::Bearer { token } => request.bearer_auth(token),
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
