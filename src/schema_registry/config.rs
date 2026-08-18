use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaFormat {
    Avro,

    #[serde(alias = "json")]
    JsonSchema,

    Protobuf,
}

impl SchemaFormat {
    #[must_use]
    pub const fn registry_name(self) -> &'static str {
        match self {
            Self::Avro => "AVRO",
            Self::JsonSchema => "JSON",
            Self::Protobuf => "PROTOBUF",
        }
    }
}

#[derive(Clone, Deserialize, JsonSchema, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum SchemaRegistryAuth {
    #[schemars(title = "No authentication")]
    None,

    #[schemars(title = "Username and password")]
    Basic {
        username: String,

        #[schemars(extend("x-ui" = { "widget": "password" }))]
        password: String,
    },

    #[schemars(title = "Bearer token")]
    Bearer {
        #[schemars(extend("x-ui" = { "widget": "password" }))]
        token: String,
    },
}

#[derive(Clone, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaRegistryConnection {
    #[schemars(title = "Registry URL")]
    pub url: String,

    #[schemars(title = "Request timeout (ms)", extend("x-ui" = { "section": "advanced" }))]
    pub request_timeout_ms: u64,

    #[serde(default = "default_auth")]
    #[schemars(title = "Authentication")]
    pub auth: SchemaRegistryAuth,

    #[serde(default)]
    #[schemars(title = "CA certificate (PEM)", extend("x-ui" = { "section": "advanced" }))]
    pub ca_certificate: Option<String>,
}

const fn default_auth() -> SchemaRegistryAuth {
    SchemaRegistryAuth::None
}

impl SchemaRegistryConnection {
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.url.trim() == self.url && !self.url.is_empty(),
            "schema_registry.url must be nonempty and must not contain leading or trailing whitespace"
        );
        let parsed = reqwest::Url::parse(&self.url).map_err(|error| {
            anyhow::anyhow!("invalid Schema Registry URL '{}': {error}", self.url)
        })?;
        anyhow::ensure!(
            matches!(parsed.scheme(), "http" | "https"),
            "Schema Registry URL '{}' must use http or https",
            self.url
        );
        anyhow::ensure!(
            parsed.username().is_empty() && parsed.password().is_none(),
            "Schema Registry URL must not contain credentials; use schema_registry.auth"
        );
        anyhow::ensure!(
            self.request_timeout_ms > 0,
            "schema_registry.request_timeout_ms must be greater than zero"
        );
        match &self.auth {
            SchemaRegistryAuth::None => {}
            SchemaRegistryAuth::Basic { username, password } => {
                anyhow::ensure!(
                    !username.is_empty(),
                    "schema_registry.auth.username must not be empty"
                );
                anyhow::ensure!(
                    !password.is_empty(),
                    "schema_registry.auth.password must not be empty"
                );
            }
            SchemaRegistryAuth::Bearer { token } => anyhow::ensure!(
                !token.is_empty(),
                "schema_registry.auth.token must not be empty"
            ),
        }
        if let Some(certificate) = &self.ca_certificate {
            anyhow::ensure!(
                !certificate.is_empty(),
                "schema_registry.ca_certificate must not be empty when configured"
            );
            reqwest::Certificate::from_pem(certificate.as_bytes()).map_err(|error| {
                anyhow::anyhow!("invalid Schema Registry CA certificate: {error}")
            })?;
        }
        Ok(())
    }
}
