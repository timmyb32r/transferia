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
    None,

    Basic {
        username: String,

        #[schemars(extend("x-ui" = { "widget": "password" }))]
        password: String,
    },

    Bearer {
        #[schemars(extend("x-ui" = { "widget": "password" }))]
        token: String,
    },
}

#[derive(Clone, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaRegistryConnection {
    #[schemars(title = "Registry URLs", extend("x-ui" = { "initial_items": 1 }))]
    pub urls: Vec<String>,

    pub subject: String,

    pub format: SchemaFormat,

    pub request_timeout_ms: u64,

    #[serde(default = "default_auth")]
    pub auth: SchemaRegistryAuth,
}

const fn default_auth() -> SchemaRegistryAuth {
    SchemaRegistryAuth::None
}

impl SchemaRegistryConnection {
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.urls.is_empty(),
            "schema_registry.urls must not be empty"
        );
        for url in &self.urls {
            let parsed = reqwest::Url::parse(url)
                .map_err(|error| anyhow::anyhow!("invalid Schema Registry URL '{url}': {error}"))?;
            anyhow::ensure!(
                matches!(parsed.scheme(), "http" | "https"),
                "Schema Registry URL '{url}' must use http or https"
            );
            anyhow::ensure!(
                parsed.username().is_empty() && parsed.password().is_none(),
                "Schema Registry URL must not contain credentials; use schema_registry.auth"
            );
        }
        anyhow::ensure!(
            !self.subject.is_empty(),
            "schema_registry.subject must not be empty"
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
        Ok(())
    }
}
