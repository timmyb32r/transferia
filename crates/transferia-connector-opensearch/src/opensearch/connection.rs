use std::collections::HashSet;
use std::fmt;
use std::io::BufReader;
use std::time::Duration;

use reqwest::Certificate;
use schemars::JsonSchema;
use serde::Deserialize;
use transferia_connector_support::outbound_http::{NetworkPolicy, OutboundHttpClient};

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum OpenSearchAuth {
    Basic {
        username: String,

        #[schemars(extend("x-ui" = { "widget": "password" }))]
        password: String,
    },

    Anonymous,
}

#[derive(Clone, Deserialize)]
pub struct OpenSearchConnectionCheckConfig {
    #[serde(default)]
    pub hosts: Vec<String>,

    #[serde(default = "default_opensearch_port")]
    pub port: u16,

    #[serde(default)]
    pub trusted_plaintext: bool,

    #[serde(default)]
    pub tls_ca_file: Option<String>,

    #[serde(default)]
    pub auth: Option<OpenSearchAuth>,

    #[serde(default = "default_request_timeout_ms")]
    pub request_timeout_ms: u64,

    #[serde(default = "default_max_response_bytes")]
    pub max_response_bytes: usize,
}

impl OpenSearchConnectionCheckConfig {
    #[must_use]
    pub fn credentials_complete(&self) -> bool {
        match &self.auth {
            Some(OpenSearchAuth::Basic { username, .. }) => !username.is_empty(),
            Some(OpenSearchAuth::Anonymous) => true,
            None => false,
        }
    }

    #[must_use]
    pub fn connection(&self) -> Option<OpenSearchConnectionConfig> {
        Some(OpenSearchConnectionConfig {
            hosts: self.hosts.clone(),
            port: self.port,
            trusted_plaintext: self.trusted_plaintext,
            tls_ca_file: self.tls_ca_file.clone(),
            auth: self.auth.clone()?,
            request_timeout_ms: self.request_timeout_ms,
            max_response_bytes: self.max_response_bytes,
        })
    }
}

impl OpenSearchAuth {
    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        if let Self::Basic { username, .. } = self {
            anyhow::ensure!(
                !username.is_empty(),
                "opensearch.auth.username must not be empty"
            );
        }
        Ok(())
    }

    pub(crate) fn basic(&self) -> Option<(&str, &str)> {
        match self {
            Self::Basic { username, password } => Some((username, password)),
            Self::Anonymous => None,
        }
    }
}

impl fmt::Debug for OpenSearchAuth {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Basic { username, .. } => formatter
                .debug_struct("Basic")
                .field("username", username)
                .field("password", &"<redacted>")
                .finish(),
            Self::Anonymous => formatter.write_str("Anonymous"),
        }
    }
}

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct OpenSearchConnectionConfig {
    #[schemars(extend("x-ui" = { "widget": "compact_array", "item_label": "host" }))]
    pub hosts: Vec<String>,

    pub port: u16,

    /// Explicit trust decision for unencrypted HTTP transport.
    pub trusted_plaintext: bool,

    #[serde(default)]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub tls_ca_file: Option<String>,

    pub auth: OpenSearchAuth,

    #[serde(default = "default_request_timeout_ms")]
    #[schemars(
        title = "Request timeout, ms",
        range(min = 1),
        extend("x-ui" = { "section": "advanced" })
    )]
    pub request_timeout_ms: u64,

    #[serde(default = "default_max_response_bytes")]
    #[schemars(
        title = "Maximum response bytes",
        description = "Maximum accepted uncompressed body for one OpenSearch response",
        range(min = 1),
        extend("x-ui" = { "section": "advanced", "widget": "byte_size" })
    )]
    pub max_response_bytes: usize,
}

impl OpenSearchConnectionConfig {
    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(!self.hosts.is_empty(), "opensearch.hosts must not be empty");
        let mut hosts = HashSet::with_capacity(self.hosts.len());
        for host in &self.hosts {
            transferia_connector_support::address::validate_host("opensearch.hosts", host)?;
            anyhow::ensure!(hosts.insert(host), "opensearch.hosts repeats host '{host}'");
        }
        transferia_connector_support::address::validate_port("opensearch.port", self.port)?;
        if let Some(path) = &self.tls_ca_file {
            anyhow::ensure!(
                !path.trim().is_empty(),
                "opensearch.tls_ca_file must not be empty"
            );
        }
        self.auth.validate()?;
        anyhow::ensure!(
            self.request_timeout_ms > 0,
            "opensearch.request_timeout_ms must be positive"
        );
        anyhow::ensure!(
            self.max_response_bytes > 0,
            "opensearch.max_response_bytes must be positive"
        );
        Ok(())
    }

    pub(crate) const fn request_timeout(&self) -> Duration {
        Duration::from_millis(self.request_timeout_ms)
    }

    pub(crate) fn http_client(&self) -> anyhow::Result<OutboundHttpClient> {
        let certificates = self.root_certificates()?;
        Ok(OutboundHttpClient::new(
            self.request_timeout(),
            certificates,
            NetworkPolicy::AllowPrivateNetworks,
        )?)
    }

    fn root_certificates(&self) -> anyhow::Result<Vec<Certificate>> {
        let Some(path) = &self.tls_ca_file else {
            return Ok(Vec::new());
        };
        let bytes = std::fs::read(path)
            .map_err(|error| anyhow::anyhow!("cannot read OpenSearch TLS CA file: {error}"))?;
        let mut reader = BufReader::new(bytes.as_slice());
        let certificates = rustls_pemfile::certs(&mut reader).collect::<Result<Vec<_>, _>>()?;
        anyhow::ensure!(
            !certificates.is_empty(),
            "OpenSearch TLS CA file contains no certificates"
        );
        certificates
            .into_iter()
            .map(|certificate| Ok(Certificate::from_der(certificate.as_ref())?))
            .collect()
    }
}

impl fmt::Debug for OpenSearchConnectionConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpenSearchConnectionConfig")
            .field("hosts", &self.hosts)
            .field("port", &self.port)
            .field("trusted_plaintext", &self.trusted_plaintext)
            .field("tls_ca_file", &self.tls_ca_file)
            .field("auth", &self.auth)
            .field("request_timeout_ms", &self.request_timeout_ms)
            .field("max_response_bytes", &self.max_response_bytes)
            .finish()
    }
}

pub fn validate_index_name(name: &str) -> anyhow::Result<()> {
    let bytes = name.as_bytes();
    anyhow::ensure!(!bytes.is_empty(), "OpenSearch index name must not be empty");
    anyhow::ensure!(
        bytes.len() <= 255,
        "OpenSearch index name exceeds the 255-byte limit"
    );
    anyhow::ensure!(
        name != "." && name != "..",
        "invalid OpenSearch index name '{name}'"
    );
    anyhow::ensure!(
        !name.starts_with(['-', '_', '+']),
        "OpenSearch index name must not start with '-', '_' or '+'"
    );
    anyhow::ensure!(
        !name.chars().any(char::is_uppercase),
        "OpenSearch index name must be lowercase"
    );
    anyhow::ensure!(
        !name.chars().any(|character| {
            matches!(
                character,
                '\\' | '/' | '*' | '?' | '"' | '<' | '>' | '|' | ' ' | ',' | '#' | ':'
            ) || character.is_control()
        }),
        "OpenSearch index name contains a forbidden character"
    );
    Ok(())
}

const fn default_request_timeout_ms() -> u64 {
    30_000
}

const fn default_opensearch_port() -> u16 {
    9200
}

const fn default_max_response_bytes() -> usize {
    64 * 1024 * 1024
}
