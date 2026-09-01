use std::path::PathBuf;

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, SslOpts};
use schemars::JsonSchema;
use serde::Deserialize;

#[derive(Clone, Deserialize)]
pub struct MySqlConnectionCheckConfig {
    #[serde(default)]
    pub host: String,

    #[serde(default = "default_mysql_port")]
    pub port: u16,

    #[serde(default)]
    pub database: String,

    #[serde(default)]
    pub username: String,

    #[serde(default)]
    pub password: String,

    #[serde(default)]
    pub trusted_plaintext: bool,

    #[serde(default)]
    pub tls_ca_file: Option<String>,
}

impl MySqlConnectionCheckConfig {
    #[must_use]
    pub const fn credentials_complete(&self) -> bool {
        !self.database.is_empty() && !self.username.is_empty()
    }

    #[must_use]
    pub fn connection(&self) -> MySqlConnectionConfig {
        MySqlConnectionConfig {
            host: self.host.clone(),
            port: self.port,
            database: self.database.clone(),
            username: self.username.clone(),
            password: self.password.clone(),
            trusted_plaintext: self.trusted_plaintext,
            tls_ca_file: self.tls_ca_file.clone(),
        }
    }
}

const fn default_mysql_port() -> u16 {
    3306
}

pub const MAX_IDENTIFIER_CHARS: usize = 64;

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MySqlConnectionConfig {
    pub host: String,

    pub port: u16,

    pub database: String,

    pub username: String,

    #[schemars(extend("x-ui" = { "widget": "password" }))]
    pub password: String,

    pub trusted_plaintext: bool,

    #[serde(default)]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub tls_ca_file: Option<String>,
}

impl MySqlConnectionConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        crate::connectors::address::validate_host("mysql.host", &self.host)?;
        crate::connectors::address::validate_port("mysql.port", self.port)?;
        validate_identifier("database", &self.database)?;
        anyhow::ensure!(!self.username.is_empty(), "mysql.username must not be empty");
        if let Some(path) = &self.tls_ca_file {
            anyhow::ensure!(!path.trim().is_empty(), "mysql.tls_ca_file must not be empty");
        }
        Ok(())
    }
}

pub async fn connect(config: &MySqlConnectionConfig) -> anyhow::Result<Conn> {
    config.validate()?;
    let mut builder = OptsBuilder::default()
        .ip_or_hostname(config.host.clone())
        .tcp_port(config.port)
        .db_name(Some(config.database.clone()))
        .user(Some(config.username.clone()))
        .pass(Some(config.password.clone()))
        .prefer_socket(false)
        .tcp_nodelay(true);
    if !config.trusted_plaintext {
        let mut ssl = SslOpts::default();
        if let Some(path) = &config.tls_ca_file {
            ssl = ssl.with_root_certs(vec![PathBuf::from(path).into()]);
        }
        builder = builder.ssl_opts(Some(ssl));
    }
    Ok(Conn::new(builder).await?)
}

pub async fn check_connection(config: &MySqlConnectionConfig) -> anyhow::Result<()> {
    let mut connection = connect(config).await?;
    connection.query_drop("SELECT 1").await?;
    connection.disconnect().await?;
    Ok(())
}

pub async fn check_network_connection(config: &MySqlConnectionCheckConfig) -> anyhow::Result<()> {
    crate::connectors::address::validate_host("mysql.host", &config.host)?;
    crate::connectors::address::validate_port("mysql.port", config.port)?;
    tokio::net::TcpStream::connect((config.host.as_str(), config.port)).await?;
    Ok(())
}

pub fn validate_identifier(kind: &str, value: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        !value.is_empty()
            && value.chars().count() <= MAX_IDENTIFIER_CHARS
            && !value.contains('\0'),
        "invalid MySQL {kind} '{value}'; expected a non-empty identifier of at most {MAX_IDENTIFIER_CHARS} characters without NUL"
    );
    Ok(())
}

#[must_use]
pub fn quote_identifier(value: &str) -> String {
    format!("`{}`", value.replace('`', "``"))
}
