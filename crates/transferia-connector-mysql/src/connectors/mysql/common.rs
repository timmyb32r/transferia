use std::path::PathBuf;

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, SslOpts};
use schemars::JsonSchema;
use serde::{Deserialize, Deserializer};

#[derive(Clone, Deserialize)]
pub struct MySqlConnectionCheckConfig {
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub host: String,

    #[serde(default = "default_mysql_port")]
    pub port: u16,

    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub database: String,

    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub username: String,

    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub password: String,

    #[serde(default)]
    pub trusted_plaintext: bool,

    #[serde(default)]
    pub tls_ca_file: Option<String>,
}

fn deserialize_null_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de> + Default,
{
    Ok(Option::<T>::deserialize(deserializer)?.unwrap_or_default())
}

impl MySqlConnectionCheckConfig {
    #[must_use]
    pub const fn credentials_complete(&self) -> bool {
        !self.username.is_empty()
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
pub const MYSQL_CLIENT_PACKET_MIN_BYTES: usize = 1_024;
pub const MYSQL_CLIENT_PACKET_MAX_BYTES: usize = 1_073_741_824;

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MySqlConnectionConfig {
    pub host: String,

    pub port: u16,

    #[serde(default)]
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
        if !self.database.is_empty() {
            validate_identifier("database", &self.database)?;
        }
        anyhow::ensure!(
            !self.username.is_empty(),
            "mysql.username must not be empty"
        );
        if let Some(path) = &self.tls_ca_file {
            anyhow::ensure!(
                !path.trim().is_empty(),
                "mysql.tls_ca_file must not be empty"
            );
        }
        Ok(())
    }
}

pub async fn connect(config: &MySqlConnectionConfig) -> anyhow::Result<Conn> {
    connect_with_packet_limit(config, None).await
}

pub async fn connect_with_max_allowed_packet(
    config: &MySqlConnectionConfig,
    max_allowed_packet: usize,
) -> anyhow::Result<Conn> {
    validate_mysql_client_packet_limit(max_allowed_packet)?;
    connect_with_packet_limit(config, Some(max_allowed_packet)).await
}

pub fn validate_mysql_client_packet_limit(max_allowed_packet: usize) -> anyhow::Result<()> {
    anyhow::ensure!(
        (MYSQL_CLIENT_PACKET_MIN_BYTES..=MYSQL_CLIENT_PACKET_MAX_BYTES)
            .contains(&max_allowed_packet),
        "MySQL client max_allowed_packet must be in {MYSQL_CLIENT_PACKET_MIN_BYTES}..={MYSQL_CLIENT_PACKET_MAX_BYTES} bytes"
    );
    Ok(())
}

async fn connect_with_packet_limit(
    config: &MySqlConnectionConfig,
    max_allowed_packet: Option<usize>,
) -> anyhow::Result<Conn> {
    config.validate()?;
    let mut builder = OptsBuilder::default()
        .ip_or_hostname(config.host.clone())
        .tcp_port(config.port)
        .db_name((!config.database.is_empty()).then(|| config.database.clone()))
        .user(Some(config.username.clone()))
        .pass(Some(config.password.clone()))
        .prefer_socket(false)
        .tcp_nodelay(true);
    if let Some(max_allowed_packet) = max_allowed_packet {
        builder = builder.max_allowed_packet(Some(max_allowed_packet));
    }
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

pub async fn list_tables(config: &MySqlConnectionConfig) -> anyhow::Result<Vec<transferia_registry::TableIdentity>> {
    let mut connection = transferia_connector_support::external_request::observe_external_request(
        "mysql", "connect_table_catalog", connect(config)).await?;
    let result = list_tables_on_connection(&mut connection).await;
    let closed = transferia_connector_support::external_request::observe_external_request(
        "mysql", "disconnect_table_catalog", connection.disconnect()).await;
    let tables = result?;
    closed?;
    Ok(tables)
}

pub(crate) async fn list_tables_on_connection(connection: &mut Conn) -> anyhow::Result<Vec<transferia_registry::TableIdentity>> {
    // information_schema visibility is evaluated for the authenticated role,
    // across all databases rather than only the connection's default database.
    let rows: Vec<(String, String)> = transferia_connector_support::external_request::observe_external_request(
        "mysql", "list_tables",
        connection.query("SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES \
            WHERE TABLE_TYPE = 'BASE TABLE' \
            ORDER BY TABLE_SCHEMA, TABLE_NAME"),
    ).await?;
    let mut tables = Vec::with_capacity(rows.len());
    for (namespace, name) in rows {
        // Metadata visibility does not imply SELECT access to all columns.
        let query = format!("SELECT * FROM {}.{} LIMIT 0", quote_identifier(&namespace), quote_identifier(&name));
        let result = transferia_connector_support::external_request::observe_external_request(
            "mysql", "check_table_read_access", connection.query_drop(query),
        ).await;
        match result {
            Ok(()) => tables.push(transferia_registry::TableIdentity { namespace, name }),
            Err(mysql_async::Error::Server(error)) if matches!(error.code, 1142 | 1143) => {},
            Err(error) => return Err(error.into()),
        }
    }
    Ok(tables)
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
