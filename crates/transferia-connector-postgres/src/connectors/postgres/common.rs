use arrow::datatypes::DataType;
use schemars::JsonSchema;
use serde::Deserialize;
use tokio_postgres::types::{Kind, Type};

use super::temporal::{timestamp_data_type, timestamp_has_timezone};

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum PostgresCopyFormat {
    #[default]
    Binary,
    Text,
}

#[derive(Clone, Deserialize)]
pub struct PostgresConnectionCheckConfig {
    #[serde(default)]
    pub host: String,

    #[serde(default = "default_postgres_port")]
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

impl PostgresConnectionCheckConfig {
    #[must_use]
    pub const fn credentials_complete(&self) -> bool {
        !self.database.is_empty() && !self.username.is_empty()
    }

    #[must_use]
    pub fn connection(&self) -> PostgresConnectionConfig {
        PostgresConnectionConfig {
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

const fn default_postgres_port() -> u16 {
    5432
}

pub const MAX_IDENTIFIER_BYTES: usize = 63;

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PostgresConnectionConfig {
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

impl PostgresConnectionConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        crate::connectors::address::validate_host("postgres.host", &self.host)?;
        crate::connectors::address::validate_port("postgres.port", self.port)?;
        anyhow::ensure!(
            !self.database.is_empty(),
            "postgres.database must not be empty"
        );
        anyhow::ensure!(
            !self.username.is_empty(),
            "postgres.username must not be empty"
        );
        if let Some(path) = &self.tls_ca_file {
            anyhow::ensure!(
                !path.trim().is_empty(),
                "postgres.tls_ca_file must not be empty"
            );
        }
        Ok(())
    }
}

/// A request-scoped connection: dropping a cancelled preview must terminate its
/// driver, not leave a detached task draining a query nobody can consume.
pub(crate) struct SampleConnection {
    client: tokio_postgres::Client,
    driver: tokio::task::JoinHandle<()>,
}

impl std::ops::Deref for SampleConnection {
    type Target = tokio_postgres::Client;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl Drop for SampleConnection {
    fn drop(&mut self) {
        self.driver.abort();
    }
}

pub(crate) async fn connect_sample(config: &PostgresConnectionConfig) -> anyhow::Result<SampleConnection> {
    let (client, driver) = connect_with_driver(config).await?;
    Ok(SampleConnection { client, driver })
}

pub async fn connect(config: &PostgresConnectionConfig) -> anyhow::Result<tokio_postgres::Client> {
    let (client, _driver) = connect_with_driver(config).await?;
    Ok(client)
}

async fn connect_with_driver(config: &PostgresConnectionConfig)
    -> anyhow::Result<(tokio_postgres::Client, tokio::task::JoinHandle<()>)> {
    let mut connection_config = tokio_postgres::Config::new();
    connection_config
        .host(&config.host)
        .port(config.port)
        .dbname(&config.database)
        .user(&config.username)
        .password(&config.password);
    let connection = if config.trusted_plaintext {
        let (client, connection) = connection_config.connect(tokio_postgres::NoTls).await?;
        let driver = tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::error!("PostgreSQL connection failed: {error}");
            }
        });
        (client, driver)
    } else {
        drop(rustls::crypto::aws_lc_rs::default_provider().install_default());
        let mut roots = rustls::RootCertStore::empty();
        let native = rustls_native_certs::load_native_certs();
        for certificate in native.certs {
            roots.add(certificate)?;
        }
        if let Some(path) = &config.tls_ca_file {
            let bytes = std::fs::read(path)?;
            let mut reader = std::io::BufReader::new(bytes.as_slice());
            for certificate in rustls_pemfile::certs(&mut reader) {
                roots.add(certificate?)?;
            }
        }
        let tls = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let (client, connection) = connection_config
            .connect(tokio_postgres_rustls::MakeRustlsConnect::new(tls))
            .await?;
        let driver = tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::error!("PostgreSQL TLS connection failed: {error}");
            }
        });
        (client, driver)
    };
    Ok(connection)
}

/// Enumerate persistent tables the authenticated role can read, including system schemas.
/// Keep the catalog complete so source filters can be toggled without reconnecting.
pub async fn list_tables(
    config: &PostgresConnectionConfig,
) -> anyhow::Result<Vec<transferia_registry::TableIdentity>> {
    let client = connect(config).await?;
    let rows = transferia_connector_support::external_request::observe_external_request(
        "postgres",
        "list_tables",
        // MDB transaction pooling may release the backend after each Sync.
        // Parse/bind/execute together without a session-scoped named statement.
        client.query_typed(
            "SELECT n.nspname, c.relname FROM pg_catalog.pg_class c \
             JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
             WHERE c.relkind IN ('r', 'p') AND c.relpersistence = 'p' \
             AND pg_catalog.has_schema_privilege(n.oid, 'USAGE') \
             AND pg_catalog.has_table_privilege(c.oid, 'SELECT') \
             ORDER BY n.nspname, c.relname",
            &[],
        ),
    )
    .await
    .map_err(|error| {
        let diagnostic = error.as_db_error().map_or_else(
            || error.to_string(),
            |database| {
                format!(
                    "{} (SQLSTATE {})",
                    database.message(),
                    database.code().code()
                )
            },
        );
        anyhow::anyhow!("PostgreSQL table discovery failed: {diagnostic}")
    })?;
    rows.into_iter()
        .map(|row| {
            Ok(transferia_registry::TableIdentity {
                namespace: row.try_get(0)?,
                name: row.try_get(1)?,
            })
        })
        .collect()
}

pub async fn check_connection(config: &PostgresConnectionConfig) -> anyhow::Result<()> {
    config.validate()?;
    let client = connect(config).await.map_err(|error| {
        let code = error
            .downcast_ref::<tokio_postgres::Error>()
            .and_then(tokio_postgres::Error::code)
            .map(tokio_postgres::error::SqlState::code);
        authentication_check_message(code, config.password.is_empty())
            .map_or(error, |message| anyhow::anyhow!(message))
    })?;
    client.simple_query("SELECT 1").await?;
    Ok(())
}

fn authentication_check_message(code: Option<&str>, empty_password: bool) -> Option<&'static str> {
    match code {
        Some("28P01") if empty_password => Some(
            "PostgreSQL is reachable, but authentication failed. The password field is empty. Enter the password for this user and try again.",
        ),
        Some("28P01") => Some(
            "PostgreSQL is reachable, but authentication failed. Check the username and password and try again.",
        ),
        Some("28000") => Some(
            "PostgreSQL is reachable, but access was rejected. Check the user and the server authentication rules (pg_hba.conf).",
        ),
        _ => None,
    }
}

#[cfg(test)]
#[path = "tests/common.rs"]
mod tests;

pub async fn check_network_connection(
    config: &PostgresConnectionCheckConfig,
) -> anyhow::Result<()> {
    crate::connectors::address::validate_host("postgres.host", &config.host)?;
    crate::connectors::address::validate_port("postgres.port", config.port)?;
    tokio::net::TcpStream::connect((config.host.as_str(), config.port)).await?;
    Ok(())
}

pub fn validate_identifier(kind: &str, value: &str) -> anyhow::Result<()> {
    let mut bytes = value.bytes();
    let valid = bytes
        .next()
        .is_some_and(|byte| byte == b'_' || byte.is_ascii_alphabetic())
        && bytes.all(|byte| byte == b'_' || byte.is_ascii_alphanumeric());
    anyhow::ensure!(
        valid && value.len() <= MAX_IDENTIFIER_BYTES,
        "invalid PostgreSQL {kind} '{value}'; expected ASCII [A-Za-z_][A-Za-z0-9_]* and at most {MAX_IDENTIFIER_BYTES} bytes"
    );
    Ok(())
}

pub fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

pub fn postgres_to_arrow(data_type: &Type) -> anyhow::Result<DataType> {
    Ok(match *data_type {
        Type::BOOL => DataType::Boolean,
        Type::CHAR => DataType::Int8,
        Type::INT2 => DataType::Int16,
        Type::INT4 => DataType::Int32,
        Type::INT8 => DataType::Int64,
        Type::OID => DataType::UInt32,
        Type::FLOAT4 => DataType::Float32,
        Type::FLOAT8 => DataType::Float64,
        Type::BYTEA => DataType::Binary,
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME => DataType::Utf8,
        Type::DATE => DataType::Date32,
        Type::TIMESTAMP => timestamp_data_type(false),
        Type::TIMESTAMPTZ => timestamp_data_type(true),
        _ => match data_type.kind() {
            Kind::Pseudo => anyhow::bail!(
                "PostgreSQL source type '{data_type}' has no supported Arrow representation; select unsupported_types=to_string for an explicit batch text conversion"
            ),
            Kind::Simple
            | Kind::Enum(_)
            | Kind::Array(_)
            | Kind::Range(_)
            | Kind::Multirange(_)
            | Kind::Domain(_)
            | Kind::Composite(_) => DataType::Utf8,
            other => anyhow::bail!("unsupported PostgreSQL type kind {other:?} for '{data_type}'"),
        },
    })
}

#[must_use]
pub const fn postgres_requires_text_projection(data_type: &Type) -> bool {
    !matches!(
        *data_type,
        Type::BOOL
            | Type::CHAR
            | Type::INT2
            | Type::INT4
            | Type::INT8
            | Type::OID
            | Type::FLOAT4
            | Type::FLOAT8
            | Type::BYTEA
            | Type::TEXT
            | Type::VARCHAR
            | Type::BPCHAR
            | Type::NAME
            | Type::DATE
            | Type::TIMESTAMP
            | Type::TIMESTAMPTZ
    )
}

pub fn arrow_to_postgres(data_type: &DataType) -> anyhow::Result<Type> {
    Ok(match data_type {
        DataType::Boolean => Type::BOOL,
        DataType::Int8 => Type::CHAR,
        DataType::Int16 | DataType::UInt8 => Type::INT2,
        DataType::Int32 | DataType::UInt16 => Type::INT4,
        DataType::Int64 => Type::INT8,
        DataType::UInt32 => Type::OID,
        DataType::UInt64 => Type::NUMERIC,
        DataType::Float32 => Type::FLOAT4,
        DataType::Float64 => Type::FLOAT8,
        DataType::Binary => Type::BYTEA,
        DataType::Utf8 => Type::TEXT,
        DataType::Date32 => Type::DATE,
        DataType::Timestamp(_, _) if timestamp_has_timezone(data_type)? => Type::TIMESTAMPTZ,
        DataType::Timestamp(_, _) => Type::TIMESTAMP,
        _ => anyhow::bail!("unsupported Arrow type {data_type:?} for PostgreSQL COPY"),
    })
}
