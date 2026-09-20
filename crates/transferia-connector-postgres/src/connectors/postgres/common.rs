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

/// A connection whose owner also owns the protocol driver. Cancellable snapshot
/// connections configure an explicit cleanup deadline: abandoned work sends a
/// PostgreSQL CancelRequest before closing its frontend socket. Closing TCP alone
/// does not interrupt a PostgreSQL lock wait when client liveness checks are off.
/// The immutable client view cannot be replaced independently of its driver.
pub(super) struct OwnedConnection {
    client: std::sync::Arc<tokio_postgres::Client>,
    driver: Option<tokio::task::JoinHandle<()>>,
    cancel_transport: CancelTransport,
    cancel_timeout: Option<std::time::Duration>,
    cancellation_armed: std::sync::atomic::AtomicBool,
}

impl OwnedConnection {
    /// The deadline comes from owning operation configuration, never a hidden
    /// transport constant. Configure it before starting a cancellable query.
    pub(super) fn with_cancellation_timeout(mut self, timeout: std::time::Duration) -> anyhow::Result<Self> {
        anyhow::ensure!(!timeout.is_zero(), "PostgreSQL cancellation timeout must be positive");
        self.cancel_timeout = Some(timeout);
        self.cancellation_armed.store(true, std::sync::atomic::Ordering::Release);
        Ok(self)
    }

    /// Call only after a checked COMMIT/ROLLBACK or another proven idle boundary.
    /// Subsequent Drop then closes the socket without an unnecessary CancelRequest.
    pub(super) fn disarm_cancellation(&self) {
        self.cancellation_armed.store(false, std::sync::atomic::Ordering::Release);
    }

    /// Interrupt a blocked query, preserving TLS and keeping the frontend alive
    /// until its cancellation packet has been sent (required by poolers).
    /// PostgreSQL sends no cancellation acknowledgement: success means the
    /// request was transmitted and the local driver closed, not that a backend
    /// termination was observed.
    pub(super) async fn cancel(&mut self) -> anyhow::Result<()> {
        let Some(driver) = self.driver.take() else { return Ok(()); };
        let driver = AbortDriver(Some(driver));
        let timeout = self.cancel_timeout.ok_or_else(|| anyhow::anyhow!("PostgreSQL cancellation deadline was not configured"))?;
        cancel_and_close(std::sync::Arc::clone(&self.client), self.cancel_transport.clone(), driver, timeout).await
    }

    /// Abort and join the driver so asynchronous shutdown returns only after
    /// its socket has closed. This never waits for PostgreSQL to drain a query.
    pub(super) async fn close(&mut self) -> anyhow::Result<()> {
        let Some(driver) = self.driver.take() else { return Ok(()); };
        self.disarm_cancellation();
        AbortDriver(Some(driver)).close().await
    }
}

impl std::ops::Deref for OwnedConnection {
    type Target = tokio_postgres::Client;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl Drop for OwnedConnection {
    fn drop(&mut self) {
        let Some(driver) = self.driver.take() else { return; };
        let driver = AbortDriver(Some(driver));
        let configured = self.cancel_timeout.filter(|_| self.cancellation_armed.load(std::sync::atomic::Ordering::Acquire));
        if let (Some(timeout), Ok(runtime)) = (configured, tokio::runtime::Handle::try_current()) {
            let client = std::sync::Arc::clone(&self.client);
            let transport = self.cancel_transport.clone();
            drop(runtime.spawn(async move {
                if cancel_and_close(client, transport, driver, timeout).await.is_err() {
                    tracing::warn!(operation = "cancel_abandoned_postgres_request", "PostgreSQL cancellation request could not be sent; the local connection driver was closed");
                }
            }));
        }
        // Without a configured cancellation deadline/runtime, the local guard
        // is still dropped and aborts its driver. No unbounded task is spawned.
    }
}

#[derive(Clone)]
enum CancelTransport {
    Plaintext,
    Tls(tokio_postgres_rustls::MakeRustlsConnect),
}

impl CancelTransport {
    async fn send(&self, token: tokio_postgres::CancelToken) -> anyhow::Result<()> {
        transferia_connector_support::external_request::observe_external_request("postgres", "cancel_query", async {
            let result = match self {
                Self::Plaintext => token.cancel_query(tokio_postgres::NoTls).await,
                Self::Tls(tls) => token.cancel_query(tls.clone()).await,
            };
            result.map_err(|error| anyhow::anyhow!("PostgreSQL cancellation transport failed (SQLSTATE {})", error.code().map_or("unavailable", tokio_postgres::error::SqlState::code)))
        }).await
    }
}

/// Closing this guard cannot recursively schedule another cancellation task.
struct AbortDriver(Option<tokio::task::JoinHandle<()>>);

impl AbortDriver {
    async fn close(mut self) -> anyhow::Result<()> {
        let Some(driver) = self.0.take() else { return Ok(()); };
        driver.abort();
        match driver.await {
            Ok(()) => Ok(()),
            Err(error) if error.is_cancelled() => Ok(()),
            Err(_) => anyhow::bail!("PostgreSQL connection driver terminated unexpectedly"),
        }
    }
}

impl Drop for AbortDriver {
    fn drop(&mut self) {
        if let Some(driver) = &self.0 { driver.abort(); }
    }
}

async fn cancel_and_close(client: std::sync::Arc<tokio_postgres::Client>, transport: CancelTransport, driver: AbortDriver, timeout: std::time::Duration) -> anyhow::Result<()> {
    let cancellation = tokio::time::timeout(timeout, transport.send(client.cancel_token())).await
        .map_err(|_| anyhow::anyhow!("PostgreSQL cancellation exceeded its configured deadline"));
    let closed = driver.close().await;
    drop(client);
    cancellation??;
    closed
}

pub(super) async fn connect_owned(
    config: &PostgresConnectionConfig,
) -> anyhow::Result<OwnedConnection> {
    let (client, driver, cancel_transport) = connect_with_driver(config).await?;
    Ok(OwnedConnection { client: std::sync::Arc::new(client), driver: Some(driver), cancel_transport, cancel_timeout: None, cancellation_armed: std::sync::atomic::AtomicBool::new(false) })
}

pub async fn connect(config: &PostgresConnectionConfig) -> anyhow::Result<tokio_postgres::Client> {
    let (client, _driver, _cancel_transport) = connect_with_driver(config).await?;
    Ok(client)
}

async fn connect_with_driver(
    config: &PostgresConnectionConfig,
) -> anyhow::Result<(tokio_postgres::Client, tokio::task::JoinHandle<()>, CancelTransport)> {
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
        (client, driver, CancelTransport::Plaintext)
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
        let tls = tokio_postgres_rustls::MakeRustlsConnect::new(tls);
        let (client, connection) = connection_config
            .connect(tls.clone())
            .await?;
        let driver = tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::error!("PostgreSQL TLS connection failed: {error}");
            }
        });
        (client, driver, CancelTransport::Tls(tls))
    };
    Ok(connection)
}

/// Enumerate persistent tables the authenticated role can read, including system schemas.
/// Keep the catalog complete so source filters can be toggled without reconnecting.
pub async fn list_tables(
    config: &PostgresConnectionConfig,
) -> anyhow::Result<Vec<transferia_registry::TableIdentity>> {
    let client = connect_owned(config).await?;
    let rows = transferia_connector_support::external_request::observe_external_request(
        "postgres",
        "list_tables",
        // MDB transaction pooling may release the backend after each Sync.
        // Parse/bind/execute together without a session-scoped named statement.
        client.query_typed(
            "SELECT n.nspname, c.relname FROM pg_catalog.pg_class c \
             JOIN pg_catalog.pg_namespace n ON n.oid OPERATOR(pg_catalog.=) c.relnamespace \
             WHERE (c.relkind OPERATOR(pg_catalog.=) 'r'::pg_catalog.\"char\" \
                    OR c.relkind OPERATOR(pg_catalog.=) 'p'::pg_catalog.\"char\") \
             AND c.relpersistence OPERATOR(pg_catalog.=) 'p'::pg_catalog.\"char\" \
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
