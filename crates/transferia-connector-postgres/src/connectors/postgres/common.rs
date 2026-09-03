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

pub async fn connect(config: &PostgresConnectionConfig) -> anyhow::Result<tokio_postgres::Client> {
    let mut connection_config = tokio_postgres::Config::new();
    connection_config
        .host(&config.host)
        .port(config.port)
        .dbname(&config.database)
        .user(&config.username)
        .password(&config.password);
    let client = if config.trusted_plaintext {
        let (client, connection) = connection_config.connect(tokio_postgres::NoTls).await?;
        tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::error!("PostgreSQL connection failed: {error}");
            }
        });
        client
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
        tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::error!("PostgreSQL TLS connection failed: {error}");
            }
        });
        client
    };
    Ok(client)
}

pub async fn check_connection(config: &PostgresConnectionConfig) -> anyhow::Result<()> {
    config.validate()?;
    let client = connect(config).await?;
    client.simple_query("SELECT 1").await?;
    Ok(())
}

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
                "PostgreSQL pseudo-type '{data_type}' cannot be stored in a source table"
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
        DataType::Int16 => Type::INT2,
        DataType::Int32 => Type::INT4,
        DataType::Int64 => Type::INT8,
        DataType::UInt8 => Type::INT2,
        DataType::UInt16 => Type::INT4,
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
