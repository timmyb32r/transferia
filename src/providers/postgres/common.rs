use arrow::datatypes::{DataType, TimeUnit};
use tokio_postgres::types::Type;

pub const MAX_IDENTIFIER_BYTES: usize = 63;

pub async fn connect(connection: &str) -> anyhow::Result<tokio_postgres::Client> {
    let (client, connection) = tokio_postgres::connect(connection, tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            tracing::error!("PostgreSQL connection failed: {error}");
        }
    });
    Ok(client)
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
        Type::INT2 => DataType::Int16,
        Type::INT4 => DataType::Int32,
        Type::INT8 => DataType::Int64,
        Type::FLOAT4 => DataType::Float32,
        Type::FLOAT8 => DataType::Float64,
        Type::TEXT | Type::VARCHAR | Type::BPCHAR => DataType::Utf8,
        Type::DATE => DataType::Date32,
        Type::TIMESTAMP => DataType::Timestamp(TimeUnit::Microsecond, None),
        _ => anyhow::bail!("unsupported PostgreSQL type '{}'", data_type.name()),
    })
}

pub fn arrow_to_postgres(data_type: &DataType) -> anyhow::Result<Type> {
    Ok(match data_type {
        DataType::Boolean => Type::BOOL,
        DataType::Int16 => Type::INT2,
        DataType::Int32 => Type::INT4,
        DataType::Int64 => Type::INT8,
        DataType::Float32 => Type::FLOAT4,
        DataType::Float64 => Type::FLOAT8,
        DataType::Utf8 => Type::TEXT,
        DataType::Date32 => Type::DATE,
        DataType::Timestamp(_, None) => Type::TIMESTAMP,
        _ => anyhow::bail!("unsupported Arrow type {data_type:?} for PostgreSQL binary COPY"),
    })
}
