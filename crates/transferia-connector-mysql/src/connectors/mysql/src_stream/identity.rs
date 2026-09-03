use super::decoder::MySqlTransactionIdentity;
use super::super::src_batch_and_stream::MySqlBinlogBoundary;

const IDENTITY_DOMAIN: &[u8] = b"transferia.mysql.source-transaction-id";
const IDENTITY_VERSION: u8 = 1;
const GTID_TRANSACTION_TAG: u8 = 1;
const ANONYMOUS_TRANSACTION_TAG: u8 = 2;
const FILE_POSITION_TRANSACTION_TAG: u8 = 3;
const SNAPSHOT_BOUNDARY_TAG: u8 = 4;

/// Encodes an exact, injective source transaction identity without hashing.
pub(crate) fn encode_transaction_identity(
    identity: &MySqlTransactionIdentity,
) -> Result<Vec<u8>, IdentityEncodingError> {
    let mut encoded = identity_prefix(match identity {
        MySqlTransactionIdentity::Gtid { .. } => GTID_TRANSACTION_TAG,
        MySqlTransactionIdentity::Anonymous { .. } => ANONYMOUS_TRANSACTION_TAG,
        MySqlTransactionIdentity::FilePosition { .. } => FILE_POSITION_TRANSACTION_TAG,
    });
    match identity {
        MySqlTransactionIdentity::Gtid { sid, tag, gno } => {
            push_field(&mut encoded, 1, sid)?;
            push_optional_field(&mut encoded, 2, tag.as_deref())?;
            push_field(&mut encoded, 3, &gno.to_be_bytes())?;
        }
        MySqlTransactionIdentity::Anonymous { begin_position }
        | MySqlTransactionIdentity::FilePosition { begin_position } => {
            push_field(&mut encoded, 1, &begin_position.filename)?;
            push_field(&mut encoded, 2, &begin_position.position.to_be_bytes())?;
        }
    }
    Ok(encoded)
}

/// Encodes the exact snapshot transaction boundary in the same identity domain.
pub(crate) fn encode_snapshot_boundary_identity(
    boundary: &MySqlBinlogBoundary,
) -> Result<Vec<u8>, IdentityEncodingError> {
    let mut encoded = identity_prefix(SNAPSHOT_BOUNDARY_TAG);
    push_field(&mut encoded, 1, boundary.filename.as_bytes())?;
    push_field(&mut encoded, 2, &boundary.position.to_be_bytes())?;
    push_field(&mut encoded, 3, boundary.gtid_executed.as_bytes())?;
    push_field(
        &mut encoded,
        4,
        &boundary.source_timestamp_micros.to_be_bytes(),
    )?;
    Ok(encoded)
}

fn identity_prefix(kind: u8) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(IDENTITY_DOMAIN.len() + 2);
    encoded.extend_from_slice(IDENTITY_DOMAIN);
    encoded.push(IDENTITY_VERSION);
    encoded.push(kind);
    encoded
}

fn push_field(
    encoded: &mut Vec<u8>,
    tag: u8,
    value: &[u8],
) -> Result<(), IdentityEncodingError> {
    let length = u64::try_from(value.len()).map_err(|_| IdentityEncodingError::FieldTooLong)?;
    encoded.push(tag);
    encoded.extend_from_slice(&length.to_be_bytes());
    encoded.extend_from_slice(value);
    Ok(())
}

fn push_optional_field(
    encoded: &mut Vec<u8>,
    tag: u8,
    value: Option<&str>,
) -> Result<(), IdentityEncodingError> {
    match value {
        None => push_field(encoded, tag, &[0]),
        Some(value) => {
            let mut framed = Vec::new();
            framed.push(1);
            let length =
                u64::try_from(value.len()).map_err(|_| IdentityEncodingError::FieldTooLong)?;
            framed.extend_from_slice(&length.to_be_bytes());
            framed.extend_from_slice(value.as_bytes());
            push_field(encoded, tag, &framed)
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IdentityEncodingError {
    FieldTooLong,
}

impl core::fmt::Display for IdentityEncodingError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::FieldTooLong => write!(
                formatter,
                "MySQL transaction identity field length does not fit the persistent u64 framing"
            ),
        }
    }
}

impl std::error::Error for IdentityEncodingError {}
