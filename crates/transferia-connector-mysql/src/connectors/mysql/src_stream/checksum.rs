use std::error::Error;
use std::fmt;

use mysql_async::binlog::events::Event;
use mysql_async::binlog::{BinlogChecksumAlg, EventType};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BinlogChecksumVerifier {
    bootstrap_rotate_seen: bool,
    crc32_format_description_seen: bool,
}

impl BinlogChecksumVerifier {
    /// Validates the checksum contract of one event in stream order.
    ///
    /// MySQL may send one artificial rotate event before the format-description
    /// event. That protocol bootstrap frame is the only unchecksummed event we
    /// accept. Once a CRC32 FDE is seen, every subsequent frame must carry a
    /// valid CRC32 checksum.
    pub fn verify(&mut self, event: &Event) -> Result<(), BinlogChecksumError> {
        let event_type = event
            .header()
            .event_type()
            .map_err(|_| BinlogChecksumError::UnknownEventType(event.header().event_type_raw()))?;
        if !self.crc32_format_description_seen {
            if is_artificial_rotate(event_type, event) {
                if self.bootstrap_rotate_seen {
                    return Err(BinlogChecksumError::DuplicateBootstrapRotate);
                }
                self.bootstrap_rotate_seen = true;
                return match event.footer().get_checksum_alg() {
                    Ok(Some(BinlogChecksumAlg::BINLOG_CHECKSUM_ALG_CRC32)) => verify_crc32(event),
                    Ok(None | Some(BinlogChecksumAlg::BINLOG_CHECKSUM_ALG_OFF)) => Ok(()),
                    Err(error) => Err(BinlogChecksumError::UnknownAlgorithm(u8::from(error))),
                };
            }
        }
        match event.footer().get_checksum_alg() {
            Ok(Some(BinlogChecksumAlg::BINLOG_CHECKSUM_ALG_CRC32)) => {
                verify_crc32(event)?;
                if event_type == EventType::FORMAT_DESCRIPTION_EVENT {
                    self.crc32_format_description_seen = true;
                }
                Ok(())
            }
            Ok(algorithm) => Err(BinlogChecksumError::Crc32Required { algorithm }),
            Err(error) => Err(BinlogChecksumError::UnknownAlgorithm(u8::from(error))),
        }
    }
}

pub fn verify_event_checksum(event: &Event) -> Result<(), BinlogChecksumError> {
    let algorithm = event
        .footer()
        .get_checksum_alg()
        .map_err(|error| BinlogChecksumError::UnknownAlgorithm(u8::from(error)))?;

    if algorithm != Some(BinlogChecksumAlg::BINLOG_CHECKSUM_ALG_CRC32) {
        return Err(BinlogChecksumError::Crc32Required { algorithm });
    }

    verify_crc32(event)
}

fn verify_crc32(event: &Event) -> Result<(), BinlogChecksumError> {
    let actual = event
        .checksum()
        .ok_or(BinlogChecksumError::MissingCrc32)?;
    let expected = event
        .calc_checksum(BinlogChecksumAlg::BINLOG_CHECKSUM_ALG_CRC32)
        .to_le_bytes();
    if actual != expected {
        return Err(BinlogChecksumError::Mismatch { expected, actual });
    }
    Ok(())
}

/// MySQL marks an artificial Rotate by setting the event header's next-log
/// position to zero. Its payload position is the start position in the named
/// binlog and is normally `4`, so `RotateEvent::is_fake` (payload position
/// equals zero) does not describe the replication-protocol frame.
pub(crate) fn is_artificial_rotate(event_type: EventType, event: &Event) -> bool {
    event_type == EventType::ROTATE_EVENT && event.header().log_pos() == 0
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BinlogChecksumError {
    UnknownAlgorithm(u8),
    UnknownEventType(u8),
    Crc32Required {
        algorithm: Option<BinlogChecksumAlg>,
    },
    MissingCrc32,
    DuplicateBootstrapRotate,
    Mismatch {
        expected: [u8; 4],
        actual: [u8; 4],
    },
}

impl fmt::Display for BinlogChecksumError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownAlgorithm(algorithm) => {
                write!(formatter, "unknown binlog checksum algorithm {algorithm}")
            }
            Self::UnknownEventType(event_type) => {
                write!(formatter, "unknown binlog event type {event_type}")
            }
            Self::Crc32Required { algorithm } => {
                write!(
                    formatter,
                    "binlog event does not declare the required CRC32 checksum (algorithm {algorithm:?})"
                )
            }
            Self::MissingCrc32 => write!(formatter, "CRC32 is configured but event checksum is absent"),
            Self::DuplicateBootstrapRotate => {
                write!(formatter, "received more than one pre-FDE artificial rotate event")
            }
            Self::Mismatch { expected, actual } => write!(
                formatter,
                "binlog CRC32 mismatch: expected {:02x?}, received {:02x?}",
                expected, actual
            ),
        }
    }
}

impl Error for BinlogChecksumError {}
