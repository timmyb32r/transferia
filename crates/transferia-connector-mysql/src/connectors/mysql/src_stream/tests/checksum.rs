use mysql_async::binlog::events::{BinlogEventFooter, Event, FormatDescriptionEvent};
use mysql_async::binlog::{BinlogChecksumAlg, BinlogVersion, EventType};

use super::super::{
    verify_event_checksum, BinlogChecksumError, BinlogChecksumVerifier,
};

#[test]
fn crc32_is_verified_and_corruption_fails_closed() {
    let mut bytes = raw_event_bytes(EventType::XID_EVENT, &7_u64.to_le_bytes(), 4, true);
    let fde = FormatDescriptionEvent::new(BinlogVersion::Version4).with_footer(
        BinlogEventFooter::new(BinlogChecksumAlg::BINLOG_CHECKSUM_ALG_CRC32),
    );
    let event = Event::read(&fde, bytes.as_slice()).unwrap();
    verify_event_checksum(&event).unwrap();

    let last = bytes.len() - 1;
    bytes[last] ^= 0x80;
    let corrupt = Event::read(&fde, bytes.as_slice()).unwrap();
    assert!(matches!(
        verify_event_checksum(&corrupt),
        Err(BinlogChecksumError::Mismatch { .. })
    ));
}

#[test]
fn only_the_protocol_fake_rotate_may_precede_crc32_events_without_a_checksum() {
    let fde = FormatDescriptionEvent::new(BinlogVersion::Version4);
    let fake_rotate = Event::read(
        &fde,
        raw_event_bytes_with_log_pos(
            EventType::ROTATE_EVENT,
            &rotate_data(b"mysql-bin.000001", 4),
            0,
            false,
        )
        .as_slice(),
    )
    .unwrap();
    let unchecksummed_xid = Event::read(
        &fde,
        raw_event_bytes(EventType::XID_EVENT, &7_u64.to_le_bytes(), 4, false).as_slice(),
    )
    .unwrap();
    let mut verifier = BinlogChecksumVerifier::default();

    verifier.verify(&fake_rotate).unwrap();
    assert!(matches!(
        verifier.verify(&fake_rotate),
        Err(BinlogChecksumError::DuplicateBootstrapRotate)
    ));
    assert!(matches!(
        verifier.verify(&unchecksummed_xid),
        Err(BinlogChecksumError::Crc32Required { .. })
    ));
}

#[test]
fn artificial_rotate_uses_header_position_then_crc32_fde_establishes_checksum_state() {
    let placeholder = FormatDescriptionEvent::new(BinlogVersion::Version4);
    let fake_rotate = Event::read(
        &placeholder,
        raw_event_bytes_with_log_pos(
            EventType::ROTATE_EVENT,
            &rotate_data(b"mysql-bin.000042", 4),
            0,
            false,
        )
        .as_slice(),
    )
    .unwrap();
    let crc32_fde = crc32_format_description_event();
    let checksummed_xid = Event::read(
        crc32_fde.fde(),
        raw_event_bytes(EventType::XID_EVENT, &7_u64.to_le_bytes(), 4, true).as_slice(),
    )
    .unwrap();
    let mut verifier = BinlogChecksumVerifier::default();

    verifier.verify(&fake_rotate).unwrap();
    assert!(matches!(
        verifier.verify(&fake_rotate),
        Err(BinlogChecksumError::DuplicateBootstrapRotate)
    ));
    verifier.verify(&crc32_fde).unwrap();
    verifier.verify(&checksummed_xid).unwrap();

    let unchecksummed_xid = Event::read(
        &placeholder,
        raw_event_bytes(EventType::XID_EVENT, &7_u64.to_le_bytes(), 4, false).as_slice(),
    )
    .unwrap();
    assert!(matches!(
        verifier.verify(&unchecksummed_xid),
        Err(BinlogChecksumError::Crc32Required { .. })
    ));
}

pub(super) fn raw_event(event_type: EventType, data: &[u8], start: u32) -> Event {
    let bytes = raw_event_bytes(event_type, data, start, true);
    Event::read(
        &FormatDescriptionEvent::new(BinlogVersion::Version4).with_footer(
            BinlogEventFooter::new(BinlogChecksumAlg::BINLOG_CHECKSUM_ALG_CRC32),
        ),
        bytes.as_slice(),
    )
    .unwrap()
}

fn raw_event_bytes(event_type: EventType, data: &[u8], start: u32, checksum: bool) -> Vec<u8> {
    let checksum_len = usize::from(checksum) * 4;
    let event_size = 19 + data.len() + checksum_len;
    let next = start + event_size as u32;
    raw_event_bytes_with_log_pos(event_type, data, next, checksum)
}

pub(super) fn raw_event_bytes_with_log_pos(
    event_type: EventType,
    data: &[u8],
    log_pos: u32,
    checksum: bool,
) -> Vec<u8> {
    let checksum_len = usize::from(checksum) * 4;
    let event_size = 19 + data.len() + checksum_len;
    let mut bytes = Vec::with_capacity(event_size);
    bytes.extend_from_slice(&1_700_000_000_u32.to_le_bytes());
    bytes.push(event_type as u8);
    bytes.extend_from_slice(&9_u32.to_le_bytes());
    bytes.extend_from_slice(&(event_size as u32).to_le_bytes());
    bytes.extend_from_slice(&log_pos.to_le_bytes());
    bytes.extend_from_slice(&0_u16.to_le_bytes());
    bytes.extend_from_slice(data);
    if checksum {
        let crc = crc32(&bytes);
        bytes.extend_from_slice(&crc.to_le_bytes());
    }
    bytes
}

fn crc32_format_description_event() -> Event {
    let mut data = Vec::with_capacity(58);
    data.extend_from_slice(&4_u16.to_le_bytes());
    let mut server_version = [0_u8; 50];
    server_version[..6].copy_from_slice(b"8.0.36");
    data.extend_from_slice(&server_version);
    data.extend_from_slice(&0_u32.to_le_bytes());
    data.push(19);
    data.push(BinlogChecksumAlg::BINLOG_CHECKSUM_ALG_CRC32 as u8);

    let event_size = 19 + data.len() + 4;
    let mut bytes = Vec::with_capacity(event_size);
    bytes.extend_from_slice(&1_700_000_000_u32.to_le_bytes());
    bytes.push(EventType::FORMAT_DESCRIPTION_EVENT as u8);
    bytes.extend_from_slice(&9_u32.to_le_bytes());
    bytes.extend_from_slice(&(event_size as u32).to_le_bytes());
    bytes.extend_from_slice(&(event_size as u32 + 4).to_le_bytes());
    bytes.extend_from_slice(&0_u16.to_le_bytes());
    bytes.extend_from_slice(&data);
    let checksum = crc32(&bytes);
    bytes.extend_from_slice(&checksum.to_le_bytes());

    Event::read(
        &FormatDescriptionEvent::new(BinlogVersion::Version4),
        bytes.as_slice(),
    )
    .unwrap()
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = u32::MAX;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0xedb8_8320 & (0_u32.wrapping_sub(crc & 1)));
        }
    }
    !crc
}

fn rotate_data(filename: &[u8], position: u64) -> Vec<u8> {
    let mut data = Vec::with_capacity(8 + filename.len());
    data.extend_from_slice(&position.to_le_bytes());
    data.extend_from_slice(filename);
    data
}
