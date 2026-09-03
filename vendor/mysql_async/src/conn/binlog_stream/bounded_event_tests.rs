use bytes::BytesMut;
use mysql_common::{
    binlog::{
        consts::{BinlogChecksumAlg, BinlogVersion, EventFlags, EventType},
        events::{BinlogEventFooter, BinlogEventHeader, FormatDescriptionEvent},
        EventStreamReader,
    },
    proto::MySerialize,
};
use tokio_util::codec::Decoder;

use super::{
    binlog_packet_limit, map_binlog_packet_error, validate_binlog_event_reader_safety,
    validate_binlog_event_size, unexpected_binlog_packet, BinlogStream, BinlogStreamRequest,
    MAX_BINLOG_EVENT_BYTES,
};
use crate::{error::DriverError, io::PacketCodec, Conn, Opts};

#[test]
fn request_builder_carries_the_exact_event_limit() {
    let request = BinlogStreamRequest::new(17).with_max_event_bytes(64 * 1024 * 1024);
    assert_eq!(request.max_event_bytes, Some(64 * 1024 * 1024));
}

#[test]
fn event_limit_reserves_only_the_replication_status_byte() {
    assert_eq!(
        binlog_packet_limit(BinlogEventHeader::LEN).unwrap(),
        BinlogEventHeader::LEN + 1
    );
    assert!(matches!(
        binlog_packet_limit(BinlogEventHeader::LEN - 1).unwrap_err(),
        crate::Error::Driver(DriverError::InvalidBinlogEventSizeLimit { .. })
    ));
    assert_eq!(
        binlog_packet_limit(MAX_BINLOG_EVENT_BYTES).unwrap(),
        MAX_BINLOG_EVENT_BYTES + 1
    );
    if let Some(above_protocol_range) = MAX_BINLOG_EVENT_BYTES.checked_add(1) {
        assert!(matches!(
            binlog_packet_limit(above_protocol_range).unwrap_err(),
            crate::Error::Driver(DriverError::InvalidBinlogEventSizeLimit { .. })
        ));
    }
}

#[test]
fn packet_header_over_the_limit_is_rejected_before_payload_arrives() {
    let max_event_bytes = 32_usize;
    let max_packet_bytes = binlog_packet_limit(max_event_bytes).unwrap();
    let oversized_packet_bytes = max_packet_bytes + 1;
    let mut encoded_header = BytesMut::from(
        &[
            oversized_packet_bytes as u8,
            (oversized_packet_bytes >> 8) as u8,
            (oversized_packet_bytes >> 16) as u8,
            0,
        ][..],
    );
    let mut codec = PacketCodec::default();
    codec.max_allowed_packet = max_packet_bytes;

    let error = map_binlog_packet_error(
        codec.decode(&mut encoded_header).unwrap_err(),
        max_event_bytes,
    );
    assert!(matches!(
        error,
        crate::Error::Driver(DriverError::BinlogEventTooLarge {
            max_event_bytes: 32,
        })
    ));
    assert_eq!(encoded_header.len(), 4);
}

#[test]
fn declared_event_size_must_exactly_match_the_complete_packet() {
    let exact = event_with_declared_size(32, 32);
    validate_binlog_event_size(&exact, 32).unwrap();

    for (declared, actual) in [(31, 32), (33, 32)] {
        let event = event_with_declared_size(declared, actual);
        assert!(matches!(
            validate_binlog_event_size(&event, 32),
            Err(DriverError::InvalidBinlogEventSize {
                declared_event_bytes,
                actual_event_bytes,
            }) if declared_event_bytes == declared && actual_event_bytes == actual
        ));
    }
}

#[test]
fn truncated_and_oversized_events_fail_before_event_reader() {
    let truncated = vec![0_u8; BinlogEventHeader::LEN - 1];
    assert!(matches!(
        validate_binlog_event_size(&truncated, BinlogEventHeader::LEN),
        Err(DriverError::TruncatedBinlogEventHeader { .. })
    ));

    let oversized = event_with_declared_size(32, 32);
    assert!(matches!(
        validate_binlog_event_size(&oversized, 31),
        Err(DriverError::BinlogEventTooLarge {
            max_event_bytes: 31,
        })
    ));
}

#[test]
fn unexpected_binlog_packet_diagnostic_never_retains_payload() {
    assert!(matches!(
        unexpected_binlog_packet(Some(7), 65_536),
        DriverError::UnexpectedBinlogPacket {
            first_byte: Some(7),
            packet_bytes: 65_536,
        }
    ));
}

#[test]
fn table_maps_are_evicted_at_an_explicit_safe_boundary() {
    let mut stream = BinlogStream::new(Conn::empty(Opts::default()), 1_024);
    for table_id in 1..=1_024 {
        let event = table_map_event(table_id);
        stream.esr.read(event.as_slice()).unwrap().unwrap();
        assert!(stream.get_tme(table_id).is_some());
    }

    stream.clear_table_maps().unwrap();
    for table_id in 1..=1_024 {
        assert!(stream.get_tme(table_id).is_none());
    }
}

#[test]
fn table_map_eviction_preserves_the_exact_checksum_format_description() {
    let format_description = crc32_format_description_event();
    let mut stream = BinlogStream::new(Conn::empty(Opts::default()), format_description.len());
    stream
        .esr
        .read(format_description.as_slice())
        .unwrap()
        .unwrap();
    stream.format_description_event = Some(format_description);

    stream.clear_table_maps().unwrap();
    assert_eq!(
        stream.esr.get_fde().footer().get_checksum_alg().unwrap(),
        Some(BinlogChecksumAlg::BINLOG_CHECKSUM_ALG_CRC32)
    );
}

#[test]
fn short_events_after_crc32_fde_fail_before_mysql_common_reader() {
    let reader = crc32_event_reader();
    let minimum = BinlogEventHeader::LEN + BinlogEventFooter::BINLOG_CHECKSUM_LEN;
    for actual in BinlogEventHeader::LEN..minimum {
        let event = event_with_declared_size(actual as u32, actual);
        assert!(matches!(
            validate_binlog_event_reader_safety(&event, &reader),
            Err(DriverError::TruncatedChecksummedBinlogEvent {
                actual_event_bytes,
                minimum_event_bytes,
            }) if actual_event_bytes == actual && minimum_event_bytes == minimum
        ));
    }

    let minimum_event = event_with_declared_size(minimum as u32, minimum);
    validate_binlog_event_reader_safety(&minimum_event, &reader).unwrap();
}

fn event_with_declared_size(declared: u32, actual: usize) -> Vec<u8> {
    assert!(actual >= BinlogEventHeader::LEN);
    let mut event = vec![0_u8; actual];
    event[9..13].copy_from_slice(&declared.to_le_bytes());
    event
}

fn table_map_event(table_id: u64) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&table_id.to_le_bytes()[..6]);
    body.extend_from_slice(&0_u16.to_le_bytes());
    body.extend_from_slice(&[1, b'd', 0, 1, b't', 0, 1]);
    body.push(mysql_common::constants::ColumnType::MYSQL_TYPE_LONG as u8);
    body.extend_from_slice(&[0, 0]);
    let event_bytes = BinlogEventHeader::LEN + body.len();
    let header = BinlogEventHeader::new(
        0,
        EventType::TABLE_MAP_EVENT,
        1,
        event_bytes as u32,
        0,
        EventFlags::empty(),
    );
    let mut event = Vec::with_capacity(event_bytes);
    header.serialize(&mut event);
    event.extend_from_slice(&body);
    event
}

fn crc32_event_reader() -> EventStreamReader {
    let event = crc32_format_description_event();
    let mut reader = EventStreamReader::new(BinlogVersion::Version4);
    reader.read(event.as_slice()).unwrap().unwrap();
    assert_eq!(
        reader.get_fde().footer().get_checksum_alg().unwrap(),
        Some(BinlogChecksumAlg::BINLOG_CHECKSUM_ALG_CRC32)
    );
    reader
}

fn crc32_format_description_event() -> Vec<u8> {
    let fde = FormatDescriptionEvent::new(BinlogVersion::Version4)
        .with_server_version(&b"8.0.36"[..])
        .with_footer(BinlogEventFooter::new(
            BinlogChecksumAlg::BINLOG_CHECKSUM_ALG_CRC32,
        ));
    let mut body = Vec::new();
    fde.serialize(&mut body);
    body.push(BinlogChecksumAlg::BINLOG_CHECKSUM_ALG_CRC32 as u8);
    body.extend_from_slice(&[0_u8; BinlogEventFooter::BINLOG_CHECKSUM_LEN]);
    let event_bytes = BinlogEventHeader::LEN + body.len();
    let header = BinlogEventHeader::new(
        0,
        EventType::FORMAT_DESCRIPTION_EVENT,
        1,
        event_bytes as u32,
        0,
        EventFlags::empty(),
    );
    let mut event = Vec::with_capacity(event_bytes);
    header.serialize(&mut event);
    event.extend_from_slice(&body);
    event
}
