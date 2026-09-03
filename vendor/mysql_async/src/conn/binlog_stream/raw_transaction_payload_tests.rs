use futures_util::StreamExt;
use mysql_common::{
    binlog::{
        consts::{EventFlags, EventType, TransactionPayloadCompressionType},
        events::{BinlogEventHeader, TransactionPayloadEvent},
    },
    proto::MySerialize,
};
use tokio::{io::AsyncWriteExt, net::TcpListener};

use super::BinlogStream;
use crate::{io::Stream, opts::HostPortOrUrl, Conn, Opts};

#[tokio::test]
async fn transaction_payload_is_returned_without_expanding_embedded_events() {
    // Zstandard encoding of a header-only STOP_EVENT. The old stream implementation
    // expanded this into a second event before reading the next network packet.
    const COMPRESSED_STOP_EVENT: &[u8] = &[
        0x28, 0xb5, 0x2f, 0xfd, 0x04, 0x58, 0x99, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,
        0x01, 0x00, 0x00, 0x00, 0x13, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x7d, 0x09, 0xe5, 0x6e,
    ];

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let endpoint = HostPortOrUrl::HostPort {
        host: address.ip().to_string(),
        port: address.port(),
        resolved_ips: Some(vec![address.ip()]),
    };

    let client = Stream::connect_tcp(&endpoint, None).await.unwrap();
    let (mut server, _) = listener.accept().await.unwrap();

    let transaction_payload = TransactionPayloadEvent::new(
        COMPRESSED_STOP_EVENT.len() as u64,
        TransactionPayloadCompressionType::ZSTD,
        BinlogEventHeader::LEN as u64,
        COMPRESSED_STOP_EVENT,
    );
    let mut event_body = Vec::new();
    transaction_payload.serialize(&mut event_body);

    let event_header = BinlogEventHeader::new(
        0,
        EventType::TRANSACTION_PAYLOAD_EVENT,
        1,
        (BinlogEventHeader::LEN + event_body.len()) as u32,
        0,
        EventFlags::empty(),
    );
    let mut event = Vec::new();
    event_header.serialize(&mut event);
    event.extend_from_slice(&event_body);

    let mut network_payload = Vec::with_capacity(1 + event.len());
    network_payload.push(0);
    network_payload.extend_from_slice(&event);
    write_packet(&mut server, 0, &network_payload).await;
    write_packet(&mut server, 1, &[0xfe, 0, 0, 0, 0]).await;

    let mut conn = Conn::empty(Opts::default());
    conn.inner.stream = Some(client);
    let mut stream = BinlogStream::new(conn, event.len());

    let event = stream.next().await.unwrap().unwrap();
    assert_eq!(
        event.header().event_type().unwrap(),
        EventType::TRANSACTION_PAYLOAD_EVENT
    );
    let transaction_payload = event.read_event::<TransactionPayloadEvent<'_>>().unwrap();
    assert_eq!(transaction_payload.payload_raw(), COMPRESSED_STOP_EVENT);
    assert_eq!(transaction_payload.uncompressed_size(), BinlogEventHeader::LEN as u64);

    assert!(stream.next().await.is_none());
}

async fn write_packet(server: &mut tokio::net::TcpStream, sequence_id: u8, payload: &[u8]) {
    let payload_len = payload.len();
    let header = [
        payload_len as u8,
        (payload_len >> 8) as u8,
        (payload_len >> 16) as u8,
        sequence_id,
    ];
    server.write_all(&header).await.unwrap();
    server.write_all(payload).await.unwrap();
}
