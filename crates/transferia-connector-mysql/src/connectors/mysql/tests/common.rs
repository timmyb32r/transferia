use mysql_async::prelude::Queryable as _;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

#[tokio::test]
async fn cancelled_table_sample_closes_pending_socket_without_draining() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (pending_tx, pending_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        // Protocol 4.1, secure native-password handshake. This wire peer never
        // finishes the final SELECT, making accidental background drains visible.
        let mut greeting = b"\x0a8.4.6\0".to_vec();
        greeting.extend_from_slice(&42_u32.to_le_bytes());
        greeting.extend_from_slice(b"12345678\0");
        greeting.extend_from_slice(&0x8201_u16.to_le_bytes());
        greeting.push(45);
        greeting.extend_from_slice(&2_u16.to_le_bytes());
        greeting.extend_from_slice(&[0; 13]);
        greeting.extend_from_slice(b"abcdefghijkl\0");
        write_packet(&mut socket, 0, &greeting).await;
        let _authentication = read_packet(&mut socket).await;
        write_packet(&mut socket, 2, &[0, 0, 0, 2, 0, 0, 0]).await;
        let settings = read_packet(&mut socket).await;
        assert_eq!(settings, b"\x03SELECT @@wait_timeout");
        write_column(&mut socket).await;
        write_packet(&mut socket, 4, b"\x0528800").await;
        write_packet(&mut socket, 5, &[0xfe, 0, 0, 2, 0]).await;
        let query = read_packet(&mut socket).await;
        assert_eq!(query, b"\x03SELECT SLEEP(60)");
        write_column(&mut socket).await;
        let mut byte = [0];
        tokio::time::timeout(std::time::Duration::from_secs(1), socket.read(&mut byte))
            .await.expect("cancelled sample left its MySQL socket draining")
            .map_or_else(|error| {
                assert_eq!(error.kind(), std::io::ErrorKind::ConnectionReset);
                0
            }, |count| count)
    });
    let task = tokio::spawn(async move {
        let mut connection = super::connect_sample_with_max_allowed_packet(&super::MySqlConnectionConfig {
            host: address.ip().to_string(), port: address.port(), database: String::new(),
            username: "reader".into(), password: String::new(), trusted_plaintext: true,
            tls_ca_file: None,
        }, 1024 * 1024).await.unwrap();
        let mut rows = connection.query_iter("SELECT SLEEP(60)").await.unwrap();
        pending_tx.send(()).unwrap();
        rows.next().await.unwrap();
    });
    pending_rx.await.unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(server.await.unwrap(), 0);
}

async fn write_column(socket: &mut tokio::net::TcpStream) {
    write_packet(socket, 1, &[1]).await;
    let mut column = b"\x03def\0\0\0\x05value\0\x0c".to_vec();
    column.extend_from_slice(&63_u16.to_le_bytes());
    column.extend_from_slice(&20_u32.to_le_bytes());
    column.extend_from_slice(&[8, 0, 0, 0, 0, 0]);
    write_packet(socket, 2, &column).await;
    write_packet(socket, 3, &[0xfe, 0, 0, 2, 0]).await;
}

async fn write_packet(socket: &mut tokio::net::TcpStream, sequence: u8, body: &[u8]) {
    let size = u32::try_from(body.len()).unwrap().to_le_bytes();
    socket.write_all(&[size[0], size[1], size[2], sequence]).await.unwrap();
    socket.write_all(body).await.unwrap();
}

async fn read_packet(socket: &mut tokio::net::TcpStream) -> Vec<u8> {
    let mut header = [0; 4];
    socket.read_exact(&mut header).await.unwrap();
    let length = u32::from_le_bytes([header[0], header[1], header[2], 0]);
    let mut body = vec![0; usize::try_from(length).unwrap()];
    socket.read_exact(&mut body).await.unwrap();
    body
}
