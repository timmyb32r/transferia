use super::*;

#[test]
fn confluent_schema_id_envelope_is_big_endian_and_lossless() -> anyhow::Result<()> {
    let bytes = ConfluentEnvelope::encode(0x0102_0304, b"payload")?;
    assert_eq!(&bytes[..5], &[0, 1, 2, 3, 4]);
    let decoded = ConfluentEnvelope::decode(&bytes)?;
    assert_eq!(decoded.schema_id, 0x0102_0304);
    assert_eq!(decoded.payload, b"payload");
    Ok(())
}

#[test]
fn protobuf_message_indexes_cover_optimized_and_nested_forms() -> anyhow::Result<()> {
    for indexes in [vec![0], vec![1], vec![2, 1, 0]] {
        let mut encoded = Vec::new();
        encode_message_indexes(&indexes, &mut encoded)?;
        encoded.extend_from_slice(b"payload");
        let (decoded, payload) = decode_message_indexes(&encoded)?;
        assert_eq!(decoded, indexes);
        assert_eq!(payload, b"payload");
    }
    Ok(())
}

#[test]
fn registry_configuration_rejects_ambiguous_or_credential_bearing_urls() {
    for url in ["ftp://registry", "https://user:secret@registry"] {
        let config = SchemaRegistryConnection {
            url: url.to_owned(),
            request_timeout_ms: 1_000,
            auth: SchemaRegistryAuth::None,
            ca_certificate: None,
        };
        assert!(config.validate().is_err());
    }
}

#[test]
fn registry_configuration_rejects_empty_or_padded_url() {
    for url in ["", " http://registry", "http://registry "] {
        let config = SchemaRegistryConnection {
            url: url.to_owned(),
            request_timeout_ms: 1_000,
            auth: SchemaRegistryAuth::None,
            ca_certificate: None,
        };
        assert!(config.validate().is_err());
    }
}

#[tokio::test]
async fn registry_client_resolves_recursive_references_once() -> anyhow::Result<()> {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move {
        let mut paths = Vec::new();
        for _ in 0..4 {
            let (mut stream, _) = listener.accept().await?;
            let mut request = vec![0_u8; 8 * 1024];
            let read = stream.read(&mut request).await?;
            let request = String::from_utf8(request[..read].to_vec())?;
            let path = request
                .lines()
                .next()
                .and_then(|line| line.split_whitespace().nth(1))
                .ok_or_else(|| anyhow::anyhow!("mock received an invalid HTTP request"))?
                .to_owned();
            let body = match path.as_str() {
                "/schemas/ids/7" | "/schemas/ids/7?format=serialized" => serde_json::json!({
                    "schemaType": "PROTOBUF",
                    "schema": "syntax = \"proto3\"; import \"middle.proto\"; message Root { Middle middle = 1; }",
                    "references": [{"name":"middle.proto","subject":"middle","version":1}]
                }),
                "/subjects/middle/versions/1" => serde_json::json!({
                    "schemaType": "PROTOBUF",
                    "schema": "syntax = \"proto3\"; import \"common.proto\"; message Middle { Common common = 1; }",
                    "references": [{"name":"common.proto","subject":"common","version":1}]
                }),
                "/subjects/common/versions/1" => serde_json::json!({
                    "schemaType": "PROTOBUF",
                    "schema": "syntax = \"proto3\"; message Common { string value = 1; }"
                }),
                other => anyhow::bail!("unexpected mock request path {other}"),
            };
            paths.push(path);
            let body = serde_json::to_vec(&body)?;
            stream
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await?;
            stream.write_all(&body).await?;
        }
        anyhow::Ok(paths)
    });

    let client = RegistryClient::new(&SchemaRegistryConnection {
        url: format!("http://{address}"),
        request_timeout_ms: 5_000,
        auth: SchemaRegistryAuth::None,
        ca_certificate: None,
    })?;
    let schema = client.schema_by_id(7).await?;
    assert_eq!(schema.references.len(), 2);
    assert_eq!(schema.references[0].name, "common.proto");
    assert_eq!(schema.references[1].name, "middle.proto");

    let paths = server.await??;
    assert_eq!(paths.iter().filter(|path| path.contains("middle")).count(), 1);
    assert_eq!(paths.iter().filter(|path| path.contains("common")).count(), 1);
    Ok(())
}
