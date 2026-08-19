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
