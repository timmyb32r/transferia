use super::ClientMetadata;
use crate::{ArrowOptions, CompressionMethod};

#[test]
fn compressed_connections_pin_their_decoder_codec() {
    for (codec, expected) in [
        (CompressionMethod::LZ4, "lz4"),
        (CompressionMethod::ZSTD, "zstd"),
    ] {
        let metadata = ClientMetadata {
            client_id: 0,
            compression: codec,
            arrow_options: ArrowOptions::strict(),
        };
        let settings = metadata.compression_settings().encode_to_key_value_strings();
        assert!(settings.contains(&("network_compression_method".into(), expected.into())));
    }
}

#[test]
fn uncompressed_connections_do_not_request_network_compression() {
    let metadata = ClientMetadata {
        client_id: 0,
        compression: CompressionMethod::None,
        arrow_options: ArrowOptions::strict(),
    };
    assert!(metadata.compression_settings().0.is_empty());
}
