use mysql_common::proto::codec::error::PacketCodecError;

use super::{DriverError, Error};

#[test]
fn packet_too_large_classifier_covers_driver_and_codec_errors() {
    assert!(Error::from(DriverError::PacketTooLarge).is_packet_too_large());
    assert!(Error::from(PacketCodecError::PacketTooLarge).is_packet_too_large());
}

#[test]
fn packet_too_large_classifier_does_not_hide_transport_failures() {
    let error = Error::from(PacketCodecError::Io(std::io::Error::new(
        std::io::ErrorKind::ConnectionReset,
        "fixture transport reset",
    )));
    assert!(!error.is_packet_too_large());
}
