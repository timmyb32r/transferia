use std::io::Read as _;

use prost::Message as _;

use super::{
    compress, request_message_count, spawn_compression, write_message, EncodedMessage, TopicCodec,
    MESSAGE_OVERHEAD_BYTES, WRITE_REQUEST_BUDGET,
};

#[test]
fn cancelled_compression_gives_its_payloads_back_before_the_job_runs() -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .max_blocking_threads(1)
        .build()?;
    let (release, blocked) = std::sync::mpsc::channel::<()>();
    runtime.block_on(async {
        // The only blocking thread is busy, so the compression job stays queued.
        let busy = tokio::task::spawn_blocking(move || blocked.recv());
        let (job, reclaim) = spawn_compression(TopicCodec::Gzip, vec![b"record\n".to_vec()]);

        drop(reclaim);
        release.send(())?;

        busy.await??;
        assert!(job.await??.is_empty());
        Ok(())
    })
}

#[test]
fn every_codec_keeps_the_payload_and_its_size() -> anyhow::Result<()> {
    let payload = b"record\n".repeat(100);

    let raw = compress(TopicCodec::Raw, payload.clone())?;
    let gzip = compress(TopicCodec::Gzip, payload.clone())?;
    let zstd = compress(TopicCodec::Zstd, payload.clone())?;

    let mut gunzipped = Vec::new();
    flate2::read::GzDecoder::new(gzip.data.as_slice()).read_to_end(&mut gunzipped)?;
    assert_eq!(raw.data, payload);
    assert_eq!(gunzipped, payload);
    assert_eq!(zstd::bulk::decompress(&zstd.data, payload.len())?, payload);
    for message in [&raw, &gzip, &zstd] {
        assert_eq!(message.uncompressed_size, payload.len());
    }
    assert!(gzip.data.len() < payload.len() && zstd.data.len() < payload.len());
    Ok(())
}

#[test]
fn request_takes_the_first_message_whatever_its_size() {
    assert_eq!(request_message_count(std::iter::empty()), 0);
    assert_eq!(
        request_message_count([WRITE_REQUEST_BUDGET * 2, 1].into_iter()),
        1
    );
}

#[test]
fn request_is_filled_up_to_its_budget() {
    let half = WRITE_REQUEST_BUDGET / 2 - MESSAGE_OVERHEAD_BYTES;
    assert_eq!(request_message_count([half, half, 1].into_iter()), 2);
    assert_eq!(request_message_count([half, half + 1, 1].into_iter()), 1);
}

#[test]
fn small_messages_are_limited_by_their_framing() {
    let count = request_message_count(std::iter::repeat_n(1, 1_000_000));
    assert_eq!(count, WRITE_REQUEST_BUDGET / (1 + MESSAGE_OVERHEAD_BYTES));
}

#[test]
fn framing_of_a_message_stays_within_the_assumed_overhead() -> anyhow::Result<()> {
    const MESSAGES: usize = 1_000;
    let messages = (0..MESSAGES)
        .map(|_| {
            let message = EncodedMessage {
                data: vec![0],
                uncompressed_size: usize::try_from(i64::MAX)?,
            };
            Ok((i64::MAX, message))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    let request = write_message(TopicCodec::Gzip, messages)?;

    assert!(request.encoded_len() <= MESSAGES * (1 + MESSAGE_OVERHEAD_BYTES));
    Ok(())
}
