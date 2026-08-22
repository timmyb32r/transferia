use bytes::Bytes;

use super::*;
use crate::connectors::s3::sink::object_key::ObjectKey;
use crate::durable::test_support;

fn object(key: &str, payload: &'static [u8]) -> ClosedObject {
    ClosedObject {
        epoch_id: 0,
        key: ObjectKey::parse(key).unwrap(),
        payload: Bytes::from_static(payload),
        rows: 1,
    }
}

#[tokio::test]
async fn epoch_transitions_open_to_closed_and_replay_skips_upload() -> anyhow::Result<()> {
    let durable = test_support::context();
    let objects = [object("events/p=0/a.json", b"one")];
    let journal = EpochJournal::new(Arc::clone(&durable.storage), 0, &objects)?;
    assert_eq!(journal.ensure_open().await?, OpenDisposition::Upload);
    journal.mark_closed().await?;

    let replay = EpochJournal::new(Arc::clone(&durable.storage), 0, &objects)?;
    assert_eq!(replay.key(), journal.key());
    assert_eq!(replay.ensure_open().await?, OpenDisposition::AlreadyClosed);
    replay.mark_closed().await?;
    Ok(())
}

#[tokio::test]
async fn object_key_identity_detects_payload_drift_instead_of_forking_state() -> anyhow::Result<()>
{
    let durable = test_support::context();
    let first = EpochJournal::new(
        Arc::clone(&durable.storage),
        0,
        &[object("events/p=0/a.json", b"one")],
    )?;
    let changed_payload = EpochJournal::new(
        Arc::clone(&durable.storage),
        0,
        &[object("events/p=0/a.json", b"two")],
    )?;
    let changed_key =
        EpochJournal::new(durable.storage, 0, &[object("events/p=0/b.json", b"one")])?;
    assert_eq!(first.key(), changed_payload.key());
    assert_ne!(first.key(), changed_key.key());
    assert_eq!(first.ensure_open().await?, OpenDisposition::Upload);
    let error = changed_payload.ensure_open().await.unwrap_err();
    assert!(error
        .to_string()
        .contains("does not match replayed object keys and payloads"));
    Ok(())
}
