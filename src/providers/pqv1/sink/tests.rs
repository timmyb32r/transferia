use super::writer::validate_ack;

#[test]
fn acknowledgements_must_match_the_exact_write_sequence_set() -> anyhow::Result<()> {
    validate_ack(&[3, 2, 1], &[1, 2, 3])?;
    assert!(validate_ack(&[1, 2], &[1, 2, 3]).is_err());
    assert!(validate_ack(&[1, 2, 4], &[1, 2, 3]).is_err());
    Ok(())
}
