use super::*;

#[test]
fn parent_tokens_are_random_and_not_empty() -> anyhow::Result<()> {
    let first = random_token()?;
    let second = random_token()?;
    assert_eq!(first.len(), 64);
    assert_ne!(first, second);
    Ok(())
}
