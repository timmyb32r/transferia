//! Source key syntax is independent of the destination's identifier whitelist.

pub(super) fn key_columns(expression: &str) -> anyhow::Result<Vec<String>> {
    let expression = expression.trim();
    let mut remaining = expression
        .strip_prefix("tuple(")
        .or_else(|| expression.strip_prefix('('))
        .and_then(|body| body.strip_suffix(')'))
        .unwrap_or(expression)
        .trim();
    if remaining.is_empty() {
        return Ok(Vec::new());
    }
    let mut names = Vec::new();
    loop {
        let (name, rest) = if remaining.starts_with(['`', '"']) {
            clickhouse_arrow::Type::parse_quoted_identifier(remaining)?
        } else {
            let end = remaining.find(',').unwrap_or(remaining.len());
            let name = remaining[..end].trim();
            // Unquoted dotted names can refer to flattened Nested columns.
            // Other characters must be quoted so expressions cannot be mistaken
            // for column references merely because their text matches a name.
            anyhow::ensure!(name.split('.').all(|part| {
                let mut bytes = part.bytes();
                bytes.next().is_some_and(|byte| byte == b'_' || byte.is_ascii_alphabetic())
                    && bytes.all(|byte| byte == b'_' || byte.is_ascii_alphanumeric())
            }), "expected a column identifier, not an expression");
            (name.to_owned(), &remaining[end..])
        };
        names.push(name);
        let rest = rest.trim_start();
        if rest.is_empty() {
            return Ok(names);
        }
        remaining = rest.strip_prefix(',')
            .ok_or_else(|| anyhow::anyhow!("expected a comma after the column identifier"))?
            .trim_start();
        anyhow::ensure!(!remaining.is_empty(), "missing column identifier after comma");
    }
}
