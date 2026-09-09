use std::sync::Arc;

use tokio_util::sync::CancellationToken;
use transferia_core::data::message::Message;
use transferia_core::TableData;
use transferia_delivery_contracts::parser::ParserFactory;
use transferia_registry::TableSampleLimits;

/// Use the production parser, including system columns and DLQ policy. Never
/// parse a truncated transport payload or a parser-autodetection approximation.
pub async fn parse_sample(
    factory: Arc<dyn ParserFactory>,
    messages: Vec<Message>,
    limits: TableSampleLimits,
    cancellation: CancellationToken,
) -> anyhow::Result<Vec<TableData>> {
    limits.validate()?;
    let task = tokio::task::spawn_blocking(move || {
        anyhow::ensure!(!cancellation.is_cancelled(), "Source sample cancelled");
        let retained = messages.iter().try_fold(0_usize, |size, message| {
            let size = size.checked_add(message.value.len())?
                .checked_add(message.key.as_ref().map_or(0, |key| key.len()))?
                .checked_add(message.meta.topic.as_ref().map_or(0, |topic| topic.len()))?;
            message.headers.iter().try_fold(size, |size, header| {
                size.checked_add(header.key.len())?.checked_add(header.value.as_ref().map_or(0, |value| value.len()))
            })
        }).ok_or_else(|| anyhow::anyhow!("Source sample byte count overflow"))?;
        let mut parser = factory.create_session(limits.max_bytes);
        let allocation = retained.checked_add(parser.output_memory_bound(&messages))
            .ok_or_else(|| anyhow::anyhow!("Source sample allocation overflow"))?;
        limits.check_bytes(allocation)?;
        let (main, dlq) = parser.parse_into(messages)?;
        anyhow::ensure!(!cancellation.is_cancelled(), "Source sample cancelled");
        let tables = std::iter::once(main).chain(dlq).collect::<Vec<_>>();
        let bytes = tables.iter().try_fold(0_usize, |bytes, table| bytes.checked_add(table.batch.get_array_memory_size()))
            .ok_or_else(|| anyhow::anyhow!("Source sample Arrow byte count overflow"))?;
        limits.check_bytes(bytes)?;
        Ok(tables)
    });
    task.await?
}

#[cfg(test)]
#[path = "tests/source_sample.rs"]
mod tests;
