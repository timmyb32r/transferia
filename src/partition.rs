use ydb::TopicClient;

/// Discover partitions from YDB and compute this worker's subset via modulo.
///
/// # CRITICAL API requirements
///
/// - `DescribeTopicOptionsBuilder::default().build()?` (NOT `::default()`)
/// - Modulo assignment: `id.unsigned_abs() as u32 % total_workers == worker_index`
/// - Handle zero-partition: log warning, return `Ok(vec![])`
/// - Log partition assignment with `tracing::info!`
pub async fn discover_my_partitions(
    topic_client: &mut TopicClient,
    topic_path: &str,
    total_workers: u32,
    worker_index: u32,
) -> anyhow::Result<Vec<i64>> {
    // Use DescribeTopicOptionsBuilder — NOT ::default() on DescribeTopicOptions
    let options = ydb::DescribeTopicOptionsBuilder::default()
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to build DescribeTopicOptions: {}", e))?;

    let description = topic_client
        .describe_topic(topic_path.to_string(), options)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to describe topic '{}': {}", topic_path, e))?;

    // Collect all active partition IDs
    let all_partitions: Vec<i64> = description
        .partitions
        .iter()
        .filter(|p| p.active)
        .map(|p| p.partition_id)
        .collect();

    let active_count = all_partitions.len();

    // Compute this worker's subset via modulo
    // id.unsigned_abs() ensures non-negative modulo for i64
    let my_partitions: Vec<i64> = all_partitions
        .into_iter()
        .filter(|id| {
            let id_mod = id.unsigned_abs() as u32 % total_workers;
            id_mod == worker_index
        })
        .collect();

    if my_partitions.is_empty() {
        tracing::warn!(
            "Worker {}/{} assigned 0 partitions (total active: {}). Exiting.",
            worker_index,
            total_workers,
            active_count,
        );
    } else {
        tracing::info!(
            "Worker {}/{} assigned {} partitions: {:?}",
            worker_index,
            total_workers,
            my_partitions.len(),
            my_partitions,
        );
    }

    Ok(my_partitions)
}
