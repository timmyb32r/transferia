-- ClickHouse table definitions for YDB Topic → ClickHouse Replicator PoC

-- Main target table (matches the Arrow schema from config.yaml column mappings)
CREATE TABLE IF NOT EXISTS events (
    user_id Int64,
    event_name String,
    created_at DateTime64(3)
) ENGINE = MergeTree()
ORDER BY (created_at, user_id);

-- Dead Letter Queue table — stores malformed/unparseable JSON messages
-- Schema: raw_bytes (original JSON), error_message, partition_id, timestamp
CREATE TABLE IF NOT EXISTS events_dlq (
    raw_bytes String,
    error_message String,
    partition_id Int64,
    timestamp String
) ENGINE = MergeTree()
ORDER BY (timestamp, partition_id);
