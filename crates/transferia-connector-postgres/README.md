# PostgreSQL replication settings

`replication.plugin` appears as **Plugin** in Advanced settings:

- `auto` (default): check both output-plugin libraries using temporary logical
  slots; prefer `pgoutput`, otherwise use `wal2json`. Permission failures,
  timeouts, missing WAL, and other errors are not evidence of an absent plugin
  and do not cause a fallback. Probing needs a free replication-slot entry.
- `pgoutput`: use the explicitly configured existing `publication`.
- `wal2json`: use the server's installed `wal2json` output plugin.

The permanent replication slot is always named exactly after the actual transfer
ID (for example, `dttabc123`), without a prefix or normalization. Slot names are
not configurable. IDs incompatible with PostgreSQL slot-name rules fail before
connecting or modifying replication resources.

When auto selects `pgoutput`, Transferia creates a publication named exactly
after the transfer ID for the selected tables. This requires the database and
table privileges needed by `CREATE PUBLICATION`. Creation and its ownership
comment are transactional. An existing publication is reused only when its
ownership and table contract match; unrelated publications are not adopted,
altered, or dropped. Changing the table set of an existing auto publication
requires deliberate operator action, rather than silently changing its feed.
If the publication disappears while the slot still exists, startup fails rather
than recreating the publication behind an existing replication position.

Automatic publications include all change actions. Unsupported `TRUNCATE`
events stop the reader before offset acknowledgement; they are not silently
excluded from the feed. Publication contracts are also rechecked during reading.

On restart, auto preserves the plugin of an existing slot even if another plugin
has since become available. Existing durable offset, source identity, and
execution-ownership checks remain mandatory. No slots are reset or recreated
to switch plugins. Existing custom slots are not renamed or deleted by this
configuration change; old `slot` and `decoder` configuration keys are rejected.

Minimal automatic replication configuration:

```yaml
replication: {}
```

Explicit plugin selection:

```yaml
replication:
  plugin:
    type: pgoutput
    publication: my_existing_publication
```
