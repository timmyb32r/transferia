# PostgreSQL UPDATE column presence

## Normalized source values

`SchemaColumn::update_value_presence` and `delete_value_presence` describe
availability on the **output of the source**, after reconstruction, separately
from physical tuple presence below. Both use the connector-neutral `ValuePresence`:
`Guaranteed` includes a present SQL NULL; `MayBeAbsent` makes no promise. They
describe the current/new value for UPDATE and the deleted value for DELETE,
not UPDATE's old-value control columns or whether a value actually changed.

PostgreSQL's shared metadata assembly sets both guarantees for every user column
under `REPLICA IDENTITY FULL`. The replication reader requires a complete full old
tuple, restores unchanged UPDATE values from it, and uses it for DELETE values.
It validates availability before constructing Arrow arrays. An incomplete old
tuple or an unresolved unchanged marker fails instead of becoming an Arrow NULL.

With DEFAULT identity, fixed-width columns are guaranteed on UPDATE; primary-key
columns are guaranteed on DELETE. Other columns conservatively have no guarantee.
These properties accompany the stable snapshot/CDC schema; they do not imply that
an append-only stream produces UPDATE or DELETE events. Other connectors default
to `MayBeAbsent` until they explicitly establish the contract.

Arrow metadata keys `transferia.update_value_presence` and
`transferia.delete_value_presence` contain `guaranteed` only for positive
guarantees. Missing keys mean no guarantee. Schema compatibility and runtime
discovery comparison include both properties. Transformations retaining column
semantics retain them; derived columns must prove availability before setting them.
These declarations are not the per-row changed-column mask and must not be
interpreted as evidence that an UPDATE changed the value.

## Physical UPDATE tuple values

`SchemaColumn::always_present_on_update` is a positive, connector-neutral
guarantee: every UPDATE new tuple contains this column. A present SQL NULL
counts as present. False means unknown/not guaranteed. It says nothing about
DELETE, old tuples, nullability, or whether the value actually changed.

The guarantee is exported in Arrow field metadata as
`transferia.always_present_on_update=true`. Consumers must not infer it from
the Arrow storage type. A transformation creating a new column must prove
this property before advertising it; metadata alone is not reconstruction of
an omitted value.

PostgreSQL discovery uses `pg_attribute.attlen > 0`, in both individual and
batched metadata reads. This covers fixed-width built-ins (boolean, integers,
floats, UUID, date/time/timestamp, interval, name), enums, and fixed-width domains
or extension types without maintaining an OID whitelist. SQL casts to Arrow
strings do not change the original physical storage classification.

Variable-width types (`attlen = -1`) receive no guarantee, even for short values,
small typmods, or current PLAIN storage. This is deliberately conservative:
not every variable-width type/value will actually be toasted. Current storage
settings are not proof about all already-stored tuples. Numeric, text, bytea,
arrays, JSON and ranges must not be classified by their typical value size.

Evidence inspected in the local PostgreSQL checkout:

- `src/backend/replication/logical/proto.c`, `logicalrep_write_tuple`: the
  unchanged marker requires `attlen == -1` and an on-disk external datum.
- `src/backend/access/heap/heaptoast.c`: EXTENDED/EXTERNAL and, as a last resort,
  MAIN attributes can be stored externally.
- `src/include/catalog/pg_type.dat`: physical lengths of built-in types.

Before constructing any Arrow arrays, the PostgreSQL replication reader checks
tuple widths and rejects an unchanged-TOAST marker on a guaranteed UPDATE
column. The check is shared by pgoutput and wal2json, and runs before FULL
replica-identity old-value fallback, so fallback cannot hide a violation.
Snapshots retain the same field metadata as CDC. General runtime schema
validation rejects metadata inconsistent with discovery.
