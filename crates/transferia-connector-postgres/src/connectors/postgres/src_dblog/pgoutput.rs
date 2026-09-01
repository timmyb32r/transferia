use std::collections::HashMap;
use std::sync::Arc;

use bytes::{Buf, Bytes};
use transferia_core::ChangeOperation;

use super::event::{ChangeEvent, LogicalValue, OldValuesKind, Relation, RelationColumn};

const POSTGRES_EPOCH_UNIX_MICROS: i64 = 946_684_800_000_000;

#[derive(Default)]
pub(super) struct PgOutputDecoder {
    relations: HashMap<u32, Arc<Relation>>,
    transaction: Option<Transaction>,
}

struct Transaction {
    xid: u32,
    commit_timestamp_micros: i64,
    changes: Vec<PendingChange>,
}

struct PendingChange {
    relation: Arc<Relation>,
    operation: ChangeOperation,
    values: Vec<LogicalValue>,
    old_values: Option<Vec<LogicalValue>>,
    old_values_kind: Option<OldValuesKind>,
}

pub(super) struct PgOutputEvent {
    pub event: ChangeEvent,
    pub relation: Arc<Relation>,
}

impl PgOutputDecoder {
    pub(super) fn decode(&mut self, data: &[u8]) -> anyhow::Result<Vec<PgOutputEvent>> {
        let (&tag, payload) = data
            .split_first()
            .ok_or_else(|| anyhow::anyhow!("empty pgoutput message"))?;
        let mut input = Bytes::copy_from_slice(payload);
        match tag {
            b'B' => self.begin(&mut input)?,
            b'C' => return self.commit(&mut input),
            b'R' => self.relation(&mut input)?,
            b'I' => self.insert(&mut input)?,
            b'U' => self.update(&mut input)?,
            b'D' => self.delete(&mut input)?,
            b'O' | b'Y' => {}
            b'T' => anyhow::bail!("pgoutput TRUNCATE is not representable as row changes"),
            other => anyhow::bail!("unsupported pgoutput message tag 0x{other:02x}"),
        }
        anyhow::ensure!(input.is_empty(), "pgoutput message has trailing bytes");
        Ok(Vec::new())
    }

    fn begin(&mut self, input: &mut Bytes) -> anyhow::Result<()> {
        anyhow::ensure!(self.transaction.is_none(), "nested pgoutput transaction");
        let _final_lsn = take_u64(input)?;
        let commit_time = take_i64(input)?;
        let xid = take_u32(input)?;
        self.transaction = Some(Transaction {
            xid,
            commit_timestamp_micros: commit_time
                .checked_add(POSTGRES_EPOCH_UNIX_MICROS)
                .ok_or_else(|| anyhow::anyhow!("pgoutput commit timestamp overflow"))?,
            changes: Vec::new(),
        });
        Ok(())
    }

    fn commit(&mut self, input: &mut Bytes) -> anyhow::Result<Vec<PgOutputEvent>> {
        let _flags = take_u8(input)?;
        let _commit_lsn = take_u64(input)?;
        let end_lsn = take_u64(input)?;
        let commit_time = take_i64(input)?;
        let transaction = self
            .transaction
            .take()
            .ok_or_else(|| anyhow::anyhow!("pgoutput COMMIT without BEGIN"))?;
        let commit_timestamp_micros = commit_time
            .checked_add(POSTGRES_EPOCH_UNIX_MICROS)
            .ok_or_else(|| anyhow::anyhow!("pgoutput commit timestamp overflow"))?;
        anyhow::ensure!(
            transaction.commit_timestamp_micros == commit_timestamp_micros,
            "pgoutput BEGIN/COMMIT timestamp mismatch"
        );
        Ok(transaction
            .changes
            .into_iter()
            .map(|change| PgOutputEvent {
                event: ChangeEvent {
                    schema: Arc::clone(&change.relation.schema),
                    table: Arc::clone(&change.relation.table),
                    operation: change.operation,
                    values: change.values,
                    old_values: change.old_values,
                    old_values_kind: change.old_values_kind,
                    lsn: end_lsn,
                    transaction_id: transaction.xid,
                    commit_timestamp_micros,
                },
                relation: change.relation,
            })
            .collect())
    }

    fn relation(&mut self, input: &mut Bytes) -> anyhow::Result<()> {
        let relation_id = take_u32(input)?;
        let schema = Arc::from(take_cstring(input)?);
        let table = Arc::from(take_cstring(input)?);
        let replica_identity = take_u8(input)?;
        let column_count = usize::from(take_u16(input)?);
        let mut columns = Vec::with_capacity(column_count);
        for _ in 0..column_count {
            columns.push(RelationColumn {
                key: take_u8(input)? & 1 == 1,
                name: Arc::from(take_cstring(input)?),
                type_oid: take_u32(input)?,
            });
            let _type_modifier = take_i32(input)?;
        }
        self.relations.insert(
            relation_id,
            Arc::new(Relation {
                schema,
                table,
                replica_identity,
                columns: columns.into(),
            }),
        );
        Ok(())
    }

    fn insert(&mut self, input: &mut Bytes) -> anyhow::Result<()> {
        let relation = self.take_relation(input)?;
        expect_tag(input, b'N')?;
        let values = take_tuple(input, relation.columns.len())?;
        self.push_change(relation, ChangeOperation::Create, values, None, None)
    }

    fn update(&mut self, input: &mut Bytes) -> anyhow::Result<()> {
        let relation = self.take_relation(input)?;
        let (old_values, old_values_kind) = match peek_u8(input)? {
            tag @ (b'K' | b'O') => {
                input.advance(1);
                (
                    Some(take_tuple(input, relation.columns.len())?),
                    Some(if tag == b'K' {
                        OldValuesKind::Key
                    } else {
                        OldValuesKind::Full
                    }),
                )
            }
            b'N' => (None, None),
            other => anyhow::bail!("invalid pgoutput UPDATE tuple tag 0x{other:02x}"),
        };
        expect_tag(input, b'N')?;
        let values = take_tuple(input, relation.columns.len())?;
        self.push_change(
            relation,
            ChangeOperation::Update,
            values,
            old_values,
            old_values_kind,
        )
    }

    fn delete(&mut self, input: &mut Bytes) -> anyhow::Result<()> {
        let relation = self.take_relation(input)?;
        let old_values_kind = match take_u8(input)? {
            b'K' => OldValuesKind::Key,
            b'O' => OldValuesKind::Full,
            other => anyhow::bail!("invalid pgoutput DELETE tuple tag 0x{other:02x}"),
        };
        let old_values = take_tuple(input, relation.columns.len())?;
        self.push_change(
            relation,
            ChangeOperation::Delete,
            vec![LogicalValue::Null; old_values.len()],
            Some(old_values),
            Some(old_values_kind),
        )
    }

    fn take_relation(&self, input: &mut Bytes) -> anyhow::Result<Arc<Relation>> {
        let relation_id = take_u32(input)?;
        self.relations
            .get(&relation_id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("pgoutput references unknown relation {relation_id}"))
    }

    fn push_change(
        &mut self,
        relation: Arc<Relation>,
        operation: ChangeOperation,
        values: Vec<LogicalValue>,
        old_values: Option<Vec<LogicalValue>>,
        old_values_kind: Option<OldValuesKind>,
    ) -> anyhow::Result<()> {
        self.transaction
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("pgoutput row change outside a transaction"))?
            .changes
            .push(PendingChange {
                relation,
                operation,
                values,
                old_values,
                old_values_kind,
            });
        Ok(())
    }
}

fn take_tuple(input: &mut Bytes, expected_columns: usize) -> anyhow::Result<Vec<LogicalValue>> {
    let count = usize::from(take_u16(input)?);
    anyhow::ensure!(
        count == expected_columns,
        "pgoutput tuple has {count} columns, relation declares {expected_columns}"
    );
    (0..count)
        .map(|_| match take_u8(input)? {
            b'n' => Ok(LogicalValue::Null),
            b'u' => Ok(LogicalValue::UnchangedToast),
            b't' => Ok(LogicalValue::Text(take_sized(input)?)),
            b'b' => Ok(LogicalValue::Binary(take_sized(input)?)),
            other => anyhow::bail!("invalid pgoutput tuple value tag 0x{other:02x}"),
        })
        .collect()
}

fn take_sized(input: &mut Bytes) -> anyhow::Result<Bytes> {
    let len = usize::try_from(take_u32(input)?)?;
    anyhow::ensure!(input.remaining() >= len, "truncated pgoutput value");
    Ok(input.split_to(len))
}

fn take_cstring(input: &mut Bytes) -> anyhow::Result<String> {
    let end = input
        .iter()
        .position(|byte| *byte == 0)
        .ok_or_else(|| anyhow::anyhow!("unterminated pgoutput string"))?;
    let value = input.split_to(end);
    input.advance(1);
    Ok(std::str::from_utf8(&value)?.to_owned())
}

fn expect_tag(input: &mut Bytes, expected: u8) -> anyhow::Result<()> {
    let actual = take_u8(input)?;
    anyhow::ensure!(actual == expected, "expected pgoutput tag 0x{expected:02x}, received 0x{actual:02x}");
    Ok(())
}

fn peek_u8(input: &Bytes) -> anyhow::Result<u8> {
    input
        .first()
        .copied()
        .ok_or_else(|| anyhow::anyhow!("truncated pgoutput message"))
}

fn take_u8(input: &mut Bytes) -> anyhow::Result<u8> {
    anyhow::ensure!(input.has_remaining(), "truncated pgoutput message");
    Ok(input.get_u8())
}

fn take_u16(input: &mut Bytes) -> anyhow::Result<u16> {
    anyhow::ensure!(input.remaining() >= 2, "truncated pgoutput message");
    Ok(input.get_u16())
}

fn take_u32(input: &mut Bytes) -> anyhow::Result<u32> {
    anyhow::ensure!(input.remaining() >= 4, "truncated pgoutput message");
    Ok(input.get_u32())
}

fn take_i32(input: &mut Bytes) -> anyhow::Result<i32> {
    anyhow::ensure!(input.remaining() >= 4, "truncated pgoutput message");
    Ok(input.get_i32())
}

fn take_u64(input: &mut Bytes) -> anyhow::Result<u64> {
    anyhow::ensure!(input.remaining() >= 8, "truncated pgoutput message");
    Ok(input.get_u64())
}

fn take_i64(input: &mut Bytes) -> anyhow::Result<i64> {
    anyhow::ensure!(input.remaining() >= 8, "truncated pgoutput message");
    Ok(input.get_i64())
}
