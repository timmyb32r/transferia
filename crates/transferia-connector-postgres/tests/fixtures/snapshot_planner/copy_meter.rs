//! Bounded COPY framing meter for the release evaluator. Field payloads are
//! consumed without decoding/copying; exact value comparisons belong to E2E.

#[derive(Clone, Copy)]
enum Stage { Header, Extension(u32), Row, FieldLength, FieldBody(u32), Done }

pub struct CopyMeter {
    stage: Stage,
    scratch: [u8; 19],
    used: usize,
    expected_fields: i16,
    fields_left: i16,
    pub rows: u64,
    pub bytes: u64,
}

impl CopyMeter {
    pub fn new(fields: usize) -> anyhow::Result<Self> {
        let expected_fields = i16::try_from(fields)?;
        anyhow::ensure!(expected_fields > 0, "COPY projection must contain fields");
        Ok(Self { stage: Stage::Header, scratch: [0; 19], used: 0,
            expected_fields, fields_left: 0, rows: 0, bytes: 0 })
    }

    pub fn push(&mut self, mut input: &[u8]) -> anyhow::Result<()> {
        self.bytes = self.bytes.checked_add(u64::try_from(input.len())?)
            .ok_or_else(|| anyhow::anyhow!("COPY byte count overflow"))?;
        while !input.is_empty() {
            match self.stage {
                Stage::Done => anyhow::bail!("bytes after COPY trailer"),
                Stage::Extension(left) | Stage::FieldBody(left) => {
                    let take = input.len().min(left as usize);
                    input = &input[take..];
                    let remaining = left - u32::try_from(take)?;
                    match self.stage {
                        Stage::Extension(_) => {
                            self.stage = if remaining == 0 { Stage::Row } else { Stage::Extension(remaining) };
                        }
                        _ if remaining == 0 => self.field_complete()?,
                        _ => self.stage = Stage::FieldBody(remaining),
                    }
                }
                stage => {
                    let needed = match stage { Stage::Header => 19, Stage::Row => 2, _ => 4 };
                    let take = input.len().min(needed - self.used);
                    self.scratch[self.used..self.used + take].copy_from_slice(&input[..take]);
                    self.used += take;
                    input = &input[take..];
                    if self.used != needed { continue; }
                    self.used = 0;
                    match stage {
                        Stage::Header => {
                            anyhow::ensure!(&self.scratch[..11] == b"PGCOPY\n\xff\r\n\0", "invalid binary COPY signature");
                            let flags = i32::from_be_bytes(self.scratch[11..15].try_into()?);
                            anyhow::ensure!(flags == 0, "unsupported binary COPY flags");
                            let extension = i32::from_be_bytes(self.scratch[15..19].try_into()?);
                            anyhow::ensure!(extension >= 0, "negative binary COPY extension length");
                            self.stage = if extension == 0 { Stage::Row } else { Stage::Extension(extension as u32) };
                        }
                        Stage::Row => {
                            let fields = i16::from_be_bytes(self.scratch[..2].try_into()?);
                            if fields == -1 { self.stage = Stage::Done; }
                            else {
                                anyhow::ensure!(fields == self.expected_fields, "COPY field count changed");
                                self.fields_left = fields;
                                self.stage = Stage::FieldLength;
                            }
                        }
                        _ => {
                            let length = i32::from_be_bytes(self.scratch[..4].try_into()?);
                            anyhow::ensure!(length >= -1, "invalid binary COPY field length");
                            if length <= 0 { self.field_complete()?; }
                            else { self.stage = Stage::FieldBody(length as u32); }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn field_complete(&mut self) -> anyhow::Result<()> {
        self.fields_left -= 1;
        if self.fields_left == 0 {
            self.rows = self.rows.checked_add(1).ok_or_else(|| anyhow::anyhow!("COPY row count overflow"))?;
            self.stage = Stage::Row;
        } else {
            self.stage = Stage::FieldLength;
        }
        Ok(())
    }

    pub fn finish(&self) -> anyhow::Result<()> {
        anyhow::ensure!(matches!(self.stage, Stage::Done) && self.used == 0, "truncated binary COPY stream");
        Ok(())
    }
}
