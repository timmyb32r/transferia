use core::fmt;
use std::io::Write as _;

use arrow::array::StringBuilder;

pub(super) struct DlqRecord {
    pub source_message: u32,
    pub byte_start: u32,
    pub byte_end: u32,
    pub reason: DlqReason,
    pub record_index: u32,
}

struct ArrowStringConsumer<'a>(&'a mut StringBuilder);

impl base64::write::StrConsumer for ArrowStringConsumer<'_> {
    #[expect(
        clippy::expect_used,
        reason = "Arrow StringBuilder implements fmt::Write infallibly"
    )]
    fn consume(&mut self, encoded: &str) {
        fmt::Write::write_str(self.0, encoded)
            .expect("writing UTF-8 base64 into an Arrow string builder cannot fail");
    }
}

pub(super) fn append_base64(builder: &mut StringBuilder, raw: &[u8]) -> anyhow::Result<()> {
    let mut encoder = base64::write::EncoderStringWriter::from_consumer(
        ArrowStringConsumer(builder),
        &base64::engine::general_purpose::STANDARD,
    );
    encoder.write_all(raw)?;
    let ArrowStringConsumer(builder) = encoder.into_inner();
    builder.append_value("");
    Ok(())
}

pub(super) fn subslice_range(parent: &[u8], subslice: &[u8]) -> core::ops::Range<usize> {
    let start = subslice.as_ptr() as usize - parent.as_ptr() as usize;
    start..start + subslice.len()
}

#[expect(
    clippy::expect_used,
    reason = "parser safety limits keep message, record, and byte indexes below u32::MAX"
)]
pub(super) fn dlq_record(
    source_message: usize,
    byte_range: core::ops::Range<usize>,
    reason: DlqReason,
    record_index: u64,
) -> DlqRecord {
    DlqRecord {
        source_message: u32::try_from(source_message)
            .expect("delivery message count is bounded far below u32::MAX"),
        byte_start: u32::try_from(byte_range.start)
            .expect("decoded message size is bounded below u32::MAX"),
        byte_end: u32::try_from(byte_range.end)
            .expect("decoded message size is bounded below u32::MAX"),
        reason,
        record_index: u32::try_from(record_index)
            .expect("record count is bounded by decoded bytes below u32::MAX"),
    }
}

pub(super) enum DlqReason {
    JsonParse,
    ExtractionFailed(String),
}

impl DlqReason {
    pub fn as_str(&self) -> &str {
        match self {
            Self::JsonParse => "JSON parse error",
            Self::ExtractionFailed(detail) => detail,
        }
    }
}
