use arrow::array::ArrayRef;

use super::dlq::DlqRecord;
use super::parser::AnyBuilder;
use super::typed::TypedScratch;

pub struct ParserWorkspace {
    pub(super) builders: Vec<AnyBuilder>,
    pub(super) typed_scratch: Vec<TypedScratch>,
    pub(super) typed_seen: Vec<bool>,
    pub(super) json_buf: Vec<u8>,
    pub(super) dlq_records: Vec<DlqRecord>,
    pub(super) arrays: Vec<ArrayRef>,
}

impl Default for ParserWorkspace {
    fn default() -> Self {
        Self::new()
    }
}

impl ParserWorkspace {
    pub(super) const MAX_RETAINED_SCRATCH_BYTES: usize = 1024 * 1024;

    #[must_use]
    pub const fn new() -> Self {
        Self {
            builders: Vec::new(),
            typed_scratch: Vec::new(),
            typed_seen: Vec::new(),
            json_buf: Vec::new(),
            dlq_records: Vec::new(),
            arrays: Vec::new(),
        }
    }

    pub(super) fn release_large_scratch(&mut self) {
        if self.json_buf.capacity() > Self::MAX_RETAINED_SCRATCH_BYTES {
            self.json_buf = Vec::new();
        } else {
            self.json_buf.clear();
        }
        if self.dlq_records.capacity() > Self::MAX_RETAINED_SCRATCH_BYTES / 32 {
            self.dlq_records = Vec::new();
        } else {
            self.dlq_records.clear();
        }
    }
}
