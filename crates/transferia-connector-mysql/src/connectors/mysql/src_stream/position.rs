use std::collections::BTreeSet;
use std::error::Error;
use std::fmt::{self, Write as _};

use mysql_async::{BinlogStreamRequest, GnoInterval, Sid};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MySqlBinlogPosition {
    /// Exact binlog filename bytes. They are deliberately not decoded lossily.
    pub filename: Vec<u8>,

    /// MySQL's non-GTID dump command and event header both carry a 32-bit position.
    pub position: u32,
}

impl MySqlBinlogPosition {
    pub fn new(filename: Vec<u8>, position: u64) -> Result<Self, PositionError> {
        if filename.is_empty() {
            return Err(PositionError::EmptyFilename);
        }
        if filename.contains(&0) {
            return Err(PositionError::FilenameContainsNul);
        }
        let position = u32::try_from(position)
            .map_err(|_| PositionError::PositionDoesNotFitProtocol(position))?;
        if position < 4 {
            return Err(PositionError::PositionBeforeBinlogHeader(position));
        }
        Ok(Self { filename, position })
    }

    pub fn validate(&self) -> Result<(), PositionError> {
        Self::new(self.filename.clone(), u64::from(self.position)).map(|_| ())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum MySqlResumePosition {
    FilePosition {
        position: MySqlBinlogPosition,
    },
    Gtid {
        executed: GtidSet,

        fallback_position: MySqlBinlogPosition,
    },
}

impl MySqlResumePosition {
    pub fn validate(&self) -> Result<(), PositionError> {
        match self {
            Self::FilePosition { position } => position.validate(),
            Self::Gtid {
                executed,
                fallback_position,
            } => {
                fallback_position.validate()?;
                executed.validate()
            }
        }
    }

    pub fn request(&self, server_id: u32) -> Result<BinlogStreamRequest<'_>, PositionError> {
        if server_id == 0 {
            return Err(PositionError::ZeroServerId);
        }
        self.validate()?;
        match self {
            Self::FilePosition { position } => Ok(BinlogStreamRequest::new(server_id)
                .with_filename(&position.filename)
                .with_pos(u64::from(position.position))),
            Self::Gtid {
                executed,
                fallback_position: _,
            } => Ok(BinlogStreamRequest::new(server_id)
                .with_gtid()
                .with_gtid_set(executed.to_mysql_sids()?)),
        }
    }

    pub fn fallback_position(&self) -> &MySqlBinlogPosition {
        match self {
            Self::FilePosition { position } => position,
            Self::Gtid {
                fallback_position, ..
            } => fallback_position,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(transparent)]
pub struct GtidSet(pub Vec<GtidSid>);

impl GtidSet {
    /// Parses the canonical text returned by `@@GLOBAL.gtid_executed`.
    ///
    /// The empty string is the exact representation of an empty executed set.
    /// Whitespace and non-canonical SID spellings are rejected instead of being
    /// normalized because this value participates in durable replay state.
    pub fn parse_mysql(value: &str) -> Result<Self, PositionError> {
        if value.is_empty() {
            return Ok(Self::default());
        }
        if value.chars().any(char::is_whitespace) {
            return Err(PositionError::GtidSetContainsWhitespace);
        }

        let mut result = Vec::new();
        for (index, text) in value.split(',').enumerate() {
            if text.is_empty() {
                return Err(PositionError::EmptyGtidSetEntry(index));
            }
            reject_sid_parser_overflow(text)?;
            let parsed = text.parse::<Sid<'static>>().map_err(|error| {
                PositionError::InvalidGtidSid {
                    text: text.to_owned(),
                    reason: error.to_string(),
                }
            })?;
            let sid = GtidSid {
                sid: parsed.uuid(),
                tag: parsed.tag().map(|tag| tag.as_str().to_owned()),
                intervals: parsed
                    .intervals()
                    .iter()
                    .map(|interval| GtidInterval {
                        start: interval.start(),
                        end_exclusive: interval.end(),
                    })
                    .collect(),
            };
            sid.validate()?;
            let canonical = sid.to_mysql_text();
            if canonical != text {
                return Err(PositionError::NonCanonicalGtidSidText {
                    received: text.to_owned(),
                    canonical,
                });
            }
            result.push(sid);
        }

        let result = Self(result);
        result.validate()?;
        Ok(result)
    }

    pub fn validate(&self) -> Result<(), PositionError> {
        let mut identities = BTreeSet::new();
        for sid in &self.0 {
            sid.validate()?;
            if !identities.insert((sid.sid, sid.tag.as_deref())) {
                return Err(PositionError::DuplicateGtidSid {
                    sid: sid.sid,
                    tag: sid.tag.clone(),
                });
            }
        }
        Ok(())
    }

    pub fn to_mysql_sids<'a>(&self) -> Result<Vec<Sid<'a>>, PositionError> {
        self.validate()?;
        self.0.iter().map(|sid| sid.to_mysql_sid()).collect()
    }

    /// Adds one newly committed transaction while preserving canonical interval
    /// ordering. Re-observing an already represented GTID is a continuity
    /// violation, not an idempotent update at this layer.
    pub fn include_transaction(
        &mut self,
        sid: [u8; 16],
        tag: Option<String>,
        gno: u64,
    ) -> Result<(), PositionError> {
        let end_exclusive = gno
            .checked_add(1)
            .ok_or(PositionError::InvalidGtidInterval {
                start: gno,
                end_exclusive: gno,
            })?;
        GtidInterval {
            start: gno,
            end_exclusive,
        }
        .validate()?;
        let identity = self
            .0
            .iter()
            .position(|existing| existing.sid == sid && existing.tag == tag);
        let intervals = if let Some(index) = identity {
            &mut self.0[index].intervals
        } else {
            self.0.push(GtidSid {
                sid,
                tag: tag.clone(),
                intervals: Vec::new(),
            });
            &mut self
                .0
                .last_mut()
                .ok_or(PositionError::InvalidGtidInterval {
                    start: gno,
                    end_exclusive,
                })?
                .intervals
        };
        if intervals
            .iter()
            .any(|interval| interval.start <= gno && gno < interval.end_exclusive)
        {
            return Err(PositionError::DuplicateCommittedGtid { sid, tag, gno });
        }
        intervals.push(GtidInterval {
            start: gno,
            end_exclusive,
        });
        intervals.sort_unstable_by_key(|interval| interval.start);
        let mut merged = Vec::<GtidInterval>::with_capacity(intervals.len());
        for interval in intervals.drain(..) {
            if let Some(previous) = merged.last_mut() {
                if previous.end_exclusive == interval.start {
                    previous.end_exclusive = interval.end_exclusive;
                    continue;
                }
            }
            merged.push(interval);
        }
        *intervals = merged;
        self.validate()
    }

    #[must_use]
    pub fn is_subset_of(&self, current: &Self) -> bool {
        self.0.iter().all(|required_sid| {
            current
                .0
                .iter()
                .find(|candidate| {
                    candidate.sid == required_sid.sid && candidate.tag == required_sid.tag
                })
                .is_some_and(|current_sid| {
                    required_sid.intervals.iter().all(|required| {
                        current_sid.intervals.iter().any(|available| {
                            available.start <= required.start
                                && available.end_exclusive >= required.end_exclusive
                        })
                    })
                })
        })
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GtidSid {
    pub sid: [u8; 16],

    pub tag: Option<String>,

    pub intervals: Vec<GtidInterval>,
}

impl GtidSid {
    pub fn validate(&self) -> Result<(), PositionError> {
        self.validate_intervals()?;

        // mysql_common owns the exact MySQL 8.4 tag grammar. Parsing the complete
        // SID also proves that the public request builder can represent this value.
        self.parse_mysql_sid().map(|_| ())
    }

    pub fn to_mysql_sid<'a>(&self) -> Result<Sid<'a>, PositionError> {
        self.validate_intervals()?;
        self.parse_mysql_sid()
    }

    pub fn to_mysql_text(&self) -> String {
        let mut out = format_uuid(self.sid);
        if let Some(tag) = &self.tag {
            out.push(':');
            out.push_str(tag);
        }
        for interval in &self.intervals {
            out.push(':');
            let _ = write!(out, "{}", interval.start);
            if interval.start.checked_add(1) != Some(interval.end_exclusive) {
                let _ = write!(out, "-{}", interval.end_exclusive.saturating_sub(1));
            }
        }
        out
    }

    fn validate_intervals(&self) -> Result<(), PositionError> {
        if self.intervals.is_empty() {
            return Err(PositionError::EmptyGtidIntervals {
                sid: self.sid,
                tag: self.tag.clone(),
            });
        }
        let mut previous_end = None;
        for interval in &self.intervals {
            interval.validate()?;
            if let Some(end) = previous_end {
                if interval.start <= end {
                    return Err(PositionError::NonCanonicalGtidIntervals {
                        previous_end_exclusive: end,
                        next_start: interval.start,
                    });
                }
            }
            previous_end = Some(interval.end_exclusive);
        }
        Ok(())
    }

    fn parse_mysql_sid<'a>(&self) -> Result<Sid<'a>, PositionError> {
        let text = self.to_mysql_text();
        text.parse::<Sid<'a>>()
            .map_err(|error| PositionError::InvalidGtidSid {
                text,
                reason: error.to_string(),
            })
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GtidInterval {
    pub start: u64,

    pub end_exclusive: u64,
}

impl GtidInterval {
    pub fn validate(self) -> Result<(), PositionError> {
        const MAX_END_EXCLUSIVE: u64 = i64::MAX as u64 + 1;

        if self.start == 0
            || self.start >= self.end_exclusive
            || self.end_exclusive > MAX_END_EXCLUSIVE
        {
            return Err(PositionError::InvalidGtidInterval {
                start: self.start,
                end_exclusive: self.end_exclusive,
            });
        }
        GnoInterval::check_and_new(self.start, self.end_exclusive)
            .map(|_| ())
            .map_err(|error| PositionError::InvalidGtidIntervalRepresentation(error.to_string()))
    }
}

fn reject_sid_parser_overflow(text: &str) -> Result<(), PositionError> {
    for component in text.split(':').skip(1) {
        for endpoint in component.split('-') {
            if matches!(endpoint.parse::<u64>(), Ok(u64::MAX)) {
                return Err(PositionError::InvalidGtidSid {
                    text: text.to_owned(),
                    reason: "GTID interval endpoint exceeds MySQL's maximum GNO".to_owned(),
                });
            }
        }
    }
    Ok(())
}

pub(crate) fn format_uuid(sid: [u8; 16]) -> String {
    let mut out = String::with_capacity(36);
    for (index, byte) in sid.into_iter().enumerate() {
        if matches!(index, 4 | 6 | 8 | 10) {
            out.push('-');
        }
        let _ = write!(out, "{byte:02x}");
    }
    out
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PositionError {
    EmptyFilename,
    FilenameContainsNul,
    PositionDoesNotFitProtocol(u64),
    PositionBeforeBinlogHeader(u32),
    ZeroServerId,
    GtidSetContainsWhitespace,
    EmptyGtidSetEntry(usize),
    EmptyGtidIntervals {
        sid: [u8; 16],
        tag: Option<String>,
    },
    DuplicateGtidSid {
        sid: [u8; 16],
        tag: Option<String>,
    },
    DuplicateCommittedGtid {
        sid: [u8; 16],
        tag: Option<String>,
        gno: u64,
    },
    InvalidGtidInterval {
        start: u64,
        end_exclusive: u64,
    },
    InvalidGtidIntervalRepresentation(String),
    NonCanonicalGtidIntervals {
        previous_end_exclusive: u64,
        next_start: u64,
    },
    InvalidGtidSid {
        text: String,
        reason: String,
    },
    NonCanonicalGtidSidText {
        received: String,
        canonical: String,
    },
}

impl fmt::Display for PositionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyFilename => write!(formatter, "binlog filename must not be empty"),
            Self::FilenameContainsNul => write!(formatter, "binlog filename must not contain NUL"),
            Self::PositionDoesNotFitProtocol(position) => write!(
                formatter,
                "binlog position {position} does not fit the non-GTID protocol's u32 field"
            ),
            Self::PositionBeforeBinlogHeader(position) => write!(
                formatter,
                "binlog position {position} points before the four-byte binlog header"
            ),
            Self::ZeroServerId => write!(formatter, "replication server_id must be non-zero"),
            Self::GtidSetContainsWhitespace => {
                write!(formatter, "GTID set must not contain whitespace")
            }
            Self::EmptyGtidSetEntry(index) => {
                write!(formatter, "GTID set entry {index} is empty")
            }
            Self::EmptyGtidIntervals { sid, tag } => write!(
                formatter,
                "GTID SID {} tag {:?} has no executed intervals",
                format_uuid(*sid),
                tag
            ),
            Self::DuplicateGtidSid { sid, tag } => write!(
                formatter,
                "GTID set repeats SID {} tag {:?}",
                format_uuid(*sid),
                tag
            ),
            Self::DuplicateCommittedGtid { sid, tag, gno } => write!(
                formatter,
                "GTID {} tag {:?} transaction {gno} was already committed before this resume position",
                format_uuid(*sid),
                tag
            ),
            Self::InvalidGtidInterval {
                start,
                end_exclusive,
            } => write!(
                formatter,
                "invalid GTID interval [{start}, {end_exclusive})"
            ),
            Self::InvalidGtidIntervalRepresentation(reason) => {
                write!(formatter, "invalid GTID interval: {reason}")
            }
            Self::NonCanonicalGtidIntervals {
                previous_end_exclusive,
                next_start,
            } => write!(
                formatter,
                "GTID intervals must be sorted, disjoint, and non-adjacent: previous end {previous_end_exclusive}, next start {next_start}"
            ),
            Self::InvalidGtidSid { text, reason } => {
                write!(formatter, "invalid GTID SID '{text}': {reason}")
            }
            Self::NonCanonicalGtidSidText {
                received,
                canonical,
            } => write!(
                formatter,
                "non-canonical GTID SID '{received}', canonical form is '{canonical}'"
            ),
        }
    }
}

impl Error for PositionError {}
