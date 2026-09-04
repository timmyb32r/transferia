use super::super::{
    GtidInterval, GtidSet, GtidSid, MySqlBinlogPosition, MySqlResumePosition, PositionError,
};

fn sid() -> GtidSid {
    GtidSid {
        sid: [0x11; 16],
        tag: Some("blue_1".to_owned()),
        intervals: vec![
            GtidInterval {
                start: 1,
                end_exclusive: 4,
            },
            GtidInterval {
                start: 8,
                end_exclusive: 9,
            },
        ],
    }
}

#[test]
fn file_position_rejects_every_lossy_protocol_conversion() {
    assert_eq!(
        MySqlBinlogPosition::new(Vec::new(), 4).unwrap_err(),
        PositionError::EmptyFilename
    );
    assert_eq!(
        MySqlBinlogPosition::new(b"mysql\0bin".to_vec(), 4).unwrap_err(),
        PositionError::FilenameContainsNul
    );
    assert!(matches!(
        MySqlBinlogPosition::new(b"mysql-bin.000001".to_vec(), u64::from(u32::MAX) + 1),
        Err(PositionError::PositionDoesNotFitProtocol(_))
    ));
    assert!(matches!(
        MySqlBinlogPosition::new(b"mysql-bin.000001".to_vec(), 3),
        Err(PositionError::PositionBeforeBinlogHeader(3))
    ));
}

#[test]
fn tagged_gtid_round_trips_through_mysql_commons_public_parser() {
    let sid = sid();
    assert_eq!(
        sid.to_mysql_text(),
        "11111111-1111-1111-1111-111111111111:blue_1:1-3:8"
    );
    let parsed = sid.to_mysql_sid().unwrap();
    assert_eq!(parsed.uuid(), [0x11; 16]);
    assert_eq!(parsed.tag().unwrap().as_str(), "blue_1");
    assert_eq!(parsed.intervals().len(), 2);
    assert_eq!(parsed.intervals()[0].start(), 1);
    assert_eq!(parsed.intervals()[0].end(), 4);
}

#[test]
fn parses_mysql_executed_set_without_losing_tags_or_intervals() {
    let parsed = GtidSet::parse_mysql(
        "11111111-1111-1111-1111-111111111111:blue_1:1-3:8,22222222-2222-2222-2222-222222222222:4-9",
    )
    .unwrap();
    assert_eq!(parsed.0.len(), 2);
    assert_eq!(parsed.0[0], sid());
    assert_eq!(parsed.0[1].sid, [0x22; 16]);
    assert_eq!(parsed.0[1].tag, None);
    assert_eq!(
        parsed.0[1].intervals,
        vec![GtidInterval {
            start: 4,
            end_exclusive: 10,
        }]
    );
    assert_eq!(GtidSet::parse_mysql("").unwrap(), GtidSet::default());
}

#[test]
fn executed_set_parser_rejects_whitespace_noncanonical_and_malformed_input() {
    assert_eq!(
        GtidSet::parse_mysql(
            "11111111-1111-1111-1111-111111111111:1, 22222222-2222-2222-2222-222222222222:1"
        )
        .unwrap_err(),
        PositionError::GtidSetContainsWhitespace
    );
    assert!(matches!(
        GtidSet::parse_mysql("11111111-1111-1111-1111-111111111111:1,"),
        Err(PositionError::EmptyGtidSetEntry(1))
    ));
    assert!(matches!(
        GtidSet::parse_mysql("11111111-1111-1111-1111-111111111111:1-2:3"),
        Err(PositionError::NonCanonicalGtidIntervals { .. })
    ));
    assert!(matches!(
        GtidSet::parse_mysql("11111111-1111-1111-1111-111111111111:bad-tag:1"),
        Err(PositionError::InvalidGtidSid { .. })
    ));
    assert!(matches!(
        GtidSet::parse_mysql("AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA:1"),
        Err(PositionError::NonCanonicalGtidSidText { .. })
    ));
    assert!(matches!(
        GtidSet::parse_mysql("11111111-1111-1111-1111-111111111111:18446744073709551615"),
        Err(PositionError::InvalidGtidSid { .. })
    ));
}

#[test]
fn gtid_set_rejects_duplicates_and_noncanonical_intervals() {
    let duplicate = GtidSet(vec![sid(), sid()]);
    assert!(matches!(
        duplicate.validate(),
        Err(PositionError::DuplicateGtidSid { .. })
    ));

    let mut adjacent = sid();
    adjacent.intervals = vec![
        GtidInterval {
            start: 1,
            end_exclusive: 4,
        },
        GtidInterval {
            start: 4,
            end_exclusive: 8,
        },
    ];
    assert!(matches!(
        adjacent.validate(),
        Err(PositionError::NonCanonicalGtidIntervals { .. })
    ));
}

#[test]
fn committed_gtids_extend_canonically_and_prove_subset_continuity() {
    let mut committed =
        GtidSet::parse_mysql("11111111-1111-1111-1111-111111111111:blue_1:1-3:8").unwrap();
    committed
        .include_transaction([0x11; 16], Some("blue_1".to_owned()), 4)
        .unwrap();
    assert_eq!(
        committed.0[0].to_mysql_text(),
        "11111111-1111-1111-1111-111111111111:blue_1:1-4:8"
    );
    assert!(matches!(
        committed.include_transaction([0x11; 16], Some("blue_1".to_owned()), 4),
        Err(PositionError::DuplicateCommittedGtid { gno: 4, .. })
    ));

    let current = GtidSet::parse_mysql("11111111-1111-1111-1111-111111111111:blue_1:1-10").unwrap();
    assert!(committed.is_subset_of(&current));
    assert!(!current.is_subset_of(&committed));
}

#[test]
fn requests_require_nonzero_replica_identity() {
    let resume = MySqlResumePosition::FilePosition {
        position: MySqlBinlogPosition::new(b"mysql-bin.000001".to_vec(), 4).unwrap(),
    };
    assert!(matches!(
        resume.request(0),
        Err(PositionError::ZeroServerId)
    ));
    resume.request(7).unwrap();
}
