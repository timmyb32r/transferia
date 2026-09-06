use super::*;

fn table(namespace: &str, name: &str) -> TableIdentity {
    TableIdentity { namespace: namespace.into(), name: name.into() }
}

fn rule(include: &str, exclude: Option<&str>) -> TableRule {
    TableRule {
        include: include.into(),
        exclude: exclude.map(str::to_owned),
        mode: PatternMode::Glob,
    }
}

fn resolve(rules: Vec<TableRule>, catalog: &[TableIdentity]) -> SelectionPreview {
    TableSelection { rules }
        .compile().unwrap().resolve(catalog).unwrap()
}

#[test]
fn glob_uses_full_names_and_literal_underscores() {
    let catalog = [table("db", "reports_1"), table("db", "reportsX1"), table("other", "reports_1")];
    let result = resolve(vec![rule("db.reports_?", None)], &catalog);
    assert!(result.issues.is_empty());
    assert_eq!(result.cards[0].selected, vec![catalog[0].clone()]);
}

#[test]
fn exclusions_are_limited_to_their_own_include() {
    let catalog = [table("db", "reports_test"), table("db", "users_test")];
    let selection = TableSelection {
        rules: vec![rule("db.reports_*", Some("db.*_test")), rule("db.users_test", None)],
    };
    let result = selection.compile().unwrap().resolve(&catalog).unwrap();
    assert_eq!(result.issues, vec![SelectionIssue::EmptyMatch { card: 0 }]);
    assert!(result.selected_tables().is_err());
    assert_eq!(result.cards[0].excluded, vec![catalog[0].clone()]);
    assert_eq!(result.cards[1].selected, vec![catalog[1].clone()]);
}

#[test]
fn two_cards_selecting_the_same_table_are_a_conflict() {
    let target = table("db", "reports_1");
    let result = resolve(vec![rule("db.*", None), rule("db.reports_*", None)], &[target.clone()]);
    assert_eq!(result.issues, vec![SelectionIssue::Conflict {
        table: target, first_card: 0, second_card: 1, kind: ConflictKind::MultipleIncludes,
    }]);
}

#[test]
fn another_cards_exclusion_conflicts_in_either_order() {
    let target = table("db", "reports_test");
    for (empty_card, rules) in [
        vec![rule("db.reports_*", Some("db.*_test")), rule("db.reports_test", None)],
        vec![rule("db.reports_test", None), rule("db.reports_*", Some("db.*_test"))],
    ].into_iter().enumerate() {
        let result = TableSelection { rules }
            .compile().unwrap().resolve(&[target.clone()]).unwrap();
        assert_eq!(result.issues, vec![SelectionIssue::Conflict {
            table: target.clone(), first_card: 0, second_card: 1, kind: ConflictKind::IncludeExclude,
        }, SelectionIssue::EmptyMatch { card: empty_card }]);
    }
}

#[test]
fn two_exclusions_do_not_conflict() {
    let result = TableSelection {
        rules: vec![rule("db.*", Some("db.*")), rule("db.*", Some("db.*"))],
    }.compile().unwrap().resolve(&[table("db", "t")]).unwrap();
    assert_eq!(result.issues, vec![SelectionIssue::EmptyMatch { card: 0 }, SelectionIssue::EmptyMatch { card: 1 }]);
}

#[test]
fn empty_cards_always_fail_in_both_pattern_modes() {
    for mode in [PatternMode::Glob, PatternMode::Regex] {
        for catalog in [vec![], vec![table("db", "unmatched")]] {
            let selection = TableSelection {
                rules: vec![TableRule { include: "missing".into(), exclude: None, mode }],
            };
            let preview = selection.compile().unwrap().resolve(&catalog).unwrap();
            assert_eq!(preview.issues, vec![SelectionIssue::EmptyMatch { card: 0 }]);
            assert!(preview.selected_tables().is_err());
        }
    }
    let preview = SelectionPreview { cards: vec![CardMatches::default()], issues: vec![] };
    assert!(preview.selected_tables().unwrap_err().to_string().contains("at least one table"));
}

#[test]
fn empty_policy_applies_after_exclusion_to_every_card() {
    let result = resolve(vec![rule("db.*", Some("db.*")), rule("other.*", None)], &[table("db", "t")]);
    assert_eq!(result.issues, vec![SelectionIssue::EmptyMatch { card: 0 }, SelectionIssue::EmptyMatch { card: 1 }]);
}

#[test]
fn no_matching_exclusion_is_not_an_error() {
    let result = resolve(vec![rule("db.*", Some("other.*"))], &[table("db", "t")]);
    assert!(result.issues.is_empty());
}

#[test]
fn regex_matches_entire_name_including_newlines() {
    let selection = TableSelection {
        rules: vec![TableRule { include: r"db\.reports_[0-9]+".into(), exclude: None, mode: PatternMode::Regex }],
    };
    let catalog = [table("db", "reports_1"), table("db", "reports_1\n"), table("xdb", "reports_1")];
    let result = selection.compile().unwrap().resolve(&catalog).unwrap();
    assert_eq!(result.cards[0].selected, vec![catalog[0].clone()]);
}

#[test]
fn escaped_glob_names_preserve_literal_special_characters() {
    let target = table("db", "a*b?c\\d");
    let include = PatternMode::Glob.exact_pattern(&target);
    let result = resolve(vec![rule(&include, None)], &[target.clone(), table("db", "aZZbXc\\d")]);
    assert_eq!(result.cards[0].selected, vec![target]);
}

#[test]
fn question_mark_matches_one_unicode_character() {
    let target = table("db", "я");
    let result = resolve(vec![rule("db.?", None)], &[target.clone(), table("db", "яя")]);
    assert_eq!(result.cards[0].selected, vec![target]);
}

#[test]
fn invalid_patterns_fail() {
    for bad in [rule("db.x\\", None), TableRule { include: "[".into(), exclude: None, mode: PatternMode::Regex }] {
        assert!(TableSelection { rules: vec![bad] }.compile().is_err());
    }
}

#[test]
fn invalid_exclude_identifies_card_and_field() {
    let error = TableSelection {
        rules: vec![rule("db.*", Some("bad\\"))],
    }.compile().err().unwrap();
    assert_eq!(error.card, 0);
    assert_eq!(error.field, PatternField::Exclude);
}

#[test]
fn exact_patterns_roundtrip_in_both_modes() {
    let target = table("db", "a+b[0](x)$");
    for mode in [PatternMode::Glob, PatternMode::Regex] {
        let result = TableSelection {
            rules: vec![TableRule { include: mode.exact_pattern(&target), exclude: None, mode }],
        }.compile().unwrap().resolve(&[target.clone()]).unwrap();
        assert_eq!(result.cards[0].selected, vec![target.clone()]);
    }
}

#[test]
fn qualified_names_do_not_merge_dots_in_different_identifier_parts() {
    let target = table("a.b", "c");
    let other = table("a", "b.c");
    assert_ne!(target.qualified_name(), other.qualified_name());
    for mode in [PatternMode::Glob, PatternMode::Regex] {
        let result = TableSelection {
            rules: vec![TableRule { include: mode.exact_pattern(&target), exclude: None, mode }],
        }.compile().unwrap().resolve(&[target.clone(), other.clone()]).unwrap();
        assert_eq!(result.cards[0].selected, vec![target.clone()]);
    }
}

#[test]
fn duplicate_catalog_identities_fail_instead_of_silent_deduplication() {
    let compiled = TableSelection {
        rules: vec![rule("*.*", None)],
    }.compile().unwrap();
    assert!(compiled.resolve(&[table("db", "t"), table("db", "t")]).is_err());
}

#[test]
fn serialized_defaults_fail_on_empty_matches_and_use_glob() {
    let selection: TableSelection = serde_json::from_value(serde_json::json!({
        "rules": [{ "include": "db.*" }]
    })).unwrap();
    assert_eq!(selection.compile().unwrap().resolve(&[]).unwrap().issues, vec![SelectionIssue::EmptyMatch { card: 0 }]);
    assert!(serde_json::from_value::<TableSelection>(serde_json::json!({
        "rules": [], "empty_matches": "allow_empty_matches"
    })).is_err());
    assert!(serde_json::to_value(schemars::schema_for!(TableSelection)).unwrap()
        .pointer("/properties/empty_matches").is_none());
    assert_eq!(selection.rules[0].mode, PatternMode::Glob);
}

#[test]
fn classifying_a_future_table_uses_the_same_conflicts_as_preview() {
    let target = table("db", "reports_2027");
    let compiled = TableSelection {
        rules: vec![rule("db.*", None), rule("db.reports_*", None)],
    }.compile().unwrap();
    assert_eq!(compiled.resolve(&[target.clone()]).unwrap().issues, compiled.classify(&target).issues);
}

#[test]
fn startup_cannot_consume_a_conflicting_preview() {
    let preview = resolve(vec![rule("db.*", None), rule("db.t", None)], &[table("db", "t")]);
    assert!(preview.selected_tables().is_err());
}

#[test]
fn no_rules_rejects_startup() {
    let preview = TableSelection {
        rules: vec![],
    }.compile().unwrap().resolve(&[]).unwrap();
    assert_eq!(preview.issues, vec![SelectionIssue::NoRules]);
    assert!(preview.selected_tables().is_err());
}
