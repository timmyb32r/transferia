use super::*;

#[test]
fn all_tables_and_independent_pattern_modes_share_startup_and_admission_rules() {
    let catalog = [table("db", "reports_1"), table("db", "reports_test"), table("db", "users")];
    let all = TableSelection::All {};
    let compiled_all = all.compile().unwrap();
    assert_eq!(compiled_all.resolve(&catalog).unwrap().selected_tables().unwrap(), catalog);
    for entry in &catalog {
        assert_eq!(compiled_all.classify(entry).selected_by, vec![0]);
        assert!(compiled_all.classify(entry).excluded_by.is_empty());
    }
    let selected = TableSelection::Selected { rules: vec![TableRule {
        include: "db.reports_*".into(), include_mode: PatternMode::Glob,
        exclude: Some(r"db\.reports_(test|temp)".into()), exclude_mode: PatternMode::Regex,
    }] };
    let compiled = selected.compile().unwrap();
    assert_eq!(compiled.resolve(&catalog).unwrap().selected_tables().unwrap(), vec![catalog[0].clone()]);
    assert_eq!(compiled.classify(&catalog[0]).selected_by, vec![0]);
    assert_eq!(compiled.classify(&catalog[1]).excluded_by, vec![0]);
    assert!(all.compile().unwrap().resolve(&[]).unwrap().selected_tables().is_err());
}

#[test]
fn all_tables_is_a_fieldless_strict_variant_in_config_and_schema() {
    let all: TableSelection = serde_json::from_value(serde_json::json!({"type": "all"})).unwrap();
    assert_eq!(serde_json::to_value(all).unwrap(), serde_json::json!({"type": "all"}));
    for invalid in [
        serde_json::json!({"type": "all", "exclude": "db.reports_*"}),
        serde_json::json!({"type": "all", "exclude_mode": "glob"}),
        serde_json::json!({"type": "all", "rules": [{"include": "ignored"}]}),
    ] {
        assert!(serde_json::from_value::<TableSelection>(invalid).is_err());
    }
    let schema = serde_json::to_value(schemars::schema_for!(TableSelection)).unwrap();
    let branches = schema["oneOf"].as_array().unwrap();
    assert_eq!(branches[0]["properties"]["type"]["const"], "selected");
    let all = branches.iter().find(|branch| branch["properties"]["type"]["const"] == "all").unwrap();
    assert_eq!(all["properties"].as_object().unwrap().keys().collect::<Vec<_>>(), vec!["type"]);
    assert_eq!(all["additionalProperties"], false);
}

fn table(namespace: &str, name: &str) -> TableIdentity {
    TableIdentity { namespace: namespace.into(), name: name.into() }
}

fn rule(include: &str, exclude: Option<&str>) -> TableRule {
    TableRule {
        include: include.into(),
        exclude: exclude.map(str::to_owned),
        include_mode: PatternMode::Glob, exclude_mode: PatternMode::Glob,
    }
}

fn resolve(rules: Vec<TableRule>, catalog: &[TableIdentity]) -> SelectionPreview {
    TableSelection::Selected { rules }
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
fn completion_globs_match_from_the_beginning_not_a_substring() {
    let catalog = [table("schema", "reports"), table("information_schema", "reports"), table("Schema", "reports")];
    let result = resolve(vec![rule("schema*", None)], &catalog);
    assert_eq!(result.cards[0].selected, vec![catalog[0].clone()]);
    let exact = resolve(vec![rule("schema", None)], &catalog);
    assert!(exact.cards[0].selected.is_empty());
}

#[test]
fn exclusions_are_limited_to_their_own_include() {
    let catalog = [table("db", "reports_test"), table("db", "users_test")];
    let selection = TableSelection::Selected {
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
        let result = TableSelection::Selected { rules }
            .compile().unwrap().resolve(&[target.clone()]).unwrap();
        assert_eq!(result.issues, vec![SelectionIssue::Conflict {
            table: target.clone(), first_card: 0, second_card: 1, kind: ConflictKind::IncludeExclude,
        }, SelectionIssue::EmptyMatch { card: empty_card }]);
    }
}

#[test]
fn two_exclusions_do_not_conflict() {
    let result = TableSelection::Selected {
        rules: vec![rule("db.*", Some("db.*")), rule("db.*", Some("db.*"))],
    }.compile().unwrap().resolve(&[table("db", "t")]).unwrap();
    assert_eq!(result.issues, vec![SelectionIssue::EmptyMatch { card: 0 }, SelectionIssue::EmptyMatch { card: 1 }]);
}

#[test]
fn empty_cards_always_fail_in_both_pattern_modes() {
    for mode in [PatternMode::Glob, PatternMode::Regex] {
        for catalog in [vec![], vec![table("db", "unmatched")]] {
            let selection = TableSelection::Selected {
                rules: vec![TableRule { include: "missing".into(), exclude: None, include_mode: mode, exclude_mode: PatternMode::Glob }],
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
    let selection = TableSelection::Selected {
        rules: vec![TableRule { include: r"db\.reports_[0-9]+".into(), exclude: None, include_mode: PatternMode::Regex, exclude_mode: PatternMode::Glob }],
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
    for bad in [rule("db.x\\", None), TableRule { include: "[".into(), exclude: None, include_mode: PatternMode::Regex, exclude_mode: PatternMode::Glob }] {
        assert!(TableSelection::Selected { rules: vec![bad] }.compile().is_err());
    }
}

#[test]
fn invalid_exclude_identifies_card_and_field() {
    let error = TableSelection::Selected {
        rules: vec![rule("db.*", Some("bad\\"))],
    }.compile().err().unwrap();
    assert_eq!(error.card, 0);
    assert_eq!(error.field, PatternField::Exclude);
}

#[test]
fn exact_patterns_roundtrip_in_both_modes() {
    let target = table("db", "a+b[0](x)$");
    for mode in [PatternMode::Glob, PatternMode::Regex] {
        let result = TableSelection::Selected {
            rules: vec![TableRule { include: mode.exact_pattern(&target), exclude: None, include_mode: mode, exclude_mode: PatternMode::Glob }],
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
        let result = TableSelection::Selected {
            rules: vec![TableRule { include: mode.exact_pattern(&target), exclude: None, include_mode: mode, exclude_mode: PatternMode::Glob }],
        }.compile().unwrap().resolve(&[target.clone(), other.clone()]).unwrap();
        assert_eq!(result.cards[0].selected, vec![target.clone()]);
    }
}

#[test]
fn duplicate_catalog_identities_fail_instead_of_silent_deduplication() {
    let compiled = TableSelection::Selected {
        rules: vec![rule("*.*", None)],
    }.compile().unwrap();
    assert!(compiled.resolve(&[table("db", "t"), table("db", "t")]).is_err());
}

#[test]
fn serialized_defaults_fail_on_empty_matches_and_use_glob() {
    let selection: TableSelection = serde_json::from_value(serde_json::json!({
        "type": "selected", "rules": [{ "include": "db.*" }]
    })).unwrap();
    assert_eq!(selection.compile().unwrap().resolve(&[]).unwrap().issues, vec![SelectionIssue::EmptyMatch { card: 0 }]);
    assert!(serde_json::from_value::<TableSelection>(serde_json::json!({
        "type": "selected", "rules": [], "empty_matches": "allow_empty_matches"
    })).is_err());
    assert!(serde_json::to_value(schemars::schema_for!(TableSelection)).unwrap()
        .pointer("/properties/empty_matches").is_none());
    assert_eq!(match selection { TableSelection::Selected { rules } => rules[0].include_mode, _ => panic!("selected") }, PatternMode::Glob);
}

#[test]
fn classifying_a_future_table_uses_the_same_conflicts_as_preview() {
    let target = table("db", "reports_2027");
    let compiled = TableSelection::Selected {
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
    let preview = TableSelection::Selected {
        rules: vec![],
    }.compile().unwrap().resolve(&[]).unwrap();
    assert_eq!(preview.issues, vec![SelectionIssue::NoRules]);
    assert!(preview.selected_tables().is_err());
}
