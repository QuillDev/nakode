use crate::{v1 as api, validate_transcript_page};

fn entry(id: &str) -> api::TranscriptEntry {
    api::TranscriptEntry {
        id: id.into(),
        ..Default::default()
    }
}

#[test]
fn prefix_validation_rejects_revisions_duplicates_empty_pages_and_bad_cursors() {
    let current = api::TranscriptPage {
        entries: vec![entry("tail")],
        prefix_before: "prefix-v1".into(),
        ..Default::default()
    };
    let valid = api::TranscriptPage {
        entries: vec![entry("older")],
        prefix_through: "prefix-v1".into(),
        ..Default::default()
    };
    assert!(validate_transcript_page(&current, &valid).is_ok());
    let mut changed = valid.clone();
    changed.prefix_through = "prefix-v2".into();
    assert!(validate_transcript_page(&current, &changed).is_err());
    let mut overlapping = valid.clone();
    overlapping.entries.push(entry("tail"));
    assert!(validate_transcript_page(&current, &overlapping).is_err());
    let mut duplicate = valid.clone();
    duplicate.entries.push(entry("older"));
    assert!(validate_transcript_page(&current, &duplicate).is_err());
    let mut empty = valid.clone();
    empty.entries.clear();
    assert!(validate_transcript_page(&current, &empty).is_err());
    let mut cursor = valid;
    cursor.next_before_entry_id = Some("tail".into());
    assert!(validate_transcript_page(&current, &cursor).is_err());
}
