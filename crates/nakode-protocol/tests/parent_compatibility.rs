use nakode_protocol::{SessionSummary, SessionView};
use serde_json::{Value, json};

fn legacy_summary() -> Value {
    json!({
        "id": "legacy-session",
        "workspace_id": "workspace",
        "title": "Retained session",
        "updated_at_ms": 200,
    })
}

fn legacy_session() -> Value {
    json!({
        "id": "legacy-session",
        "revision": 1,
        "workspace_id": "workspace",
        "title": "Retained session",
        "status_message": "",
        "diagnostic_count": 0,
        "activity": "idle",
        "transcript": {
            "entries": [],
            "has_earlier": false,
            "stream_active": false,
            "stream_label": "",
        },
        "queue": [],
        "interactions": [],
        "todos": [],
        "runs": [],
        "notices": [],
    })
}

#[test]
fn older_session_projections_decode_without_inventing_parentage() {
    let summary: SessionSummary = serde_json::from_value(legacy_summary()).unwrap();
    let session: SessionView = serde_json::from_value(legacy_session()).unwrap();
    assert!(summary.parent_session_id.is_none());
    assert!(session.parent_session_id.is_none());
    assert_eq!(summary.id, session.id);
    assert_eq!(summary.title, "Retained session");
    assert_eq!(session.revision, 1);
}

#[test]
fn explicit_parent_identity_and_null_round_trip() {
    for parent in [Value::Null, json!("parent-session")] {
        let mut summary = legacy_summary();
        let mut session = legacy_session();
        summary["parent_session_id"] = parent.clone();
        session["parent_session_id"] = parent.clone();
        let summary: SessionSummary = serde_json::from_value(summary).unwrap();
        let session: SessionView = serde_json::from_value(session).unwrap();
        assert_eq!(summary.parent_session_id, session.parent_session_id);
        assert_eq!(
            serde_json::to_value(summary).unwrap()["parent_session_id"],
            parent
        );
        assert_eq!(
            serde_json::to_value(session).unwrap()["parent_session_id"],
            parent
        );
    }
}

#[test]
fn malformed_parent_identity_is_not_treated_as_absent() {
    for parent in [json!(42), json!(false), json!({}), json!([])] {
        let mut summary = legacy_summary();
        let mut session = legacy_session();
        summary["parent_session_id"] = parent.clone();
        session["parent_session_id"] = parent;
        assert!(serde_json::from_value::<SessionSummary>(summary).is_err());
        assert!(serde_json::from_value::<SessionView>(session).is_err());
    }
}
