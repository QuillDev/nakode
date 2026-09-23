//! Server-owned provenance and display records. Prompt text is never a provenance parser.
use super::{Result, failure, refuse};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Source {
    pub kind: String,
    pub session_id: String,
    pub title: String,
    pub call_id: Option<String>,
    pub status: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct DisplayMessage<'a> {
    pub message_id: &'a str,
    pub sequence: i64,
    pub received_at_ms: i64,
    pub source: &'a Source,
    pub text: &'a str,
    pub attachment_start_index: usize,
    pub attachment_count: usize,
    pub file_paths: Vec<&'a str>,
}

/// Called only after the runtime has matched the exact pending source call and its arguments.
/// The immutable, same-owner runtime relationship—not payload claims—grants downward authority.
pub(crate) fn relay_source(
    connection: &Connection,
    source: &str,
    target: &str,
    call: &str,
) -> Result<Source> {
    super::authorize(connection, source, true)?;
    let title: Option<String> = connection
        .query_row(
            "SELECT s.title FROM sessions s JOIN sessions t ON t.id = ?2
         LEFT JOIN session_skill_profiles sp ON sp.session_id = s.id
         LEFT JOIN session_skill_profiles tp ON tp.session_id = t.id
         WHERE s.id = ?1 AND s.id <> t.id AND (
           (sp.profile_id IS NOT NULL AND sp.profile_id = tp.profile_id)
           OR (sp.profile_id IS NULL AND tp.profile_id IS NULL AND s.workspace = t.workspace))",
            params![source, target],
            |row| row.get(0),
        )
        .optional()
        .map_err(failure)?;
    let title =
        title.ok_or_else(|| refuse("relay sessions must share current runtime ownership"))?;
    let downward: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM session_child_links WHERE parent_id = ?1 AND child_id = ?2)",
        params![source, target], |row| row.get(0),
    ).map_err(failure)?;
    Ok(Source {
        kind: if downward {
            "delegated_instruction"
        } else {
            "peer_context"
        }
        .to_owned(),
        session_id: source.to_owned(),
        title: title.chars().take(120).collect(),
        call_id: Some(call.to_owned()),
        status: None,
    })
}

pub(super) fn authenticate_source(
    connection: &Connection,
    command: &nakode_protocol::Command,
    authenticate: impl FnOnce() -> Result<()>,
) -> Result<Option<Source>> {
    let nakode_protocol::Command::RelayAgentFollowup {
        session_id,
        source_session_id,
        source_call_id,
        ..
    } = command
    else {
        return Ok(None);
    };
    authenticate()?;
    relay_source(
        connection,
        source_session_id.as_str(),
        session_id.as_str(),
        source_call_id,
    )
    .map(Some)
}

pub(super) fn save_source(
    connection: &Connection,
    session: &str,
    message: &str,
    source: &Source,
) -> Result<()> {
    connection
        .execute(
            "INSERT INTO followup_sources(message_sequence, source_json)
         SELECT sequence, ?3 FROM followup_messages WHERE session_id = ?1 AND message_id = ?2",
            params![
                session,
                message,
                serde_json::to_string(source).map_err(failure)?
            ],
        )
        .map_err(failure)?;
    Ok(())
}
