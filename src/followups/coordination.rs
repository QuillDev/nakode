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
    /// Admitted as an owner Chat's instruction; its authority does not rest on a parent link.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub owner_chat: bool,
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
/// Instruction authority comes from the runtime, never payload claims: the immutable same-owner
/// parent link, or the authenticated integration vouching that the source is one of the owner's
/// Chats, which may instruct any of the owner's agents. Everything else stays peer context.
pub(crate) fn relay_source(
    connection: &Connection,
    source: &str,
    target: &str,
    call: &str,
    owner_chat: bool,
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
        kind: if downward || owner_chat {
            "delegated_instruction"
        } else {
            "peer_context"
        }
        .to_owned(),
        session_id: source.to_owned(),
        title: title.chars().take(120).collect(),
        call_id: Some(call.to_owned()),
        status: None,
        owner_chat,
    })
}

/// An owner Chat that instructs an agent becomes its parent, so the agent's reports return to
/// whoever last directed it and it appears among that Chat's agents. The link row is re-pointed,
/// never deleted, so the agent's report and question history stay attached to it. Nothing changes
/// when either session is itself nested (a parent's child, or a child's parent) or the Chat
/// already has the most children a parent may hold; the message is admitted either way.
pub(super) fn adopt(connection: &Connection, chat: &str, agent: &str) -> Result<()> {
    let current: Option<String> = connection
        .query_row(
            "SELECT parent_id FROM session_child_links WHERE child_id = ?1",
            [agent],
            |row| row.get(0),
        )
        .optional()
        .map_err(failure)?;
    if current.as_deref() == Some(chat) {
        return Ok(());
    }
    let nested: bool = connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM session_child_links WHERE child_id = ?1 OR parent_id = ?2)",
            params![chat, agent],
            |row| row.get(0),
        )
        .map_err(failure)?;
    let children: u32 = connection
        .query_row(
            "SELECT count(*) FROM session_child_links WHERE parent_id = ?1",
            [chat],
            |row| row.get(0),
        )
        .map_err(failure)?;
    if nested || children >= 32 {
        return Ok(());
    }
    if current.is_some() {
        connection
            .execute(
                "UPDATE session_child_links SET parent_id = ?1 WHERE child_id = ?2",
                params![chat, agent],
            )
            .map_err(failure)?;
    } else {
        connection
            .execute(
                "INSERT INTO session_child_links(child_id, parent_id, child_title)
                 SELECT id, ?1, COALESCE(title, '') FROM sessions WHERE id = ?2",
                params![chat, agent],
            )
            .map_err(failure)?;
    }
    Ok(())
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
        source_owner_chat,
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
        *source_owner_chat,
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
