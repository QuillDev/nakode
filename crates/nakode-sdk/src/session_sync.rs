//! A follow owns one endpoint, credential and logical ID. Its cursor never crosses clients.
use super::{
    NakodeClient, RETRY_DELAY, SdkError, StreamExt, Watch, api, managed_watch, retryable_status,
    spawn_traced,
};
use std::collections::{HashMap, HashSet, VecDeque};
use tokio::sync::mpsc;
use tonic::Status;

/// One complete bounded viewport. Revisions are observations in this history, not database IDs.
#[derive(Clone, Debug)]
pub struct FollowedSession {
    pub state: api::SessionState,
    /// Absent on a legacy replacement stream.
    pub cursor: Option<api::SessionSyncCursor>,
    /// Last authoritative projection revision for each visible entry (including snapshot seeds).
    pub entry_revisions: HashMap<String, u64>,
}

type Sender = mpsc::Sender<Result<FollowedSession, SdkError>>;
struct Applied {
    cursor: api::SessionSyncCursor,
    state: api::SessionState,
    versions: HashMap<String, u64>,
    completed: VecDeque<(u64, u64)>,
}

fn valid_entries(entries: &[api::TranscriptEntry]) -> bool {
    let mut ids = HashSet::new();
    entries.iter().all(|entry| {
        !entry.id.is_empty()
            && ids.insert(&entry.id)
            && entry.body_total_bytes.checked_sub(entry.body_start_byte)
                == u64::try_from(entry.body.len()).ok()
            && api::TranscriptEntryStatus::try_from(entry.status)
                .is_ok_and(|s| s != api::TranscriptEntryStatus::Unspecified)
    })
}
impl Applied {
    fn seed(id: &str, snapshot: api::SessionSyncSnapshot) -> Option<Self> {
        let cursor = snapshot.cursor?;
        let state = snapshot.state?;
        let versions = state
            .transcript
            .as_ref()?
            .entries
            .iter()
            .map(|e| (e.id.clone(), cursor.revision))
            .collect();
        (cursor.session_id == id
            && !cursor.incarnation.is_empty()
            && state.id == id
            && cursor.revision == state.revision
            && valid_entries(&state.transcript.as_ref()?.entries))
        .then_some(Self {
            cursor,
            state,
            versions,
            completed: VecDeque::new(),
        })
    }
    fn observed(&self) -> FollowedSession {
        FollowedSession {
            state: self.state.clone(),
            cursor: Some(self.cursor.clone()),
            entry_revisions: self.versions.clone(),
        }
    }
    fn apply(&mut self, batch: api::SessionSyncBatch) -> Option<bool> {
        let base = batch.base?;
        let next = batch.next?;
        if base.session_id != self.cursor.session_id
            || next.session_id != base.session_id
            || base.incarnation != self.cursor.incarnation
            || next.incarnation != base.incarnation
            || next.revision <= base.revision
        {
            return None;
        }
        if self.completed.contains(&(base.revision, next.revision)) {
            return Some(false);
        }
        if base != self.cursor {
            return None;
        }
        let mut state = batch.projection?;
        if state.revision != next.revision || state.id != next.session_id {
            return None;
        }
        let page = state.transcript.as_mut()?;
        if !valid_entries(&page.entries) {
            return None;
        }
        let old = &self.state.transcript.as_ref()?.entries;
        let previous: HashMap<_, _> = old.iter().map(|entry| (entry.id.as_str(), entry)).collect();
        let mut sent: HashMap<_, _> = std::mem::take(&mut page.entries)
            .into_iter()
            .map(|entry| (entry.id.clone(), entry))
            .collect();
        let order = batch.order.map_or_else(
            || old.iter().map(|entry| entry.id.clone()).collect(),
            |order| order.entry_ids,
        );
        let mut versions = HashMap::new();
        let mut seen = HashSet::new();
        for id in &order {
            if !seen.insert(id) {
                return None;
            }
            versions.insert(
                id.clone(),
                if sent.contains_key(id) {
                    next.revision
                } else {
                    *self.versions.get(id)?
                },
            );
            page.entries.push(match sent.remove(id) {
                Some(entry) => entry,
                None => (*previous.get(id.as_str())?).clone(),
            });
        }
        if !sent.is_empty() {
            return None;
        }
        if let Some(agent) = &mut state.active_agent_session {
            if agent.transcript.is_some() {
                return None;
            }
            agent.transcript = Some(page.clone());
        }
        self.completed.push_back((base.revision, next.revision));
        if self.completed.len() > 256 {
            self.completed.pop_front();
        }
        self.versions = versions;
        self.state = state;
        self.cursor = next;
        Some(true)
    }
}

impl NakodeClient {
    /// Follows the bounded session projection, resuming after disconnect and replacing it only
    /// after explicit reset. Older servers use the existing watch. Does not open a session,
    /// replay commands or hydrate historical pages. Dropping the watch cancels its producer.
    #[must_use]
    pub fn follow_session(&self, session_id: impl Into<String>) -> Watch<api::SessionState> {
        Box::pin(
            self.follow_session_versioned(session_id)
                .map(|item| item.map(|followed| followed.state)),
        )
    }

    /// Like [`Self::follow_session`], with cursor and per-entry projection versions.
    #[must_use]
    pub fn follow_session_versioned(
        &self,
        session_id: impl Into<String>,
    ) -> Watch<FollowedSession> {
        let id = session_id.into();
        let client = self.clone();
        let (sender, receiver) = mpsc::channel(super::WATCH_BUFFER);
        let task = spawn_traced(async move {
            let mut held = None;
            let mut reported = false;
            loop {
                let result =
                    Box::pin(client.follow_connection(&id, &sender, &mut held, &mut reported))
                        .await;
                match result {
                    Ok(()) if sender.is_closed() => return,
                    Ok(()) => (),
                    Err(SdkError::Status(s))
                        if retryable_status(&s) || s.code() == tonic::Code::Aborted =>
                    {
                        if !reported && sender.send(Err(s.into())).await.is_err() {
                            return;
                        }
                        reported = true;
                    }
                    Err(error) => {
                        let _ = sender.send(Err(error)).await;
                        return;
                    }
                }
                tokio::time::sleep(RETRY_DELAY).await;
            }
        });
        managed_watch(receiver, task)
    }

    /// Reads the page immediately before a followed page without joining it to the live window.
    /// A changed historical prefix is never silently appended to an older projection.
    ///
    /// # Errors
    /// Returns a transport error or a projection error when the requested prefix changed.
    pub async fn previous_session_page(
        &self,
        session_id: &str,
        page: &api::TranscriptPage,
        limit: u32,
    ) -> Result<Option<api::TranscriptPage>, SdkError> {
        let Some(before) = &page.next_before_entry_id else {
            return Ok(None);
        };
        if page.prefix_before.is_empty() {
            return Err(SdkError::InvalidProjection(
                "missing historical prefix proof".into(),
            ));
        }
        let previous = self
            .get_transcript_page(api::GetTranscriptPageRequest {
                owner_kind: api::TranscriptOwnerKind::Session.into(),
                owner_id: session_id.into(),
                before_entry_id: Some(before.clone()),
                limit,
            })
            .await?;
        super::validate_transcript_page(page, &previous)?;
        Ok(Some(previous))
    }

    async fn follow_replacements(&self, id: &str, sender: &Sender) -> Result<(), SdkError> {
        let mut watch = self.watch_session(id);
        while let Some(item) = watch.next().await {
            if sender
                .send(Ok(FollowedSession {
                    state: item?,
                    cursor: None,
                    entry_revisions: HashMap::new(),
                }))
                .await
                .is_err()
            {
                break;
            }
        }
        Ok(())
    }

    async fn follow_connection(
        &self,
        id: &str,
        sender: &Sender,
        held: &mut Option<Applied>,
        reported: &mut bool,
    ) -> Result<(), SdkError> {
        let info = self.get_server_info().await?;
        if !info.capabilities.iter().any(|c| c == "SessionSyncV1") {
            *held = None;
            return self.follow_replacements(id, sender).await;
        }
        if held.is_none() {
            let snapshot = self
                .transport
                .clone()
                .get_session_sync_snapshot(api::GetSessionRequest {
                    session_id: id.into(),
                    ..Default::default()
                })
                .await;
            let snapshot = match snapshot {
                Ok(response) => response.into_inner(),
                Err(status)
                    if status.code() == tonic::Code::FailedPrecondition
                        && status.metadata().contains_key("nakode-sync-retained") =>
                {
                    return self.follow_replacements(id, sender).await;
                }
                Err(status) => return Err(status.into()),
            };
            let seed = Applied::seed(id, snapshot)
                .ok_or_else(|| SdkError::InvalidProjection("invalid sync snapshot".into()))?;
            if sender.send(Ok(seed.observed())).await.is_err() {
                return Ok(());
            }
            *held = Some(seed);
            *reported = false;
        }
        let applied = held
            .as_mut()
            .ok_or(SdkError::MissingState("incremental baseline"))?;
        let mut stream = self
            .transport
            .clone()
            .follow_session(api::FollowSessionRequest {
                session_id: id.into(),
                after: Some(applied.cursor.clone()),
            })
            .await?
            .into_inner();
        while let Some(update) = stream.message().await? {
            let Some(api::session_sync_update::Update::Batch(batch)) = update.update else {
                *held = None;
                return Ok(());
            };
            match applied.apply(batch) {
                Some(true) => {
                    *reported = false;
                    if sender.send(Ok(applied.observed())).await.is_err() {
                        return Ok(());
                    }
                }
                Some(false) => (),
                None => {
                    *held = None;
                    return Ok(());
                }
            }
        }
        Err(Status::unavailable("incremental session stream closed").into())
    }
}

#[cfg(test)]
mod tests;
