//! Publications are wakeups, not cursors. Subscribe before the first actor read and read
//! complete retained batches after every wake (including broadcast overflow).
use super::{ApiStream, GrpcService, ReceiverStream, api, protocol, session, status};
use protocol::sync::{Batch, Cursor, Read, Reply};
use tonic::Status;

fn wire(c: Cursor) -> api::SessionSyncCursor {
    api::SessionSyncCursor {
        session_id: c.session_id.to_string(),
        incarnation: c.incarnation,
        revision: c.revision,
    }
}
fn batch(b: Batch) -> api::SessionSyncUpdate {
    let mut projection = session(*b.projection);
    if let Some(agent) = &mut projection.active_agent_session {
        agent.transcript = None;
    }
    api::SessionSyncUpdate {
        update: Some(api::session_sync_update::Update::Batch(
            api::SessionSyncBatch {
                base: Some(wire(b.base)),
                next: Some(wire(b.next)),
                projection: Some(projection),
                order: b.changes.entry_ids.map(|ids| api::TranscriptOrder {
                    entry_ids: ids.into_iter().map(|id| id.to_string()).collect(),
                }),
            },
        )),
    }
}
fn reset() -> api::SessionSyncUpdate {
    api::SessionSyncUpdate {
        update: Some(api::session_sync_update::Update::ResetRequired(
            api::SessionSyncResetRequired {
                reason: "cursor is not a retained boundary for this session".into(),
            },
        )),
    }
}

impl GrpcService {
    async fn sync_read(&self, id: &protocol::SessionId, read: Read) -> Result<Reply, Status> {
        if !self
            .endpoint
            .capabilities()
            .supports(protocol::ServiceCapability::SessionSyncV1)
        {
            return Err(Status::unimplemented("SessionSyncV1 unavailable"));
        }
        let is_snapshot = matches!(read, Read::Snapshot);
        match self
            .endpoint
            .read_session_sync(self.client_id.clone(), id.clone(), read)
            .await
            .map_err(|error| {
                if error.retryable && error.code == protocol::ErrorCode::Internal {
                    Status::unavailable(error.message)
                } else {
                    status(error)
                }
            })? {
            protocol::SubscriptionView::Sync(reply) => Ok(reply),
            protocol::SubscriptionView::Session(_) if !is_snapshot => Ok(Reply::Reset),
            protocol::SubscriptionView::Session(_) => {
                let mut status =
                    Status::failed_precondition("retained session has no live publication history");
                status.metadata_mut().insert(
                    "nakode-sync-retained",
                    "1".parse().expect("static metadata"),
                );
                Err(status)
            }
            _ => Err(Status::internal("unexpected incremental session reply")),
        }
    }
    pub(super) async fn sync_snapshot(
        &self,
        id: String,
    ) -> Result<api::SessionSyncSnapshot, Status> {
        let Reply::Snapshot(cursor, state) = self.sync_read(&id.into(), Read::Snapshot).await?
        else {
            return Err(Status::internal("expected incremental snapshot"));
        };
        Ok(api::SessionSyncSnapshot {
            cursor: Some(wire(cursor)),
            state: Some(session(*state)),
        })
    }
    pub(super) async fn sync_stream(
        &self,
        input: api::FollowSessionRequest,
    ) -> Result<ApiStream<api::SessionSyncUpdate>, Status> {
        let mut wakes = self.endpoint.subscribe_publications();
        let id = protocol::SessionId::from(input.session_id);
        let scope = protocol::SubscriptionScope::Session {
            session_id: id.clone(),
        };
        let mut after = input.after.map(|c| Cursor {
            session_id: c.session_id.into(),
            incarnation: c.incarnation,
            revision: c.revision,
        });
        let mut reply = self.sync_read(&id, Read::After(after.clone())).await?;
        let service = self.clone();
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        tokio::spawn(async move {
            loop {
                match reply {
                    Reply::Reset => {
                        let _ = sender.send(Ok(reset())).await;
                        return;
                    }
                    Reply::Changes(batches) => {
                        for b in batches {
                            after = Some(b.next.clone());
                            if sender.send(Ok(batch(b))).await.is_err() {
                                return;
                            }
                        }
                    }
                    Reply::Snapshot(..) => {
                        let _ = sender
                            .send(Err(Status::internal("expected incremental changes")))
                            .await;
                        return;
                    }
                }
                loop {
                    tokio::select! {
                        () = sender.closed() => return,
                        () = service.endpoint.inner.subscription_requests.closed() => {
                            let _ = sender.send(Err(Status::unavailable("session runtime stopped"))).await;
                            return;
                        }
                        wake = wakes.recv() => match wake {
                            Ok(event) if event.scopes.contains(&scope) || event.scopes.iter().any(|scope| matches!(scope,protocol::SubscriptionScope::Workspace { .. })) => break,
                            Ok(_) => (),
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => break,
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                        }
                    }
                }
                super::drain_pending_publications(&mut wakes);
                match service.sync_read(&id, Read::After(after.clone())).await {
                    Ok(next) => reply = next,
                    Err(error) => {
                        let _ = sender.send(Err(error)).await;
                        return;
                    }
                }
            }
        });
        Ok(Box::pin(ReceiverStream::new(receiver)))
    }
}
