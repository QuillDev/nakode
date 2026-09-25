//! Source-call authenticated durable relationship commands. Actor execution does not yield between
//! authentication, the database commit and publication; provider turns are left untouched.
use super::{NativeServerRuntime, ServerCore};
use crate::child_reports::{ReportStore, relationships::Reparent};
use nakode_protocol::{Command, ServiceError};

impl NativeServerRuntime {
    pub(super) fn handle_relationship_request(
        &mut self,
        request: nakode_server::ServerRequest,
    ) -> Option<nakode_server::ServerRequest> {
        let nakode_server::ServerRequest::Command {
            command: command @ Command::ReparentChildSession { .. },
            idempotency_key,
            expected_revision,
            replay_only,
            respond,
            ..
        } = request
        else {
            return Some(request);
        };
        let result = if expected_revision.is_some() {
            Err(crate::followups::refuse(
                "use the required relationship revision, not a volatile turn revision",
            ))
        } else {
            ReportStore::open(&self.effects.persistence.database).and_then(|mut store| {
                store.reparent(
                    &command,
                    idempotency_key.as_str(),
                    replay_only,
                    super::super::unix_timestamp_ms(),
                    || self.authenticate_reparent(&command),
                )
            })
        };
        if result.is_ok() {
            self.refresh_catalogs();
            if let Command::ReparentChildSession {
                source_session_id,
                child_session_id,
                expected_parent_session_id,
                ..
            } = &command
            {
                for id in [
                    Some(child_session_id),
                    Some(source_session_id),
                    expected_parent_session_id.as_ref(),
                ]
                .into_iter()
                .flatten()
                {
                    self.core.commit_and_publish_session(&self.endpoint, id);
                }
            }
            self.followup_polling_enabled = true;
        }
        let resource = Reparent::from_command(&command)
            .map(|request| request.child.to_owned())
            .ok();
        let _ = respond.send(result.map(|()| ServerCore::accepted(resource, Vec::new()).0));
        None
    }

    fn authenticate_reparent(&self, command: &Command) -> Result<(), ServiceError> {
        let request = Reparent::from_command(command)?;
        let source = nakode_protocol::SessionId::from(request.parent);
        let call = self
            .core
            .engine_for(&source)
            .and_then(|engine| {
                engine
                    .state()
                    .external_tool_calls
                    .iter()
                    .find(|call| call.id == request.call)
            })
            .ok_or_else(|| crate::followups::refuse("claim/transfer source call is not pending"))?;
        let name = if request.transfer {
            "TransferAgent"
        } else {
            "ClaimAgent"
        };
        let arguments: serde_json::Value = serde_json::from_str(&call.arguments_json)
            .map_err(|_| crate::followups::refuse("invalid source call arguments"))?;
        let expected_parent = arguments
            .get("expectedParentSessionId")
            .and_then(serde_json::Value::as_str);
        if call.name != name
            || arguments
                .get("sessionId")
                .and_then(serde_json::Value::as_str)
                != Some(request.child)
            || arguments
                .get("expectedRelationshipRevision")
                .and_then(serde_json::Value::as_u64)
                != Some(request.revision)
            || expected_parent != request.previous_parent
        {
            return Err(crate::followups::refuse(
                "relationship command differs from the exact pending source call",
            ));
        }
        Ok(())
    }
}
