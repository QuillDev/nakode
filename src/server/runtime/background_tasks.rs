use super::{NativeServerRuntime, ServerCore};
use crate::background_tasks;
use nakode_protocol::{Command, ErrorCode, Query, QueryResult, ServiceError, Snapshot};

impl NativeServerRuntime {
    /// Owner-integration access to the runtime's background task registry.
    ///
    /// The public API is the authenticated owner integration's surface: it sees every task on
    /// this machine, including orphans, and may stop an exact task without its original owner.
    /// Agents reach tasks only through their caller-scoped tool. Work that waits on processes
    /// runs off the actor so a slow stop never blocks other requests.
    pub(super) fn handle_background_task_request(
        &mut self,
        request: nakode_server::ServerRequest,
    ) -> Option<nakode_server::ServerRequest> {
        match request {
            nakode_server::ServerRequest::Query {
                query: Query::ListBackgroundTasks { owner_session_id },
                respond,
                ..
            } => {
                let cursor = self.endpoint.cursor();
                let result = service().and_then(|service| {
                    service
                        .inventory(
                            owner_session_id
                                .as_ref()
                                .map(nakode_protocol::SessionId::as_str),
                        )
                        .map_err(internal)
                });
                let _ = respond.send(result.map(|inventory| Snapshot {
                    cursor,
                    value: QueryResult::BackgroundTasks(inventory),
                }));
                None
            }
            nakode_server::ServerRequest::Query {
                query:
                    Query::GetBackgroundTask {
                        task_id,
                        log_tail_bytes,
                    },
                respond,
                ..
            } => {
                let cursor = self.endpoint.cursor();
                tokio::spawn(async move {
                    let result = match service() {
                        Ok(service) => service
                            .inspect(&task_id, log_tail_bytes)
                            .await
                            .map_err(task_error),
                        Err(error) => Err(error),
                    };
                    let _ = respond.send(result.map(|detail| Snapshot {
                        cursor,
                        value: QueryResult::BackgroundTask(Box::new(detail)),
                    }));
                });
                None
            }
            nakode_server::ServerRequest::Command {
                command:
                    command @ (Command::DismissBackgroundTask { .. }
                    | Command::StopBackgroundTask { .. }),
                expected_revision,
                respond,
                ..
            } => {
                if expected_revision.is_some() {
                    let _ = respond.send(Err(ServiceError {
                        code: ErrorCode::InvalidRequest,
                        message: "background tasks use exact task identities, not session revision fences".to_owned(),
                        retryable: false,
                    }));
                    return None;
                }
                tokio::spawn(async move {
                    let result = execute_command(command).await;
                    let _ =
                        respond.send(result.map(|id| ServerCore::accepted(Some(id), Vec::new()).0));
                });
                None
            }
            request => Some(request),
        }
    }
}

async fn execute_command(command: Command) -> Result<String, ServiceError> {
    let service = service()?;
    match command {
        Command::DismissBackgroundTask { task_id } => {
            service.dismiss(None, &task_id).await.map_err(task_error)?;
            Ok(task_id)
        }
        Command::StopBackgroundTask { task_id } => {
            service
                .stop_ack(None, &task_id, None)
                .await
                .map_err(task_error)?;
            Ok(task_id)
        }
        _ => unreachable!("only background task commands are dispatched here"),
    }
}

fn service() -> Result<std::sync::Arc<background_tasks::BackgroundTaskService>, ServiceError> {
    background_tasks::service().ok_or_else(|| ServiceError {
        code: ErrorCode::CapabilityUnsupported,
        message: "background tasks are not initialized in this runtime".to_owned(),
        retryable: true,
    })
}

fn internal(message: String) -> ServiceError {
    ServiceError {
        code: ErrorCode::Internal,
        message,
        retryable: true,
    }
}

fn task_error(message: String) -> ServiceError {
    let code = if message.starts_with("unknown background task") {
        ErrorCode::NotFound
    } else {
        ErrorCode::Internal
    };
    ServiceError {
        retryable: code == ErrorCode::Internal,
        code,
        message,
    }
}
