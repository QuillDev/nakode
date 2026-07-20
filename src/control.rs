use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{UnixListener, UnixStream},
};

#[derive(Debug, Error)]
pub enum ControlError {
    #[error("Nakode control socket error at {path}: {source}")]
    Io {
        path: String,
        source: std::io::Error,
    },
    #[error("invalid Nakode control message: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Nakode control service closed without a result")]
    MissingResponse,
    #[error("a Nakode control service is already running at {0}")]
    AlreadyRunning(String),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AgentInvocation {
    pub agent: String,
    pub session_id: String,
    pub task: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AgentResponse {
    pub success: bool,
    pub result: String,
}

pub struct IncomingInvocation {
    pub id: u64,
    pub invocation: AgentInvocation,
    response: tokio::sync::oneshot::Sender<AgentResponse>,
}

pub struct ControlServer {
    pub requests: tokio::sync::mpsc::Receiver<IncomingInvocation>,
    task: tokio::task::JoinHandle<()>,
}

impl ControlServer {
    /// Binds the workspace control socket.
    ///
    /// # Errors
    /// Returns an error when the socket cannot be prepared or bound.
    pub async fn bind(path: &Path) -> Result<Self, ControlError> {
        if path.exists() {
            if UnixStream::connect(path).await.is_ok() {
                return Err(ControlError::AlreadyRunning(path.display().to_string()));
            }
            std::fs::remove_file(path).map_err(|source| ControlError::Io {
                path: path.display().to_string(),
                source,
            })?;
        }
        let listener = UnixListener::bind(path).map_err(|source| ControlError::Io {
            path: path.display().to_string(),
            source,
        })?;
        let (tx, requests) = tokio::sync::mpsc::channel(32);
        let task = tokio::spawn(async move {
            let mut next_id = 1_u64;
            while let Ok((stream, _)) = listener.accept().await {
                let tx = tx.clone();
                let id = next_id;
                next_id = next_id.wrapping_add(1);
                tokio::spawn(async move {
                    handle_connection(stream, id, tx).await;
                });
            }
        });
        Ok(Self { requests, task })
    }

    pub fn shutdown(self, path: &Path) {
        self.task.abort();
        let _ = std::fs::remove_file(path);
    }
}

impl IncomingInvocation {
    pub fn respond(self, response: AgentResponse) {
        let _ = self.response.send(response);
    }
}

async fn handle_connection(
    stream: UnixStream,
    id: u64,
    tx: tokio::sync::mpsc::Sender<IncomingInvocation>,
) {
    let (reader, mut writer) = stream.into_split();
    let mut line = String::new();
    if BufReader::new(reader).read_line(&mut line).await.is_err() {
        return;
    }
    let Ok(invocation) = serde_json::from_str(&line) else {
        return;
    };
    let (response, receive) = tokio::sync::oneshot::channel();
    if tx
        .send(IncomingInvocation {
            id,
            invocation,
            response,
        })
        .await
        .is_err()
    {
        return;
    }
    if let Ok(response) = receive.await
        && let Ok(encoded) = serde_json::to_string(&response)
    {
        let _ = writer.write_all(encoded.as_bytes()).await;
        let _ = writer.write_all(b"\n").await;
    }
}

/// Sends an invocation to the workspace control service and waits for its result.
///
/// # Errors
/// Returns an error when the service cannot be reached or exchanges an invalid response.
pub async fn invoke(
    path: &Path,
    invocation: &AgentInvocation,
) -> Result<AgentResponse, ControlError> {
    let display_path = path.display().to_string();
    let stream = UnixStream::connect(path)
        .await
        .map_err(|source| ControlError::Io {
            path: display_path.clone(),
            source,
        })?;
    let (reader, mut writer) = stream.into_split();
    let encoded = serde_json::to_string(invocation)?;
    writer
        .write_all(encoded.as_bytes())
        .await
        .map_err(|source| ControlError::Io {
            path: display_path.clone(),
            source,
        })?;
    writer
        .write_all(b"\n")
        .await
        .map_err(|source| ControlError::Io {
            path: display_path.clone(),
            source,
        })?;
    let mut line = String::new();
    BufReader::new(reader)
        .read_line(&mut line)
        .await
        .map_err(|source| ControlError::Io {
            path: display_path,
            source,
        })?;
    if line.is_empty() {
        return Err(ControlError::MissingResponse);
    }
    Ok(serde_json::from_str(&line)?)
}

#[must_use]
pub fn socket_path(workspace: &Path) -> PathBuf {
    workspace.join(".nakode").join("control.sock")
}

#[must_use]
pub fn client_socket_path(workspace: &Path) -> PathBuf {
    let current = socket_path(workspace);
    if current.exists() {
        return current;
    }
    let legacy = workspace.join(".nako-agent").join("control.sock");
    if legacy.exists() { legacy } else { current }
}

#[cfg(test)]
mod tests {
    use super::{
        AgentInvocation, AgentResponse, ControlError, ControlServer, client_socket_path, invoke,
        socket_path,
    };

    #[test]
    fn client_socket_falls_back_to_the_legacy_workspace_path() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let legacy = directory.path().join(".nako-agent/control.sock");
        std::fs::create_dir_all(legacy.parent().expect("legacy parent"))
            .expect("legacy control directory");
        std::fs::write(&legacy, []).expect("legacy socket placeholder");

        assert_eq!(client_socket_path(directory.path()), legacy);
        assert_eq!(
            socket_path(directory.path()),
            directory.path().join(".nakode/control.sock")
        );
    }

    #[tokio::test]
    async fn invocation_round_trips_through_the_control_service() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("control.sock");
        let mut server = ControlServer::bind(&path).await.expect("control server");
        let invocation = AgentInvocation {
            agent: "reviewer".to_owned(),
            session_id: "session-7".to_owned(),
            task: "Review auth".to_owned(),
        };
        let client_path = path.clone();
        let client = tokio::spawn(async move { invoke(&client_path, &invocation).await });

        let request = server.requests.recv().await.expect("incoming invocation");
        assert_eq!(request.invocation.agent, "reviewer");
        assert_eq!(request.invocation.session_id, "session-7");
        assert_eq!(request.invocation.task, "Review auth");
        request.respond(AgentResponse {
            success: true,
            result: "No defects".to_owned(),
        });

        let response = client.await.expect("client task").expect("agent response");
        assert!(response.success);
        assert_eq!(response.result, "No defects");
        server.shutdown(&path);
    }

    #[tokio::test]
    async fn concurrent_invocations_keep_independent_response_channels() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("control.sock");
        let mut server = ControlServer::bind(&path).await.expect("control server");
        let first_path = path.clone();
        let first = tokio::spawn(async move {
            invoke(
                &first_path,
                &AgentInvocation {
                    agent: "explorer".to_owned(),
                    session_id: "session-7".to_owned(),
                    task: "hardware".to_owned(),
                },
            )
            .await
        });
        let second_path = path.clone();
        let second = tokio::spawn(async move {
            invoke(
                &second_path,
                &AgentInvocation {
                    agent: "explorer".to_owned(),
                    session_id: "session-7".to_owned(),
                    task: "operating system".to_owned(),
                },
            )
            .await
        });

        for _ in 0..2 {
            let request = server.requests.recv().await.expect("concurrent invocation");
            let result = format!("{} complete", request.invocation.task);
            request.respond(AgentResponse {
                success: true,
                result,
            });
        }

        let first = first
            .await
            .expect("first client task")
            .expect("first result");
        let second = second
            .await
            .expect("second client task")
            .expect("second result");
        assert_eq!(first.result, "hardware complete");
        assert_eq!(second.result, "operating system complete");
        server.shutdown(&path);
    }

    #[tokio::test]
    async fn binding_does_not_replace_a_live_control_service() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("control.sock");
        let server = ControlServer::bind(&path).await.expect("control server");

        let error = ControlServer::bind(&path)
            .await
            .err()
            .expect("second server must fail");
        assert!(matches!(error, ControlError::AlreadyRunning(_)));
        server.shutdown(&path);
    }
}
