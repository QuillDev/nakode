use std::{
    future::Future,
    io::{self, BufRead, Write},
};

use thiserror::Error;

use crate::{
    control_service::{self, BulkServiceShutdownReport},
    session::{SessionPurgeReport, SessionRepository, SqliteSessionRepository},
};

const WARNING: &str =
    "Warning: all Nakode sessions and their persisted session state will be removed.";
const PROMPT: &str = "Continue? [N/y] ";

#[derive(Debug, Error)]
pub enum PurgeError {
    #[error("could not read confirmation: {0}")]
    Confirmation(#[source] io::Error),
    #[error("could not write purge output: {0}")]
    Output(#[source] io::Error),
    #[error("could not enumerate Nakode runtime resources: {0}")]
    RuntimeDiscovery(String),
    #[error("session purge stopped because some runtime resources could not be terminated")]
    RuntimeCleanup,
    #[error("could not purge session persistence: {0}")]
    Persistence(String),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PurgeOutcome {
    Aborted,
    Purged(SessionPurgeReport),
}

/// Runs the deliberately interactive global session purge.
///
/// The command first asks for an explicit `y`/`Y`, then shuts down all workspace services through
/// their lifecycle sockets. Persistence is touched only after every discoverable service has
/// released its provider, shell, delegated-run, transport, and socket resources.
///
/// # Errors
/// Returns an error when confirmation I/O, runtime shutdown, or atomic persistence cleanup fails.
pub async fn run() -> Result<PurgeOutcome, PurgeError> {
    let stdin = io::stdin();
    let stdout = io::stdout();
    run_with(
        stdin.lock(),
        stdout.lock(),
        control_service::shutdown_all_services,
        || {
            let repository = SqliteSessionRepository::open_default()
                .map_err(|error| PurgeError::Persistence(error.to_string()))?;
            repository
                .purge_all()
                .map_err(|error| PurgeError::Persistence(error.to_string()))
        },
    )
    .await
}

async fn run_with<R, W, Stop, StopFuture, Purge>(
    mut reader: R,
    mut writer: W,
    stop: Stop,
    purge: Purge,
) -> Result<PurgeOutcome, PurgeError>
where
    R: BufRead,
    W: Write,
    Stop: FnOnce() -> StopFuture,
    StopFuture: Future<Output = Result<BulkServiceShutdownReport, control_service::ControlError>>,
    Purge: FnOnce() -> Result<SessionPurgeReport, PurgeError>,
{
    writeln!(writer, "{WARNING}").map_err(PurgeError::Output)?;
    write!(writer, "{PROMPT}").map_err(PurgeError::Output)?;
    writer.flush().map_err(PurgeError::Output)?;

    let mut response = String::new();
    let read = reader
        .read_line(&mut response)
        .map_err(PurgeError::Confirmation)?;
    if read == 0 || !matches!(response.trim(), "y" | "Y") {
        writeln!(writer, "Purge aborted.").map_err(PurgeError::Output)?;
        return Ok(PurgeOutcome::Aborted);
    }

    let runtime = match stop().await {
        Ok(runtime) => runtime,
        Err(error) => {
            let error = PurgeError::RuntimeDiscovery(error.to_string());
            writeln!(writer, "Failed: {error}").map_err(PurgeError::Output)?;
            writeln!(writer, "No persisted sessions were purged.").map_err(PurgeError::Output)?;
            return Err(error);
        }
    };
    if !runtime.failures.is_empty() {
        writeln!(
            writer,
            "Stopped {} active service(s); removed {} stale runtime socket set(s).",
            runtime.stopped, runtime.stale
        )
        .map_err(PurgeError::Output)?;
        for failure in &runtime.failures {
            writeln!(writer, "Failed: {failure}").map_err(PurgeError::Output)?;
        }
        writeln!(writer, "No persisted sessions were purged.").map_err(PurgeError::Output)?;
        return Err(PurgeError::RuntimeCleanup);
    }

    let report = match purge() {
        Ok(report) => report,
        Err(error) => {
            writeln!(
                writer,
                "Stopped {} active service(s); removed {} stale runtime socket set(s).",
                runtime.stopped, runtime.stale
            )
            .map_err(PurgeError::Output)?;
            writeln!(writer, "Failed: {error}").map_err(PurgeError::Output)?;
            writeln!(writer, "No persisted sessions were purged.").map_err(PurgeError::Output)?;
            return Err(error);
        }
    };
    writeln!(
        writer,
        "Purged {} session(s), {} orchestration run(s), {} agent turn(s), and {} native runtime history record(s).",
        report.sessions,
        report.orchestration_runs,
        report.agent_turns,
        report.native_runtime_sessions
    )
    .map_err(PurgeError::Output)?;
    writeln!(
        writer,
        "Stopped {} active service(s); removed {} stale runtime socket set(s).",
        runtime.stopped, runtime.stale
    )
    .map_err(PurgeError::Output)?;
    Ok(PurgeOutcome::Purged(report))
}

#[cfg(test)]
mod tests {
    use std::{cell::Cell, io::Cursor};

    use super::*;

    fn runtime_ok()
    -> impl Future<Output = Result<BulkServiceShutdownReport, control_service::ControlError>> {
        std::future::ready(Ok(BulkServiceShutdownReport::default()))
    }

    #[tokio::test]
    async fn only_explicit_yes_accepts_the_default_negative_prompt() {
        for input in ["", "\n", "n\n", "N\n", "yes\n", "maybe\n", " y extra\n"] {
            let changed = Cell::new(false);
            let mut output = Vec::new();
            let outcome = run_with(Cursor::new(input), &mut output, runtime_ok, || {
                changed.set(true);
                Ok(SessionPurgeReport::default())
            })
            .await
            .expect("decline is successful");
            assert_eq!(outcome, PurgeOutcome::Aborted, "input {input:?}");
            assert!(!changed.get(), "input {input:?} changed state");
            let output = String::from_utf8(output).expect("UTF-8 output");
            assert!(output.starts_with(&format!("{WARNING}\n{PROMPT}")));
            assert!(output.ends_with("Purge aborted.\n"));
        }

        for input in ["y\n", "Y\n", " y \n"] {
            let changed = Cell::new(false);
            let outcome = run_with(Cursor::new(input), Vec::new(), runtime_ok, || {
                changed.set(true);
                Ok(SessionPurgeReport::default())
            })
            .await
            .expect("yes purges");
            assert!(matches!(outcome, PurgeOutcome::Purged(_)));
            assert!(changed.get());
        }
    }

    #[tokio::test]
    async fn runtime_failure_reports_partial_cleanup_and_leaves_persistence_untouched() {
        let changed = Cell::new(false);
        let mut output = Vec::new();
        let result = run_with(
            Cursor::new("y\n"),
            &mut output,
            || {
                std::future::ready(Ok(BulkServiceShutdownReport {
                    stopped: 1,
                    stale: 2,
                    failures: vec!["active provider would remain".to_owned()],
                }))
            },
            || {
                changed.set(true);
                Ok(SessionPurgeReport::default())
            },
        )
        .await;

        assert!(matches!(result, Err(PurgeError::RuntimeCleanup)));
        assert!(!changed.get());
        let output = String::from_utf8(output).expect("UTF-8 output");
        assert!(output.contains("Failed: active provider would remain"));
        assert!(output.contains("No persisted sessions were purged."));
    }

    #[tokio::test]
    async fn persistence_failure_is_reported_without_claiming_success() {
        let mut output = Vec::new();
        let result = run_with(Cursor::new("y\n"), &mut output, runtime_ok, || {
            Err(PurgeError::Persistence(
                "injected storage failure".to_owned(),
            ))
        })
        .await;

        assert!(matches!(result, Err(PurgeError::Persistence(_))));
        let output = String::from_utf8(output).expect("UTF-8 output");
        assert!(output.contains("Failed: could not purge session persistence"));
        assert!(output.contains("No persisted sessions were purged."));
        assert!(!output.contains("Purged 0 session"));
    }

    #[tokio::test]
    async fn confirmed_and_repeated_purge_reports_authoritative_counts() {
        let directory = tempfile::tempdir().expect("tempdir");
        let repository = SqliteSessionRepository::open(directory.path().join("sessions.db"))
            .expect("repository");
        repository
            .create("openai-codex", "native-1", "/tmp/workspace", "Work", None)
            .expect("session");

        let first = run_with(Cursor::new("y\n"), Vec::new(), runtime_ok, || {
            repository
                .purge_all()
                .map_err(|error| PurgeError::Persistence(error.to_string()))
        })
        .await
        .expect("first purge");
        assert_eq!(
            first,
            PurgeOutcome::Purged(SessionPurgeReport {
                sessions: 1,
                ..SessionPurgeReport::default()
            })
        );

        let repeated = run_with(Cursor::new("Y\n"), Vec::new(), runtime_ok, || {
            repository
                .purge_all()
                .map_err(|error| PurgeError::Persistence(error.to_string()))
        })
        .await
        .expect("repeated purge");
        assert_eq!(
            repeated,
            PurgeOutcome::Purged(SessionPurgeReport::default())
        );
    }
}
