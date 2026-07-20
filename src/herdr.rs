use std::{
    ffi::{OsStr, OsString},
    process::Stdio,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use tokio::{process::Command, sync::mpsc, task::JoinHandle};

use crate::state::AppState;

const REPORT_SOURCE: &str = "nakode:native";
const AGENT_LABEL: &str = "nakode";
const COMMAND_TIMEOUT: Duration = Duration::from_secs(2);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AgentStatus {
    Idle,
    Working,
    Blocked,
}

impl AgentStatus {
    const fn from_state(blocked: bool, busy: bool) -> Self {
        if blocked {
            Self::Blocked
        } else if busy {
            Self::Working
        } else {
            Self::Idle
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Working => "working",
            Self::Blocked => "blocked",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AgentSnapshot {
    status: AgentStatus,
    session_id: Option<String>,
}

impl AgentSnapshot {
    fn from_state(state: &AppState) -> Self {
        Self {
            status: AgentStatus::from_state(
                !state.approvals.is_empty() || !state.questions.is_empty(),
                state.is_busy(),
            ),
            session_id: state.session_id.clone(),
        }
    }
}

enum Report {
    State {
        snapshot: AgentSnapshot,
        sequence: u64,
    },
    Release {
        sequence: u64,
    },
}

/// Optional lifecycle bridge for a Nakode TUI running inside a Herdr pane.
///
/// The bridge deliberately fails open. Herdr is not a Nakode runtime dependency,
/// and a missing or unavailable Herdr CLI must never prevent Nakode from running.
pub(crate) struct Reporter {
    reports: mpsc::UnboundedSender<Report>,
    worker: JoinHandle<()>,
    last_snapshot: Option<AgentSnapshot>,
    next_sequence: u64,
}

impl Reporter {
    #[must_use]
    pub(crate) fn from_environment() -> Option<Self> {
        if std::env::var_os("HERDR_ENV").as_deref() != Some(OsStr::new("1")) {
            return None;
        }
        let pane_id = non_empty_environment_value("HERDR_PANE_ID")?;
        let program = non_empty_environment_value("HERDR_BIN_PATH")
            .unwrap_or_else(|| OsString::from("herdr"));
        let (reports, receive) = mpsc::unbounded_channel();
        let worker = tokio::spawn(run_worker(program, pane_id, receive));
        Some(Self {
            reports,
            worker,
            last_snapshot: None,
            next_sequence: initial_sequence(),
        })
    }

    pub(crate) fn sync(&mut self, state: &AppState) {
        let snapshot = AgentSnapshot::from_state(state);
        if self.last_snapshot.as_ref() == Some(&snapshot) {
            return;
        }
        let sequence = self.take_sequence();
        if self
            .reports
            .send(Report::State {
                snapshot: snapshot.clone(),
                sequence,
            })
            .is_ok()
        {
            self.last_snapshot = Some(snapshot);
        }
    }

    pub(crate) async fn shutdown(mut self) {
        let sequence = self.take_sequence();
        let _ = self.reports.send(Report::Release { sequence });
        drop(self.reports);
        let mut worker = self.worker;
        if tokio::time::timeout(SHUTDOWN_TIMEOUT, &mut worker)
            .await
            .is_err()
        {
            worker.abort();
        }
    }

    fn take_sequence(&mut self) -> u64 {
        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.saturating_add(1);
        sequence
    }
}

fn non_empty_environment_value(name: &str) -> Option<OsString> {
    std::env::var_os(name).filter(|value| !value.is_empty())
}

fn initial_sequence() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .try_into()
        .unwrap_or(u64::MAX.saturating_sub(1_000_000))
}

async fn run_worker(
    program: OsString,
    pane_id: OsString,
    mut reports: mpsc::UnboundedReceiver<Report>,
) {
    while let Some(report) = reports.recv().await {
        let arguments = report_arguments(&pane_id, &report);
        let mut command = Command::new(&program);
        command
            .args(arguments)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        let _ = tokio::time::timeout(COMMAND_TIMEOUT, command.status()).await;
    }
}

fn report_arguments(pane_id: &OsStr, report: &Report) -> Vec<OsString> {
    let mut arguments = vec![OsString::from("pane")];
    match report {
        Report::State { snapshot, sequence } => {
            arguments.extend([
                OsString::from("report-agent"),
                pane_id.to_os_string(),
                OsString::from("--source"),
                OsString::from(REPORT_SOURCE),
                OsString::from("--agent"),
                OsString::from(AGENT_LABEL),
                OsString::from("--state"),
                OsString::from(snapshot.status.as_str()),
                OsString::from("--seq"),
                OsString::from(sequence.to_string()),
            ]);
            if let Some(session_id) = &snapshot.session_id {
                arguments.extend([
                    OsString::from("--agent-session-id"),
                    OsString::from(session_id),
                ]);
            }
        }
        Report::Release { sequence } => arguments.extend([
            OsString::from("release-agent"),
            pane_id.to_os_string(),
            OsString::from("--source"),
            OsString::from(REPORT_SOURCE),
            OsString::from("--agent"),
            OsString::from(AGENT_LABEL),
            OsString::from("--seq"),
            OsString::from(sequence.to_string()),
        ]),
    }
    arguments
}

#[cfg(test)]
mod tests {
    use std::ffi::{OsStr, OsString};

    use super::{AgentSnapshot, AgentStatus, Report, report_arguments};

    #[test]
    fn blocked_takes_precedence_over_working() {
        assert_eq!(AgentStatus::from_state(true, true), AgentStatus::Blocked);
        assert_eq!(AgentStatus::from_state(false, true), AgentStatus::Working);
        assert_eq!(AgentStatus::from_state(false, false), AgentStatus::Idle);
    }

    #[test]
    fn state_report_includes_the_logical_session() {
        let arguments = report_arguments(
            OsStr::new("w1:p2"),
            &Report::State {
                snapshot: AgentSnapshot {
                    status: AgentStatus::Working,
                    session_id: Some("nakode-session-1".to_owned()),
                },
                sequence: 42,
            },
        );
        assert_eq!(
            arguments,
            [
                "pane",
                "report-agent",
                "w1:p2",
                "--source",
                "nakode:native",
                "--agent",
                "nakode",
                "--state",
                "working",
                "--seq",
                "42",
                "--agent-session-id",
                "nakode-session-1",
            ]
            .map(OsString::from)
        );
    }

    #[test]
    fn release_report_clears_nakode_authority() {
        let arguments = report_arguments(OsStr::new("w1:p2"), &Report::Release { sequence: 43 });
        assert_eq!(
            arguments,
            [
                "pane",
                "release-agent",
                "w1:p2",
                "--source",
                "nakode:native",
                "--agent",
                "nakode",
                "--seq",
                "43",
            ]
            .map(OsString::from)
        );
    }
}
