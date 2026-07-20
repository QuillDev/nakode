use std::{collections::HashMap, ffi::OsString, path::Path, time::Duration};

use serde::Deserialize;
use tokio_util::sync::CancellationToken;

use super::process::{ProcessRequest, capture_process};

const REWRITE_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Eq, PartialEq)]
pub enum RewriteDecision {
    Command(String),
    Passthrough,
    Blocked(String),
    Interrupted,
}

#[derive(Deserialize)]
struct RewritePayload {
    outcome: String,
    command: String,
}

pub async fn rewrite_command(
    workspace: &Path,
    command: &str,
    environment: &HashMap<String, String>,
    cancellation: &CancellationToken,
) -> RewriteDecision {
    if is_hypa_command(command) {
        return RewriteDecision::Passthrough;
    }

    let arguments = [
        OsString::from("rewrite"),
        OsString::from("--json"),
        OsString::from(command),
    ];
    let result = capture_process(
        workspace,
        ProcessRequest {
            program: "hypa",
            arguments: &arguments,
            input: None,
            environment: Some(environment),
            timeout: Some(REWRITE_TIMEOUT),
        },
        cancellation,
    )
    .await;

    let Ok(result) = result else {
        return RewriteDecision::Passthrough;
    };
    if result.interrupted {
        return RewriteDecision::Interrupted;
    }
    if !result.success {
        return RewriteDecision::Passthrough;
    }

    decode_rewrite(&result.stdout)
}

fn decode_rewrite(stdout: &[u8]) -> RewriteDecision {
    let Ok(payload) = serde_json::from_slice::<RewritePayload>(stdout) else {
        return RewriteDecision::Passthrough;
    };
    match payload.outcome.as_str() {
        "Rewritten" | "GenericWrapper" if !payload.command.trim().is_empty() => {
            RewriteDecision::Command(payload.command)
        }
        "Deny" => RewriteDecision::Blocked("command blocked by Hypa policy".to_owned()),
        "Ask" => RewriteDecision::Blocked(
            "Hypa requires confirmation; command was not executed in unattended mode".to_owned(),
        ),
        _ => RewriteDecision::Passthrough,
    }
}

fn is_hypa_command(command: &str) -> bool {
    let command = command.trim_start();
    command == "hypa" || command.starts_with("hypa ")
}

#[cfg(test)]
mod tests {
    use super::{RewriteDecision, decode_rewrite, is_hypa_command};

    #[test]
    fn rewrite_payloads_follow_hypa_policy() {
        assert_eq!(
            decode_rewrite(br#"{"outcome":"Rewritten","command":"hypa -c cargo test"}"#),
            RewriteDecision::Command("hypa -c cargo test".to_owned())
        );
        assert_eq!(
            decode_rewrite(br#"{"outcome":"GenericWrapper","command":"hypa -c cargo test"}"#),
            RewriteDecision::Command("hypa -c cargo test".to_owned())
        );
        assert_eq!(
            decode_rewrite(br#"{"outcome":"Passthrough","command":"cargo test"}"#),
            RewriteDecision::Passthrough
        );
        assert!(matches!(
            decode_rewrite(br#"{"outcome":"Deny","command":""}"#),
            RewriteDecision::Blocked(message) if message.contains("blocked")
        ));
        assert!(matches!(
            decode_rewrite(br#"{"outcome":"Ask","command":"cargo test"}"#),
            RewriteDecision::Blocked(message) if message.contains("confirmation")
        ));
        assert_eq!(decode_rewrite(b"not json"), RewriteDecision::Passthrough);
    }

    #[test]
    fn explicit_hypa_commands_are_not_nested() {
        assert!(is_hypa_command("hypa -c 'cargo test'"));
        assert!(is_hypa_command("  hypa"));
        assert!(!is_hypa_command("hypaclassifier"));
        assert!(!is_hypa_command("cargo test"));
    }
}
