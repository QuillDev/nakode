pub mod activation;
pub mod agent;
pub mod agent_cli;
pub(crate) mod api_projection;
pub mod app;
pub mod backend;
pub mod claude;
pub mod clipboard;
pub mod codemode_worker;
pub mod codex;
pub mod commands;
pub mod config;
pub mod control_service;
pub mod controls;
pub mod credential;
pub mod cursor;
pub mod devin;
pub mod diagnostics;
pub mod domain_transcript;
pub mod editor;
pub mod execution_host;
pub mod glm;
pub mod handoff;
mod herdr;
pub mod kimi;
mod markdown;
pub mod mcp;
mod media;
pub mod memory;
mod native_client;
pub mod personality;
pub mod pty;
pub mod purge;
pub mod remote;
pub mod render;
pub mod runtime;
pub mod searchable_dropdown;
pub mod selection;
pub mod server;
pub mod service;
pub mod service_cli;
pub mod service_log;
pub mod session;
pub mod settings;
mod shell;
pub mod skill;
pub mod soul;
pub mod state;
pub mod terminal;
pub mod terminal_image;
pub mod tools;
pub mod transcript;
mod tui_client;
pub mod tui_eval;
mod tui_input;
pub mod tui_state;
pub mod update;
pub mod vision;
pub mod web;

/// Immutable source revision embedded by the trusted build pipeline.
///
/// The managed installer builds an immutable source snapshot and writes its revision into this
/// otherwise-empty tracked input. Direct or dirty builds therefore report no revision instead of
/// inferring one at runtime.
pub const BUILD_REVISION: Option<&str> =
    validated_build_revision(Some(include_str!("build_revision.txt")));

const fn validated_build_revision(value: Option<&'static str>) -> Option<&'static str> {
    let Some(value) = value else {
        return None;
    };
    let bytes = value.as_bytes();
    if bytes.len() != 40 && bytes.len() != 64 {
        return None;
    }

    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        if !matches!(byte, b'0'..=b'9' | b'a'..=b'f') {
            return None;
        }
        index += 1;
    }
    Some(value)
}

#[cfg(test)]
mod build_revision_tests {
    use super::{BUILD_REVISION, validated_build_revision};

    #[test]
    fn direct_checkout_build_has_no_embedded_revision() {
        assert_eq!(BUILD_REVISION, None);
    }

    #[test]
    fn accepts_lowercase_sha1_and_sha256_revisions() {
        let sha1 = "105cba008073dc1230df660d382bf131af82c063";
        let sha256 = "105cba008073dc1230df660d382bf131af82c063105cba008073dc1230df660d";

        assert_eq!(validated_build_revision(Some(sha1)), Some(sha1));
        assert_eq!(validated_build_revision(Some(sha256)), Some(sha256));
    }

    #[test]
    fn rejects_missing_or_untrusted_revision_shapes() {
        assert_eq!(validated_build_revision(None), None);
        assert_eq!(validated_build_revision(Some("")), None);
        assert_eq!(validated_build_revision(Some("105cba0")), None);
        assert_eq!(
            validated_build_revision(Some("105CBA008073DC1230DF660D382BF131AF82C063")),
            None
        );
        assert_eq!(
            validated_build_revision(Some("105cba008073dc1230df660d382bf131af82c06g")),
            None
        );
    }
}
