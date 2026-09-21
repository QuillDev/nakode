use super::{git, inspect_with_git};
use nakode_protocol::ErrorCode;
use std::path::Path;

fn workspace() -> tempfile::TempDir {
    let temporary = Path::new(env!("CARGO_MANIFEST_DIR")).join(".tmp");
    std::fs::create_dir_all(&temporary).expect("temporary root");
    tempfile::tempdir_in(temporary).expect("workspace")
}

#[tokio::test]
async fn missing_git_keeps_general_workspace_available_but_cannot_verify_repository() {
    let workspace = workspace();
    let missing = workspace.path().join("missing-git");
    let path = workspace.path().to_str().expect("path");
    let inspection = inspect_with_git(path, None, &missing)
        .await
        .expect("general workspace");
    assert!(inspection.git_repository.is_none());
    assert!(!inspection.dirty);
    let error = inspect_with_git(path, Some("github.com/team/repo"), &missing)
        .await
        .expect_err("unverified repository");
    assert_eq!(error.code, ErrorCode::Conflict);
}

#[cfg(unix)]
fn executable(workspace: &Path, body: &str) -> std::path::PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let executable = workspace.join("git-probe");
    std::fs::write(&executable, format!("#!/bin/sh\n{body}\n")).expect("probe script");
    std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700))
        .expect("executable");
    executable
}

#[cfg(unix)]
#[tokio::test]
async fn git_output_is_bounded_and_status_stops_at_first_change() {
    let workspace = workspace();
    let executable = executable(workspace.path(), "exec yes change");
    let path = workspace.path().to_str().expect("path");
    let error = git(&executable, path, &["config"])
        .await
        .expect_err("bounded metadata");
    assert!(error.message.contains("exceeds its limit"));
    assert_eq!(
        git(&executable, path, &["status"])
            .await
            .expect("dirty status"),
        Some("dirty".to_owned())
    );
}

#[cfg(unix)]
#[tokio::test]
async fn stalled_git_times_out_instead_of_reporting_clean() {
    let workspace = workspace();
    let executable = executable(workspace.path(), "exec sleep 30");
    let error = git(
        &executable,
        workspace.path().to_str().expect("path"),
        &["status"],
    )
    .await
    .expect_err("deadline");
    assert!(error.retryable);
    assert!(error.message.contains("timed out"));
}
