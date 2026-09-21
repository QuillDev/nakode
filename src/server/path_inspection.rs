//! Read-only filesystem/Git inspection, outside the canonical state actor.
#[cfg(test)]
#[path = "path_inspection_tests.rs"]
mod tests;
use std::{path::Path, time::Duration};

use nakode_protocol::{ErrorCode, ServiceError, WorkspacePathInspectionView};

use super::{
    canonical_working_directory, domain_error, sanitized_repository_identity, service_error,
};

static CANONICALIZATIONS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(4);

pub(super) async fn inspect_workspace_path(
    requested: &str,
    expected_git_repository: Option<&str>,
) -> Result<WorkspacePathInspectionView, ServiceError> {
    inspect_with_git(requested, expected_git_repository, Path::new("git")).await
}

async fn inspect_with_git(
    requested: &str,
    expected_git_repository: Option<&str>,
    executable: &Path,
) -> Result<WorkspacePathInspectionView, ServiceError> {
    let requested = requested.to_owned();
    let permit = CANONICALIZATIONS.try_acquire().map_err(|_| {
        service_error(
            ErrorCode::ProviderUnavailable,
            "workspace filesystem inspection is busy",
            true,
        )
    })?;
    let canonical_path = tokio::time::timeout(
        Duration::from_secs(5),
        tokio::task::spawn_blocking(move || {
            // Filesystem syscalls cannot be cancelled. Keep their slot until the real work ends,
            // even when the caller or the deadline stops waiting for this task.
            let _permit = permit;
            canonical_working_directory(Some(&requested), &requested).map_err(domain_error)
        }),
    )
    .await
    .map_err(|_| {
        service_error(
            ErrorCode::Internal,
            "workspace filesystem inspection timed out",
            true,
        )
    })?
    .map_err(|error| service_error(ErrorCode::Internal, &error.to_string(), true))??;
    let (origin, branch, revision, status) = tokio::try_join!(
        git(
            executable,
            &canonical_path,
            &["config", "--get", "remote.origin.url"]
        ),
        git(
            executable,
            &canonical_path,
            &["symbolic-ref", "--quiet", "--short", "HEAD"]
        ),
        git(executable, &canonical_path, &["rev-parse", "HEAD"]),
        git(
            executable,
            &canonical_path,
            &["status", "--porcelain=v1", "--untracked-files=normal"]
        ),
    )?;
    let git_repository = origin
        .filter(|value| !value.is_empty())
        .map(|value| sanitized_repository_identity(&value));
    if let Some(expected) = expected_git_repository {
        let expected = sanitized_repository_identity(expected);
        let actual = git_repository.as_deref().ok_or_else(|| {
            service_error(
                ErrorCode::Conflict,
                "workspace path has no configured origin repository",
                false,
            )
        })?;
        if actual != expected {
            return Err(service_error(
                ErrorCode::Conflict,
                &format!("workspace repository mismatch: expected {expected}, found {actual}"),
                false,
            ));
        }
    }
    // A failed status in a known repository is not proof of a clean checkout.
    if status.is_none() && (git_repository.is_some() || revision.is_some()) {
        return Err(service_error(
            ErrorCode::Internal,
            "workspace Git status is unavailable",
            true,
        ));
    }
    Ok(WorkspacePathInspectionView {
        canonical_path,
        git_repository,
        branch: branch.filter(|value| !value.is_empty()),
        revision: revision.filter(|value| !value.is_empty()),
        dirty: status.is_some_and(|value| !value.is_empty()),
    })
}

async fn git(
    executable: &Path,
    directory: &str,
    arguments: &[&str],
) -> Result<Option<String>, ServiceError> {
    tokio::time::timeout(
        Duration::from_secs(5),
        git_output(executable, directory, arguments),
    )
    .await
    .map_err(|_| {
        service_error(
            ErrorCode::Internal,
            "workspace Git inspection timed out",
            true,
        )
    })?
}

#[cfg(unix)]
struct GitProcessGroup(Option<u32>);

#[cfg(unix)]
impl Drop for GitProcessGroup {
    fn drop(&mut self) {
        if let Some(id) = self.0.and_then(|id| i32::try_from(id).ok()) {
            let _ = nix::sys::signal::killpg(
                nix::unistd::Pid::from_raw(id),
                nix::sys::signal::Signal::SIGKILL,
            );
        }
    }
}

async fn git_output(
    executable: &Path,
    directory: &str,
    arguments: &[&str],
) -> Result<Option<String>, ServiceError> {
    use std::process::Stdio;
    use tokio::io::AsyncReadExt;
    let failure = |_| service_error(ErrorCode::Internal, "workspace Git inspection failed", true);
    let mut command = tokio::process::Command::new(executable);
    command
        .envs(crate::machine_path::environment())
        .args(["-C", directory])
        .args(arguments)
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_OPTIONAL_LOCKS", "0")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.as_std_mut().process_group(0);
    }
    let mut child = match command.spawn() {
        Ok(child) => child,
        // Git is optional for a general workspace. Missing metadata never satisfies an
        // explicitly requested repository identity, which is checked by the caller.
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(failure(error)),
    };
    #[cfg(unix)]
    let _group = GitProcessGroup(child.id());
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| service_error(ErrorCode::Internal, "Git stdout unavailable", true))?;
    let is_status = arguments.first() == Some(&"status");
    // Status only needs evidence of one change, not a retained file catalogue.
    let mut bytes = Vec::new();
    stdout
        .take(if is_status { 1 } else { 4097 })
        .read_to_end(&mut bytes)
        .await
        .map_err(failure)?;
    if is_status && !bytes.is_empty() {
        child.kill().await.map_err(failure)?;
        return Ok(Some("dirty".to_owned()));
    }
    if bytes.len() > 4096 {
        return Err(service_error(
            ErrorCode::Internal,
            "workspace Git metadata exceeds its limit",
            false,
        ));
    }
    let status = child.wait().await.map_err(failure)?;
    Ok(status
        .success()
        .then(|| String::from_utf8_lossy(&bytes).trim().to_owned()))
}
