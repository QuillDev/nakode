use std::{
    path::{Path, PathBuf},
    process::{Command, ExitStatus},
};

use thiserror::Error;

const SOURCE_DIRECTORY: &str = ".nakode/src";
const CANONICAL_SOURCE_REMOTE: &str = "https://github.com/QuillDev/nakode.git";

#[derive(Debug, Error)]
pub enum UpdateError {
    #[error("HOME is not set; cannot locate the Nakode source checkout")]
    MissingHome,
    #[error(
        "the managed Nakode source checkout was not found at {0}\n\
         Reinstall Nakode with the GitHub command in README.md to create it"
    )]
    MissingSource(String),
    #[error("the Nakode installer was not found at {0}")]
    MissingInstaller(String),
    #[error("failed to start git: {0}")]
    StartGit(#[source] std::io::Error),
    #[error("git could not update the Nakode source checkout (exit status {status})")]
    GitFailed { status: ExitStatus },
    #[error("failed to start install.sh: {0}")]
    StartInstaller(#[source] std::io::Error),
    #[error("install.sh could not install the updated Nakode build (exit status {0})")]
    InstallerFailed(ExitStatus),
}

/// Updates the managed source checkout and installs the resulting build.
///
/// # Errors
///
/// Returns an error when the managed checkout is missing, Git cannot pull the
/// update, or the installer cannot complete successfully.
pub fn run() -> Result<(), UpdateError> {
    let home = std::env::var_os("HOME").ok_or(UpdateError::MissingHome)?;
    run_from(&source_directory(&PathBuf::from(home)))
}

fn source_directory(home: &Path) -> PathBuf {
    home.join(SOURCE_DIRECTORY)
}

fn run_from(source: &Path) -> Result<(), UpdateError> {
    if !source.is_dir() {
        return Err(UpdateError::MissingSource(source.display().to_string()));
    }

    let installer = source.join("install.sh");
    if !installer.is_file() {
        return Err(UpdateError::MissingInstaller(
            installer.display().to_string(),
        ));
    }

    retarget_managed_source_remote(source)?;

    println!("Updating Nakode source in {}…", source.display());
    let status = Command::new("git")
        .args(["pull", "--ff-only"])
        .current_dir(source)
        .status()
        .map_err(UpdateError::StartGit)?;
    if !status.success() {
        return Err(UpdateError::GitFailed { status });
    }

    println!("Installing the updated Nakode build…");
    let status = Command::new("sh")
        .arg("./install.sh")
        .current_dir(source)
        .status()
        .map_err(UpdateError::StartInstaller)?;
    if !status.success() {
        return Err(UpdateError::InstallerFailed(status));
    }

    println!("Nakode is up to date.");
    Ok(())
}

fn strip_url_credentials(url: &str) -> String {
    let Some((scheme, rest)) = url.split_once("://") else {
        return url.to_owned();
    };
    let Some(at) = rest.find('@') else {
        return url.to_owned();
    };
    if rest[..at].contains('/') {
        return url.to_owned();
    }
    format!("{scheme}://{}", &rest[at + 1..])
}

fn repo_key(url: &str) -> Option<String> {
    let cleaned = strip_url_credentials(url.trim());
    let host_path = if let Some(rest) = cleaned.strip_prefix("git@") {
        rest.replacen(':', "/", 1)
    } else {
        cleaned.split_once("://")?.1.to_owned()
    };
    let host_path = host_path.trim_matches('/');
    let host_path = host_path
        .strip_suffix(".git")
        .unwrap_or(host_path)
        .trim_end_matches('/');
    if host_path.is_empty() {
        None
    } else {
        Some(host_path.to_owned())
    }
}

fn is_managed_upstream_url(url: &str) -> bool {
    matches!(
        repo_key(url).as_deref(),
        Some(
            "github.com/QuillDev/nakode"
                | "origin.cursor.com/fragile-inc/nakode"
                | "origin.cursor.com/git/fragile-inc/nakode"
        )
    )
}

fn should_retarget_remote(url: &str) -> bool {
    // Compare normalized repository identity rather than the literal URL so an
    // equivalent spelling of the canonical remote -- SSH, scp-like, or
    // credential-bearing -- is recognized as already correct and left alone.
    is_managed_upstream_url(url) && repo_key(url) != repo_key(CANONICAL_SOURCE_REMOTE)
}

fn remote_has_userinfo(url: &str) -> bool {
    url.split_once("://")
        .is_some_and(|(_, rest)| rest.find('@').is_some_and(|at| !rest[..at].contains('/')))
}

fn origin_remote_url(source: &Path) -> Result<Option<String>, UpdateError> {
    // Read the configured URL. `git remote get-url` applies insteadOf rewrites
    // that may embed a short-lived Origin access token.
    let output = Command::new("git")
        .args(["config", "--get", "remote.origin.url"])
        .current_dir(source)
        .output()
        .map_err(UpdateError::StartGit)?;
    if !output.status.success() {
        return Ok(None);
    }
    let url = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    if url.is_empty() {
        Ok(None)
    } else {
        Ok(Some(url))
    }
}

fn retarget_managed_source_remote(source: &Path) -> Result<(), UpdateError> {
    let Some(current) = origin_remote_url(source)? else {
        return Ok(());
    };
    if !should_retarget_remote(&current) {
        return Ok(());
    }

    if remote_has_userinfo(&current) {
        println!("Retargeting the managed source remote to {CANONICAL_SOURCE_REMOTE}…");
    } else {
        println!(
            "Retargeting the managed source remote from {current} to {CANONICAL_SOURCE_REMOTE}…"
        );
    }

    let status = Command::new("git")
        .args(["remote", "set-url", "origin", CANONICAL_SOURCE_REMOTE])
        .current_dir(source)
        .status()
        .map_err(UpdateError::StartGit)?;
    if !status.success() {
        return Err(UpdateError::GitFailed { status });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::process::Command;

    use tempfile::tempdir;

    use super::{
        CANONICAL_SOURCE_REMOTE, UpdateError, is_managed_upstream_url, origin_remote_url,
        remote_has_userinfo, repo_key, retarget_managed_source_remote, run_from,
        should_retarget_remote, source_directory, strip_url_credentials,
    };

    #[test]
    fn rejects_a_missing_managed_checkout() {
        let home = tempdir().expect("temporary home");
        let source = home.path().join(".nakode/src");

        assert!(matches!(
            run_from(&source),
            Err(UpdateError::MissingSource(path)) if path == source.display().to_string()
        ));
    }

    #[test]
    fn rejects_a_checkout_without_an_installer() {
        let source = tempdir().expect("temporary checkout");
        let installer = source.path().join("install.sh");

        assert!(matches!(
            run_from(source.path()),
            Err(UpdateError::MissingInstaller(path)) if path == installer.display().to_string()
        ));
    }

    #[test]
    fn managed_checkout_is_under_nakode_in_home() {
        assert_eq!(
            source_directory(Path::new("/home/example")),
            Path::new("/home/example/.nakode/src")
        );
    }

    #[test]
    fn strips_https_userinfo_without_changing_ssh_scp_urls() {
        assert_eq!(
            strip_url_credentials(
                "https://x-access-token:secret@origin.cursor.com/git/fragile-inc/nakode.git"
            ),
            "https://origin.cursor.com/git/fragile-inc/nakode.git"
        );
        assert_eq!(
            strip_url_credentials("git@github.com:QuillDev/nakode.git"),
            "git@github.com:QuillDev/nakode.git"
        );
    }

    #[test]
    fn recognizes_github_and_origin_upstreams() {
        for url in [
            "https://github.com/QuillDev/nakode.git",
            "git@github.com:QuillDev/nakode",
            "ssh://git@github.com/QuillDev/nakode.git",
            "https://origin.cursor.com/fragile-inc/nakode.git",
            "https://origin.cursor.com/git/fragile-inc/nakode.git",
            "https://x-access-token:secret@origin.cursor.com/git/fragile-inc/nakode.git",
        ] {
            assert!(is_managed_upstream_url(url), "{url}");
        }

        assert!(!is_managed_upstream_url(
            "https://github.com/someone/nakode.git"
        ));
        assert!(!is_managed_upstream_url(
            "https://origin.cursor.com/other-team/nakode.git"
        ));
    }

    #[test]
    fn retargets_only_remotes_pointing_away_from_the_canonical_repository() {
        assert!(should_retarget_remote(
            "https://origin.cursor.com/git/fragile-inc/nakode.git"
        ));
        assert!(should_retarget_remote(
            "https://x-access-token:secret@origin.cursor.com/fragile-inc/nakode.git"
        ));
        assert!(!should_retarget_remote(CANONICAL_SOURCE_REMOTE));
        assert!(!should_retarget_remote(
            "https://github.com/someone/nakode.git"
        ));

        // Every spelling of the canonical repository is already correct, so the
        // configured remote is preserved instead of rewritten to the HTTPS form.
        for url in [
            "https://github.com/QuillDev/nakode.git",
            "https://github.com/QuillDev/nakode",
            "git@github.com:QuillDev/nakode.git",
            "git@github.com:QuillDev/nakode",
            "ssh://git@github.com/QuillDev/nakode.git",
        ] {
            assert!(!should_retarget_remote(url), "{url}");
        }
    }

    #[test]
    fn repo_key_normalizes_legacy_and_ssh_forms() {
        assert_eq!(
            repo_key("https://origin.cursor.com/git/fragile-inc/nakode.git").as_deref(),
            Some("origin.cursor.com/git/fragile-inc/nakode")
        );
        assert_eq!(
            repo_key("git@origin.cursor.com:fragile-inc/nakode.git").as_deref(),
            Some("origin.cursor.com/fragile-inc/nakode")
        );
    }

    #[test]
    fn detects_embedded_remote_userinfo() {
        assert!(remote_has_userinfo(
            "https://x-access-token:secret@origin.cursor.com/fragile-inc/nakode.git"
        ));
        assert!(!remote_has_userinfo(CANONICAL_SOURCE_REMOTE));
        assert!(!remote_has_userinfo("git@github.com:QuillDev/nakode.git"));
    }

    fn init_git_repo(path: &Path) {
        let status = Command::new("git")
            .args(["init", "--quiet"])
            .current_dir(path)
            .status()
            .expect("git init");
        assert!(status.success(), "git init should succeed");
    }

    #[test]
    fn retargets_cursor_origin_remote_to_github() {
        let source = tempdir().expect("temporary checkout");
        init_git_repo(source.path());
        let status = Command::new("git")
            .args([
                "remote",
                "add",
                "origin",
                "https://origin.cursor.com/fragile-inc/nakode.git",
            ])
            .current_dir(source.path())
            .status()
            .expect("git remote add");
        assert!(status.success());

        retarget_managed_source_remote(source.path()).expect("retarget Origin remote");
        assert_eq!(
            origin_remote_url(source.path())
                .expect("read origin")
                .as_deref(),
            Some(CANONICAL_SOURCE_REMOTE)
        );
    }

    #[test]
    fn retargets_legacy_origin_git_path_using_configured_url() {
        let source = tempdir().expect("temporary checkout");
        init_git_repo(source.path());
        let status = Command::new("git")
            .args([
                "remote",
                "add",
                "origin",
                "https://origin.cursor.com/git/fragile-inc/nakode.git",
            ])
            .current_dir(source.path())
            .status()
            .expect("git remote add");
        assert!(status.success());

        retarget_managed_source_remote(source.path()).expect("retarget legacy Origin path");
        assert_eq!(
            origin_remote_url(source.path())
                .expect("read origin")
                .as_deref(),
            Some(CANONICAL_SOURCE_REMOTE)
        );
    }

    #[test]
    fn leaves_unrelated_remotes_and_canonical_github_unchanged() {
        let source = tempdir().expect("temporary checkout");
        init_git_repo(source.path());
        let status = Command::new("git")
            .args([
                "remote",
                "add",
                "origin",
                "https://github.com/someone/nakode.git",
            ])
            .current_dir(source.path())
            .status()
            .expect("git remote add");
        assert!(status.success());

        retarget_managed_source_remote(source.path()).expect("ignore fork remote");
        assert_eq!(
            origin_remote_url(source.path())
                .expect("read origin")
                .as_deref(),
            Some("https://github.com/someone/nakode.git")
        );

        let status = Command::new("git")
            .args(["remote", "set-url", "origin", CANONICAL_SOURCE_REMOTE])
            .current_dir(source.path())
            .status()
            .expect("git remote set-url");
        assert!(status.success());
        retarget_managed_source_remote(source.path()).expect("keep canonical remote");
        assert_eq!(
            origin_remote_url(source.path())
                .expect("read origin")
                .as_deref(),
            Some(CANONICAL_SOURCE_REMOTE)
        );
    }

    #[test]
    fn preserves_an_ssh_spelling_of_the_canonical_github_remote() {
        let source = tempdir().expect("temporary checkout");
        init_git_repo(source.path());
        let ssh_remote = "git@github.com:QuillDev/nakode.git";
        let status = Command::new("git")
            .args(["remote", "add", "origin", ssh_remote])
            .current_dir(source.path())
            .status()
            .expect("git remote add");
        assert!(status.success());

        retarget_managed_source_remote(source.path()).expect("keep the SSH remote");
        assert_eq!(
            origin_remote_url(source.path())
                .expect("read origin")
                .as_deref(),
            Some(ssh_remote)
        );
    }
}
