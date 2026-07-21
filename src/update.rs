use std::{
    path::{Path, PathBuf},
    process::Command,
};

use thiserror::Error;

const HOMEBREW_FORMULA: &str = "quilldev/tap/nakode";

#[derive(Debug, Error)]
pub enum UpdateError {
    #[error("failed to locate the running Nakode executable: {0}")]
    CurrentExecutable(#[source] std::io::Error),
    #[error(
        "this Nakode installation is not managed by Homebrew ({executable})\n\
         Update a source installation with `git pull --ff-only` followed by `./install.sh`, \
         or install the managed release with `brew install {HOMEBREW_FORMULA}`"
    )]
    UnsupportedInstall { executable: String },
    #[error("the Homebrew executable was not found at {0}")]
    MissingHomebrew(String),
    #[error("failed to start Homebrew: {0}")]
    StartHomebrew(#[source] std::io::Error),
    #[error("Homebrew could not update Nakode (exit status {0})")]
    HomebrewFailed(std::process::ExitStatus),
}

/// Updates a Homebrew-managed Nakode installation.
///
/// # Errors
///
/// Returns an error when the current installation is not managed by Homebrew
/// or when Homebrew cannot complete the upgrade.
pub fn run() -> Result<(), UpdateError> {
    let executable = std::env::current_exe().map_err(UpdateError::CurrentExecutable)?;
    let resolved = executable.canonicalize().unwrap_or(executable);
    let Some(prefix) = homebrew_prefix(&resolved) else {
        return Err(UpdateError::UnsupportedInstall {
            executable: resolved.display().to_string(),
        });
    };
    let brew = prefix.join("bin/brew");
    if !brew.is_file() {
        return Err(UpdateError::MissingHomebrew(brew.display().to_string()));
    }

    println!("Updating Nakode with Homebrew…");
    let status = Command::new(brew)
        .args(["upgrade", HOMEBREW_FORMULA])
        .status()
        .map_err(UpdateError::StartHomebrew)?;
    if !status.success() {
        return Err(UpdateError::HomebrewFailed(status));
    }
    println!("Nakode is up to date. Restart open Nakode windows to use the new version.");
    Ok(())
}

fn homebrew_prefix(executable: &Path) -> Option<PathBuf> {
    let components = executable.components().collect::<Vec<_>>();
    let cellar = components
        .iter()
        .position(|component| component.as_os_str() == "Cellar")?;
    if components
        .get(cellar + 1)
        .is_none_or(|component| component.as_os_str() != "nakode")
    {
        return None;
    }

    let mut prefix = PathBuf::new();
    for component in &components[..cellar] {
        prefix.push(component.as_os_str());
    }
    Some(prefix)
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use super::homebrew_prefix;

    #[test]
    fn recognizes_apple_silicon_homebrew_cellar_install() {
        assert_eq!(
            homebrew_prefix(Path::new("/opt/homebrew/Cellar/nakode/0.2.0/bin/nakode")),
            Some(PathBuf::from("/opt/homebrew"))
        );
    }

    #[test]
    fn recognizes_intel_homebrew_cellar_install() {
        assert_eq!(
            homebrew_prefix(Path::new("/usr/local/Cellar/nakode/0.2.0/bin/nakode")),
            Some(PathBuf::from("/usr/local"))
        );
    }

    #[test]
    fn rejects_source_and_other_cellar_installs() {
        assert_eq!(
            homebrew_prefix(Path::new("/Users/quill/.local/bin/nakode")),
            None
        );
        assert_eq!(
            homebrew_prefix(Path::new("/opt/homebrew/Cellar/other/1.0/bin/nakode")),
            None
        );
    }
}
