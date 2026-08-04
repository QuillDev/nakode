use std::{
    path::{Path, PathBuf},
    process::Command,
};

use thiserror::Error;

const SOURCE_DIRECTORY: &str = ".nakode/src";

#[derive(Debug, Error)]
pub enum UpdateError {
    #[error("HOME is not set; cannot locate the Nakode source checkout")]
    MissingHome,
    #[error(
        "the managed Nakode source checkout was not found at {0}\n\
         Reinstall Nakode with the command in README.md to create it"
    )]
    MissingSource(String),
    #[error("the Nakode installer was not found at {0}")]
    MissingInstaller(String),
    #[error("failed to start git: {0}")]
    StartGit(#[source] std::io::Error),
    #[error("git could not update the Nakode source checkout (exit status {0})")]
    GitFailed(std::process::ExitStatus),
    #[error("failed to start install.sh: {0}")]
    StartInstaller(#[source] std::io::Error),
    #[error("install.sh could not install the updated Nakode build (exit status {0})")]
    InstallerFailed(std::process::ExitStatus),
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

    println!("Updating Nakode source in {}…", source.display());
    let status = Command::new("git")
        .args(["pull", "--ff-only"])
        .current_dir(source)
        .status()
        .map_err(UpdateError::StartGit)?;
    if !status.success() {
        return Err(UpdateError::GitFailed(status));
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

#[cfg(test)]
mod tests {
    use std::path::Path;

    use tempfile::tempdir;

    use super::{UpdateError, run_from, source_directory};

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
}
