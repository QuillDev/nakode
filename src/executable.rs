//! Embedders register their self-invocation prefix before starting the runtime.
use std::{
    ffi::OsString,
    path::{Path, PathBuf},
    sync::OnceLock,
};

static EXECUTABLE: OnceLock<(PathBuf, Vec<OsString>)> = OnceLock::new();

pub(crate) fn initialize(prefix: &[OsString]) -> std::io::Result<()> {
    let executable = std::env::current_exe()?;
    if let Some((registered, arguments)) = EXECUTABLE.get() {
        if registered == &executable && arguments == prefix {
            return Ok(());
        }
        return Err(std::io::Error::other(
            "runtime executable was already configured differently",
        ));
    }
    EXECUTABLE
        .set((executable, prefix.to_vec()))
        .map_err(|_| std::io::Error::other("runtime executable initialization raced"))
}

pub(crate) fn command(executable: &Path) -> tokio::process::Command {
    let mut command = tokio::process::Command::new(executable);
    command.envs(crate::machine_path::environment());
    if let Some((registered, prefix)) = EXECUTABLE.get()
        && executable == registered
    {
        command.args(prefix);
    }
    command
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_prefix_is_replayed_only_for_the_containing_executable() {
        let prefix = [OsString::from("agent-runtime")];
        initialize(&prefix).expect("register embedding executable");
        initialize(&prefix).expect("identical registration is idempotent");
        assert!(initialize(&[OsString::from("different")]).is_err());

        let current = std::env::current_exe().expect("current executable");
        let mut own = command(&current);
        own.arg("codemode-worker").env_clear();
        assert_eq!(
            own.as_std().get_args().collect::<Vec<_>>(),
            ["agent-runtime", "codemode-worker"]
        );
        let mut external = command(Path::new("/fixture/worker"));
        external.arg("codemode-worker");
        assert_eq!(
            external.as_std().get_args().collect::<Vec<_>>(),
            ["codemode-worker"]
        );
    }
}
