use std::fmt::Write as _;

const MAX_HOST_FACT_CHARS: usize = 128;

/// Stable, server-observed facts about the machine that owns provider and tool execution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionHost {
    hostname: String,
    operating_system: String,
    architecture: String,
}

impl ExecutionHost {
    /// Reads host facts in the Nakode server process. A failure is kept explicit so a server never
    /// substitutes client metadata or a guessed machine identity.
    ///
    /// # Errors
    /// Returns the operating-system error when the server hostname cannot be read.
    pub fn detect() -> std::io::Result<Self> {
        Ok(Self::new(
            hostname::get()?.to_string_lossy(),
            std::env::consts::OS,
            std::env::consts::ARCH,
        ))
    }

    #[must_use]
    pub fn new(
        hostname: impl AsRef<str>,
        operating_system: impl AsRef<str>,
        architecture: impl AsRef<str>,
    ) -> Self {
        Self {
            hostname: one_line(hostname.as_ref()),
            operating_system: one_line(operating_system.as_ref()),
            architecture: one_line(architecture.as_ref()),
        }
    }

    #[must_use]
    pub fn prompt_context(&self) -> String {
        let mut context = String::from(
            "[Nakode Execution Host]\nThese facts come from the authoritative Nakode server process. Nakode workspace access, tools, shells, and delegated agents execute on this host; client-device prose or paths do not describe the execution host.\n",
        );
        let _ = writeln!(context, "Hostname: {}", self.hostname);
        let _ = writeln!(context, "Operating system: {}", self.operating_system);
        let _ = writeln!(context, "Architecture: {}", self.architecture);
        context.push_str("[/Nakode Execution Host]");
        context
    }
}

impl Default for ExecutionHost {
    fn default() -> Self {
        Self::new("unknown", std::env::consts::OS, std::env::consts::ARCH)
    }
}

fn one_line(value: &str) -> String {
    let normalized = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.is_empty() {
        "unknown".to_owned()
    } else {
        normalized.chars().take(MAX_HOST_FACT_CHARS).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::{ExecutionHost, MAX_HOST_FACT_CHARS};

    #[test]
    fn prompt_context_contains_only_bounded_server_observed_specs() {
        let oversized_hostname = format!("nakohoko\n{}", "x".repeat(MAX_HOST_FACT_CHARS * 2));
        let context = ExecutionHost::new(&oversized_hostname, "linux", "aarch64").prompt_context();
        let hostname = context
            .lines()
            .find_map(|line| line.strip_prefix("Hostname: "))
            .expect("host context should contain a hostname");

        assert!(hostname.starts_with("nakohoko x"));
        assert_eq!(hostname.chars().count(), MAX_HOST_FACT_CHARS);
        assert!(context.contains("Operating system: linux"));
        assert!(context.contains("Architecture: aarch64"));
        assert!(!context.contains("macOS"));
        assert!(!context.contains("client hostname"));
    }
}
