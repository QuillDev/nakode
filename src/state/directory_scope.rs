use super::{DomainCommandError, DomainState};
use crate::backend::{CODEX_PROVIDER, DEVIN_PROVIDER, GLM_PROVIDER, KIMI_PROVIDER};

/// This policy deliberately has no general process, network, memory, skill, or delegation tool.
/// Filesystem checks are application-level containment, not a hostile-process sandbox.
pub(crate) const DIRECTORY_TOOLS: &[&str] =
    &["read", "write", "edit", "ls", "find", "grep", "ask", "todo"];

impl DomainState {
    pub(crate) fn install_directory_scope(
        &mut self,
        root: Option<&str>,
        provider: &str,
    ) -> Result<(), DomainCommandError> {
        let Some(root) = root else {
            return Ok(());
        };
        if root != self.working_directory || !std::path::Path::new(root).is_absolute() {
            return Err(DomainCommandError::Invalid(
                "directory_scope must equal the canonical working directory approved by the owner"
                    .to_owned(),
            ));
        }
        self.validate_directory_provider(provider)?;
        self.directory_scope = Some(root.to_owned());
        Ok(())
    }

    pub(crate) fn validate_directory_provider(
        &self,
        provider: &str,
    ) -> Result<(), DomainCommandError> {
        if !matches!(
            provider,
            CODEX_PROVIDER | DEVIN_PROVIDER | GLM_PROVIDER | KIMI_PROVIDER
        ) || !self
            .provider_capabilities(provider)
            .is_some_and(|capabilities| capabilities.scoped_runtime_policy.is_supported())
        {
            return Err(DomainCommandError::Unsupported(format!(
                "provider {provider} cannot enforce directory-scoped structured tools; choose a portable Nakode runtime provider"
            )));
        }
        Ok(())
    }

    pub(crate) fn validate_directory_attachment(
        &self,
        root: Option<&str>,
    ) -> Result<(), DomainCommandError> {
        if root.is_some() && root != self.directory_scope.as_deref() {
            return Err(DomainCommandError::Conflict(
                "directory scope cannot change on resume; create a new session after explicit approval".to_owned(),
            ));
        }
        Ok(())
    }

    pub(crate) fn require_unrestricted_session(
        &self,
        operation: &str,
    ) -> Result<(), DomainCommandError> {
        if self.directory_scope.is_some() {
            return Err(DomainCommandError::Unsupported(format!(
                "{operation} is unavailable in directory scope; broader access requires a separately approved new session"
            )));
        }
        Ok(())
    }

    pub(crate) fn validate_directory_tools(
        &self,
        tools: &[nakode_protocol::ExternalToolDefinition],
        replacement: bool,
        code_mode: bool,
        allowed: Option<&[String]>,
    ) -> Result<(), DomainCommandError> {
        if self.directory_scope.is_none() {
            return Ok(());
        }
        if !tools.is_empty()
            || replacement
            || code_mode
            || !allowed.is_some_and(|names| {
                !names.is_empty()
                    && names
                        .iter()
                        .all(|name| DIRECTORY_TOOLS.contains(&name.as_str()))
            })
        {
            return Err(DomainCommandError::Invalid(
                "directory scope permits only an explicit structured-file-tool allowlist; external tools, processes, evaluators, skills, memory, and delegation are unavailable".to_owned(),
            ));
        }
        Ok(())
    }
}
