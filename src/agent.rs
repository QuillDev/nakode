use std::{collections::HashSet, fs, path::Path};

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const CANONICAL_AGENT_TOOLS: &[&str] = &[
    "read",
    "grep",
    "find",
    "ls",
    "bash",
    "write",
    "edit",
    "eval",
    "todo",
    "ask",
    "memory_search",
    "memory_store",
    "vision",
    "browser",
];

const CANONICAL_CAPABILITIES: &[&str] = &[
    "filesystem_read",
    "filesystem_write",
    "command_execution",
    "network",
    "memory",
    "vision",
    "interaction",
    "delegation",
];

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum AgentOwnership {
    BuiltIn,
    #[default]
    OwnerDefined,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum AgentToolProfile {
    None,
    ReadOnly,
    CommandRunner,
    BoundedWatcher,
    #[default]
    Custom,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum AgentFallbackPolicy {
    Prohibited,
    #[default]
    ConfiguredOnly,
}

fn enabled_by_default() -> bool {
    true
}

fn attributed_by_default() -> bool {
    true
}

fn default_concurrency() -> u32 {
    4
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(default, deny_unknown_fields)]
// Persisted archetype policy keeps these independent flags for backward-compatible definition files.
#[allow(clippy::struct_excessive_bools)]
pub struct AgentDefinition {
    pub slug: String,
    pub description: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub system_prompt: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub first_message: String,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub fallback_models: Vec<String>,
    #[serde(default)]
    pub fast_mode: bool,
    /// How hard this archetype thinks, or `None` for whatever its model does by default.
    ///
    /// **A level belongs to the model**, so it is only meaningful beside `model` and is refused
    /// without one — "default" means the default model at its own default level. `serde(default)`
    /// is what makes every definition already on disk mean exactly that: no level written, no level
    /// applied, and the run gets the model's own.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<String>,
    /// Whether the definition is shipped as immutable policy or belongs to the local owner.
    #[serde(default, skip_serializing_if = "is_owner_defined")]
    pub ownership: AgentOwnership,
    /// Disabled definitions remain inspectable but cannot be delegated.
    #[serde(default = "enabled_by_default", skip_serializing_if = "is_enabled")]
    pub enabled: bool,
    /// Provider-neutral capability and tool policy. Names are canonical Nakode runtime names.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_capabilities: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub denied_capabilities: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_tools: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub denied_tools: Vec<String>,
    #[serde(default)]
    pub tool_profile: AgentToolProfile,
    /// Human-readable task and machine-readable result expectations.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub task_shape: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub output_contract: String,
    /// Bounded execution policy. Zero/None means the runtime default where documented.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub poll_interval_ms: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_turns: Option<u32>,
    #[serde(
        default = "default_concurrency",
        skip_serializing_if = "is_default_concurrency"
    )]
    pub max_concurrency: u32,
    #[serde(default)]
    pub fallback_policy: AgentFallbackPolicy,
    /// Recursive Nakode delegation is denied unless both fields explicitly allow a bounded depth.
    #[serde(default)]
    pub can_delegate: bool,
    #[serde(default)]
    pub max_delegation_depth: u32,
    #[serde(
        default = "attributed_by_default",
        skip_serializing_if = "is_attributed"
    )]
    pub require_parent_attribution: bool,
}

// Serde's `skip_serializing_if` callback contract passes the field by reference, including Copy
// fields, so these predicates cannot take their arguments by value.
#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_owner_defined(value: &AgentOwnership) -> bool {
    *value == AgentOwnership::OwnerDefined
}

#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_enabled(value: &bool) -> bool {
    *value
}

#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_attributed(value: &bool) -> bool {
    *value
}

#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_default_concurrency(value: &u32) -> bool {
    *value == default_concurrency()
}

impl Default for AgentDefinition {
    fn default() -> Self {
        Self {
            slug: String::new(),
            description: String::new(),
            system_prompt: String::new(),
            first_message: String::new(),
            model: None,
            fallback_models: Vec::new(),
            fast_mode: false,
            reasoning_effort: None,
            ownership: AgentOwnership::OwnerDefined,
            enabled: true,
            allowed_capabilities: Vec::new(),
            denied_capabilities: Vec::new(),
            allowed_tools: Vec::new(),
            denied_tools: Vec::new(),
            tool_profile: AgentToolProfile::Custom,
            task_shape: String::new(),
            output_contract: String::new(),
            timeout_seconds: None,
            poll_interval_ms: None,
            max_turns: None,
            max_concurrency: default_concurrency(),
            fallback_policy: AgentFallbackPolicy::ConfiguredOnly,
            can_delegate: false,
            max_delegation_depth: 0,
            require_parent_attribution: true,
        }
    }
}

impl AgentDefinition {
    #[must_use]
    pub fn provider<'a>(&'a self, parent_provider: &'a str) -> &'a str {
        self.model
            .as_deref()
            .and_then(|model| model.split_once('/'))
            .map_or(parent_provider, |(provider, _)| provider)
    }

    #[must_use]
    pub fn provider_model(&self) -> Option<String> {
        self.model
            .as_deref()
            .and_then(|model| model.split_once('/'))
            .map(|(_, model)| model.to_owned())
    }

    #[must_use]
    pub fn instructions(&self) -> &str {
        let system_prompt = self.system_prompt.trim();
        if system_prompt.is_empty() {
            self.description.trim()
        } else {
            system_prompt
        }
    }

    /// The exact built-ins a delegated run receives, or `None` when a legacy custom definition
    /// intentionally retains the provider runtime's defaults.
    ///
    /// Capabilities are structural enforcement, not labels: a configured tool that requires an
    /// absent capability is removed from the runtime allowlist even for legacy deny-only policy.
    #[must_use]
    pub fn builtin_tool_allowlist(&self) -> Option<Vec<String>> {
        let configured =
            if self.tool_profile == AgentToolProfile::Custom && self.allowed_tools.is_empty() {
                if self.denied_tools.is_empty() {
                    return None;
                }
                CANONICAL_AGENT_TOOLS
                    .iter()
                    .filter(|tool| !self.denied_tools.iter().any(|denied| denied == *tool))
                    .map(|tool| (*tool).to_owned())
                    .collect()
            } else {
                self.allowed_tools.clone()
            };
        Some(
            configured
                .into_iter()
                .filter(|tool| {
                    required_capability(tool).is_none_or(|required| {
                        self.allowed_capabilities
                            .iter()
                            .any(|capability| capability == required)
                            && !self
                                .denied_capabilities
                                .iter()
                                .any(|capability| capability == required)
                    })
                })
                .collect(),
        )
    }

    /// The exact declared capability boundary paired with the interpreted built-in set.
    /// `None` is reserved for the same legacy runtime-default case as the tool projection.
    #[must_use]
    pub fn effective_capabilities(&self) -> Option<Vec<String>> {
        self.builtin_tool_allowlist()
            .map(|_| self.allowed_capabilities.clone())
    }

    /// Nakode-owned findings that explain security-relevant consequences of the interpreted policy.
    #[must_use]
    pub fn policy_warnings(&self) -> Vec<String> {
        let mut warnings = Vec::new();
        let effective = self.builtin_tool_allowlist();
        if effective.is_none() {
            warnings.push(
                "Legacy custom policy has no allow/deny boundary; the provider runtime's default built-ins remain available and the capability boundary is ambiguous."
                    .to_owned(),
            );
        }
        if self.tool_profile == AgentToolProfile::Custom
            && self.allowed_tools.is_empty()
            && !self.denied_tools.is_empty()
        {
            warnings.push(
                "Legacy deny-only policy is reconciled against canonical tool capabilities; tools missing their required capability are not runnable."
                    .to_owned(),
            );
        }
        if let Some(tools) = &effective {
            if tools.iter().any(|tool| tool == "bash") {
                warnings.push(
                    "bash grants general command execution. System-prompt instructions such as read-only Git usage are behavioral guidance, not subcommand-level enforcement."
                        .to_owned(),
                );
            }
            if tools.iter().any(|tool| tool == "write" || tool == "edit") {
                warnings.push("Direct filesystem mutation tools are runnable.".to_owned());
            }
            if tools.iter().any(|tool| tool == "eval") {
                warnings.push(
                    "eval can execute code through an available language runtime.".to_owned(),
                );
            }
            if tools.iter().any(|tool| tool == "browser") {
                warnings.push(
                    "browser can reach network resources through the configured browser runtime."
                        .to_owned(),
                );
            }
        }
        if self.can_delegate {
            warnings.push(format!(
                "Recursive delegation is enabled to depth {}; every child remains parent-attributed.",
                self.max_delegation_depth
            ));
        }
        warnings
    }

    /// Whether this definition needs provider-side enforcement beyond ordinary unrestricted turns.
    #[must_use]
    pub fn requires_scoped_runtime_policy(&self) -> bool {
        self.builtin_tool_allowlist().is_some()
            || self.max_turns.is_some()
            || self.timeout_seconds.is_some()
    }

    #[must_use]
    pub fn initial_prompt(&self, task: &str) -> String {
        let first_message = self.first_message.trim();
        if first_message.is_empty() {
            format!("# Delegated task\n\n{}", task.trim())
        } else {
            format!("{first_message}\n\n# Delegated task\n\n{}", task.trim())
        }
    }
}

#[derive(Debug, Error)]
pub enum AgentCatalogError {
    #[error("failed to read agent directory {path}: {source}")]
    ReadDirectory {
        path: String,
        source: std::io::Error,
    },
    #[error("failed to read agent definition {path}: {source}")]
    ReadDefinition {
        path: String,
        source: std::io::Error,
    },
    #[error("failed to create agent directory {path}: {source}")]
    CreateDirectory {
        path: String,
        source: std::io::Error,
    },
    #[error("failed to write agent definition {path}: {source}")]
    WriteDefinition {
        path: String,
        source: std::io::Error,
    },
    #[error("failed to remove agent definition {path}: {source}")]
    RemoveDefinition {
        path: String,
        source: std::io::Error,
    },
    #[error("failed to serialize agent {slug:?}: {source}")]
    SerializeDefinition {
        slug: String,
        source: toml::ser::Error,
    },
    #[error("invalid agent definition {path}: {source}")]
    ParseDefinition {
        path: String,
        source: toml::de::Error,
    },
    #[error("agent slug {slug:?} in {path} must contain lowercase letters, digits, or hyphens")]
    InvalidSlug { path: String, slug: String },
    #[error("agent definition {path} has an empty {field}")]
    EmptyField { path: String, field: &'static str },
    #[error("agent {slug:?} is defined more than once")]
    DuplicateSlug { slug: String },
    #[error("agent {slug:?} model must use provider/model form: {model}")]
    InvalidModel { slug: String, model: String },
    #[error("agent {slug:?} sets reasoning_effort {effort:?} without a model to run it at")]
    EffortWithoutModel { slug: String, effort: String },
    #[error("agent {slug:?} has contradictory policy: {detail}")]
    ContradictoryPolicy { slug: String, detail: String },
    #[error(
        "built-in agent {slug:?} is immutable; create an owner-defined archetype with a different slug"
    )]
    ImmutableBuiltIn { slug: String },
}

#[derive(Clone, Debug, Default)]
pub struct AgentCatalog {
    definitions: Vec<AgentDefinition>,
}

impl AgentCatalog {
    #[must_use]
    #[cfg(test)]
    pub(crate) fn from_definitions(definitions: Vec<AgentDefinition>) -> Self {
        Self { definitions }
    }

    /// Validates an agent definition before it is persisted.
    ///
    /// # Errors
    /// Returns the same field and model validation errors used by catalog loading and saving.
    pub fn validate_definition(definition: &AgentDefinition) -> Result<(), AgentCatalogError> {
        validate(definition, "agent editor")
    }

    /// Loads all TOML agent definitions from `directory` in filename order.
    ///
    /// A missing or empty directory produces an empty catalog.
    ///
    /// # Errors
    /// Returns an error when a definition cannot be read or validated.
    pub fn load(directory: &Path) -> Result<Self, AgentCatalogError> {
        if !directory.exists() {
            return Ok(Self::default());
        }
        let entries =
            fs::read_dir(directory).map_err(|source| AgentCatalogError::ReadDirectory {
                path: directory.display().to_string(),
                source,
            })?;
        let mut paths = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|source| AgentCatalogError::ReadDirectory {
                path: directory.display().to_string(),
                source,
            })?;
            let path = entry.path();
            if path
                .extension()
                .is_some_and(|extension| extension == "toml")
            {
                paths.push(path);
            }
        }
        paths.sort();

        let mut definitions = Vec::with_capacity(paths.len());
        let mut slugs = HashSet::new();
        for path in paths {
            let display_path = path.display().to_string();
            let source =
                fs::read_to_string(&path).map_err(|source| AgentCatalogError::ReadDefinition {
                    path: display_path.clone(),
                    source,
                })?;
            let definition = toml::from_str::<AgentDefinition>(&source).map_err(|source| {
                AgentCatalogError::ParseDefinition {
                    path: display_path.clone(),
                    source,
                }
            })?;
            validate(&definition, &display_path)?;
            if !slugs.insert(definition.slug.clone()) {
                return Err(AgentCatalogError::DuplicateSlug {
                    slug: definition.slug,
                });
            }
            definitions.push(definition);
        }
        Ok(Self { definitions })
    }

    #[must_use]
    pub fn definitions(&self) -> &[AgentDefinition] {
        &self.definitions
    }

    #[must_use]
    pub fn find(&self, slug: &str) -> Option<&AgentDefinition> {
        self.definitions
            .iter()
            .find(|definition| definition.slug == slug)
    }

    /// Persists `definition` into an authoritative workspace catalog.
    ///
    /// Creates the workspace catalog on the first saved definition.
    ///
    /// # Errors
    /// Returns an error when validation, serialization, or filesystem access fails.
    pub fn save(
        &self,
        directory: &Path,
        definition: &AgentDefinition,
        previous_slug: Option<&str>,
    ) -> Result<(), AgentCatalogError> {
        validate(definition, &directory.display().to_string())?;
        if let Some(previous) = previous_slug.and_then(|slug| self.find(slug))
            && previous.ownership == AgentOwnership::BuiltIn
        {
            return Err(AgentCatalogError::ImmutableBuiltIn {
                slug: previous.slug.clone(),
            });
        }
        if previous_slug.is_none() && definition.ownership == AgentOwnership::BuiltIn {
            return Err(AgentCatalogError::ImmutableBuiltIn {
                slug: definition.slug.clone(),
            });
        }
        if self.definitions.iter().any(|existing| {
            existing.slug == definition.slug
                && previous_slug.is_none_or(|previous| previous != existing.slug)
        }) {
            return Err(AgentCatalogError::DuplicateSlug {
                slug: definition.slug.clone(),
            });
        }
        self.materialize_if_missing(directory)?;
        if let Some(previous_slug) = previous_slug.filter(|slug| *slug != definition.slug) {
            let previous_path = definition_path(directory, previous_slug);
            let backup = directory.join(format!(".{previous_slug}.toml.rename-backup"));
            fs::rename(&previous_path, &backup).map_err(|source| {
                AgentCatalogError::RemoveDefinition {
                    path: previous_path.display().to_string(),
                    source,
                }
            })?;
            if let Err(error) = write_definition(directory, definition) {
                let _ = fs::rename(&backup, &previous_path);
                return Err(error);
            }
            if let Err(source) = fs::remove_file(&backup) {
                let _ = fs::remove_file(definition_path(directory, &definition.slug));
                let _ = fs::rename(&backup, &previous_path);
                return Err(AgentCatalogError::RemoveDefinition {
                    path: backup.display().to_string(),
                    source,
                });
            }
            return Ok(());
        }
        write_definition(directory, definition)
    }

    /// Removes an archetype from the authoritative workspace catalog.
    ///
    /// # Errors
    /// Returns an error when the catalog cannot be materialized or the file removed.
    pub fn delete(&self, directory: &Path, slug: &str) -> Result<(), AgentCatalogError> {
        if let Some(definition) = self.find(slug)
            && definition.ownership == AgentOwnership::BuiltIn
        {
            return Err(AgentCatalogError::ImmutableBuiltIn {
                slug: slug.to_owned(),
            });
        }
        self.materialize_if_missing(directory)?;
        remove_if_present(&definition_path(directory, slug))
    }

    fn materialize_if_missing(&self, directory: &Path) -> Result<(), AgentCatalogError> {
        if directory.exists() {
            return Ok(());
        }
        fs::create_dir_all(directory).map_err(|source| AgentCatalogError::CreateDirectory {
            path: directory.display().to_string(),
            source,
        })?;
        for definition in &self.definitions {
            write_definition(directory, definition)?;
        }
        Ok(())
    }
}

fn definition_path(directory: &Path, slug: &str) -> std::path::PathBuf {
    directory.join(format!("{slug}.toml"))
}

fn write_definition(
    directory: &Path,
    definition: &AgentDefinition,
) -> Result<(), AgentCatalogError> {
    let source = toml::to_string_pretty(definition).map_err(|source| {
        AgentCatalogError::SerializeDefinition {
            slug: definition.slug.clone(),
            source,
        }
    })?;
    let path = definition_path(directory, &definition.slug);
    let pending = directory.join(format!(".{}.toml.pending", definition.slug));
    fs::write(&pending, source).map_err(|source| AgentCatalogError::WriteDefinition {
        path: pending.display().to_string(),
        source,
    })?;
    fs::rename(&pending, &path).map_err(|source| AgentCatalogError::WriteDefinition {
        path: path.display().to_string(),
        source,
    })
}

fn remove_if_present(path: &Path) -> Result<(), AgentCatalogError> {
    if !path.exists() {
        return Ok(());
    }
    fs::remove_file(path).map_err(|source| AgentCatalogError::RemoveDefinition {
        path: path.display().to_string(),
        source,
    })
}

/// The capability that must be present for a canonical built-in to survive into a run.
#[must_use]
pub fn required_capability(tool: &str) -> Option<&'static str> {
    match tool {
        "read" | "grep" | "find" | "ls" => Some("filesystem_read"),
        "write" | "edit" => Some("filesystem_write"),
        "bash" => Some("command_execution"),
        "eval" | "ask" => Some("interaction"),
        "browser" => Some("network"),
        "memory_store" | "memory_search" => Some("memory"),
        "vision" => Some("vision"),
        _ => None,
    }
}

#[allow(clippy::too_many_lines)]
// Validation deliberately reports one policy-specific error at a time from this persisted shape;
// splitting the ordered checks would obscure the single authoritative validation boundary.
fn validate(definition: &AgentDefinition, path: &str) -> Result<(), AgentCatalogError> {
    if definition.slug.is_empty()
        || !definition.slug.chars().all(|character| {
            character.is_ascii_lowercase() || character.is_ascii_digit() || character == '-'
        })
    {
        return Err(AgentCatalogError::InvalidSlug {
            path: path.to_owned(),
            slug: definition.slug.clone(),
        });
    }
    if definition.description.trim().is_empty() {
        return Err(AgentCatalogError::EmptyField {
            path: path.to_owned(),
            field: "description",
        });
    }
    // A level with nothing to apply it to. The provider is asked for the parent session's model in
    // that case, and a level chosen against a model nobody named is a level for a different model.
    if let Some(effort) = definition.reasoning_effort.as_deref()
        && definition.model.is_none()
    {
        return Err(AgentCatalogError::EffortWithoutModel {
            slug: definition.slug.clone(),
            effort: effort.to_owned(),
        });
    }
    if definition.max_concurrency == 0 || definition.max_concurrency > 16 {
        return Err(AgentCatalogError::ContradictoryPolicy {
            slug: definition.slug.clone(),
            detail: "max_concurrency must be between 1 and 16".to_owned(),
        });
    }
    if definition
        .timeout_seconds
        .is_some_and(|value| value == 0 || value > 86_400)
    {
        return Err(AgentCatalogError::ContradictoryPolicy {
            slug: definition.slug.clone(),
            detail: "timeout_seconds must be between 1 and 86400".to_owned(),
        });
    }
    if definition
        .poll_interval_ms
        .is_some_and(|value| !(100..=3_600_000).contains(&value))
    {
        return Err(AgentCatalogError::ContradictoryPolicy {
            slug: definition.slug.clone(),
            detail: "poll_interval_ms must be between 100 and 3600000".to_owned(),
        });
    }
    if definition
        .max_turns
        .is_some_and(|value| value == 0 || value > 100)
    {
        return Err(AgentCatalogError::ContradictoryPolicy {
            slug: definition.slug.clone(),
            detail: "max_turns must be between 1 and 100".to_owned(),
        });
    }
    if definition.max_delegation_depth > 4 {
        return Err(AgentCatalogError::ContradictoryPolicy {
            slug: definition.slug.clone(),
            detail: "max_delegation_depth cannot exceed 4".to_owned(),
        });
    }
    if definition.tool_profile == AgentToolProfile::BoundedWatcher
        && (definition.poll_interval_ms.is_none() || definition.timeout_seconds.is_none())
    {
        return Err(AgentCatalogError::ContradictoryPolicy {
            slug: definition.slug.clone(),
            detail: "bounded_watcher requires timeout_seconds and poll_interval_ms".to_owned(),
        });
    }
    if definition.can_delegate != (definition.max_delegation_depth > 0) {
        return Err(AgentCatalogError::ContradictoryPolicy {
            slug: definition.slug.clone(),
            detail:
                "can_delegate and max_delegation_depth must enable or disable recursion together"
                    .to_owned(),
        });
    }
    if !definition.require_parent_attribution {
        return Err(AgentCatalogError::ContradictoryPolicy {
            slug: definition.slug.clone(),
            detail: "parent attribution is required for every supervised archetype".to_owned(),
        });
    }
    if definition.fallback_policy == AgentFallbackPolicy::Prohibited
        && !definition.fallback_models.is_empty()
    {
        return Err(AgentCatalogError::ContradictoryPolicy {
            slug: definition.slug.clone(),
            detail: "fallback_models must be empty when fallback_policy is prohibited".to_owned(),
        });
    }
    for name in &definition.allowed_tools {
        if definition.denied_tools.contains(name) {
            return Err(AgentCatalogError::ContradictoryPolicy {
                slug: definition.slug.clone(),
                detail: format!("tool {name:?} is both allowed and denied"),
            });
        }
    }
    for name in &definition.allowed_capabilities {
        if definition.denied_capabilities.contains(name) {
            return Err(AgentCatalogError::ContradictoryPolicy {
                slug: definition.slug.clone(),
                detail: format!("capability {name:?} is both allowed and denied"),
            });
        }
    }
    let mut all_names = definition
        .allowed_tools
        .iter()
        .chain(&definition.denied_tools)
        .chain(&definition.allowed_capabilities)
        .chain(&definition.denied_capabilities);
    if all_names.any(|name| {
        name.is_empty()
            || !name.chars().all(|character| {
                character.is_ascii_lowercase()
                    || character.is_ascii_digit()
                    || character == '-'
                    || character == '_'
            })
    }) {
        return Err(AgentCatalogError::ContradictoryPolicy {
            slug: definition.slug.clone(),
            detail:
                "capability and tool names use lowercase letters, digits, hyphens or underscores"
                    .to_owned(),
        });
    }
    if let Some(name) = definition
        .allowed_tools
        .iter()
        .chain(&definition.denied_tools)
        .find(|name| !CANONICAL_AGENT_TOOLS.contains(&name.as_str()))
    {
        return Err(AgentCatalogError::ContradictoryPolicy {
            slug: definition.slug.clone(),
            detail: format!("unknown canonical tool {name:?}"),
        });
    }
    if let Some(name) = definition
        .allowed_capabilities
        .iter()
        .chain(&definition.denied_capabilities)
        .find(|name| !CANONICAL_CAPABILITIES.contains(&name.as_str()))
    {
        return Err(AgentCatalogError::ContradictoryPolicy {
            slug: definition.slug.clone(),
            detail: format!("unknown canonical capability {name:?}"),
        });
    }
    let profile_tools: &[&str] = match definition.tool_profile {
        AgentToolProfile::None => &[],
        AgentToolProfile::ReadOnly => &[
            "read",
            "grep",
            "find",
            "ls",
            "todo",
            "ask",
            "memory_search",
            "vision",
        ],
        AgentToolProfile::CommandRunner => &["read", "grep", "find", "ls", "bash", "todo", "ask"],
        AgentToolProfile::BoundedWatcher => &["read", "grep", "find", "ls", "todo", "ask"],
        AgentToolProfile::Custom => CANONICAL_AGENT_TOOLS,
    };
    if let Some(name) = definition
        .allowed_tools
        .iter()
        .find(|name| !profile_tools.contains(&name.as_str()))
    {
        return Err(AgentCatalogError::ContradictoryPolicy {
            slug: definition.slug.clone(),
            detail: format!("tool {name:?} is not permitted by the selected tool_profile"),
        });
    }
    if definition.tool_profile == AgentToolProfile::None && !definition.allowed_tools.is_empty() {
        return Err(AgentCatalogError::ContradictoryPolicy {
            slug: definition.slug.clone(),
            detail: "the none tool profile cannot allow tools".to_owned(),
        });
    }
    if definition.tool_profile != AgentToolProfile::Custom
        && definition
            .allowed_capabilities
            .iter()
            .any(|name| name == "network" || name == "filesystem_write")
    {
        return Err(AgentCatalogError::ContradictoryPolicy {
            slug: definition.slug.clone(),
            detail: "restrictive tool profiles cannot allow network or filesystem_write".to_owned(),
        });
    }
    for tool in &definition.allowed_tools {
        if let Some(capability) = required_capability(tool)
            && !definition
                .allowed_capabilities
                .iter()
                .any(|name| name == capability)
        {
            return Err(AgentCatalogError::ContradictoryPolicy {
                slug: definition.slug.clone(),
                detail: format!("tool {tool:?} requires capability {capability:?}"),
            });
        }
    }
    let write_tools = ["write", "edit"];
    if matches!(
        definition.tool_profile,
        AgentToolProfile::ReadOnly | AgentToolProfile::BoundedWatcher
    ) && definition
        .allowed_tools
        .iter()
        .any(|tool| write_tools.contains(&tool.as_str()) || tool == "bash")
    {
        return Err(AgentCatalogError::ContradictoryPolicy {
            slug: definition.slug.clone(),
            detail: "read-only and bounded-watcher profiles cannot allow write, edit, or bash"
                .to_owned(),
        });
    }
    if definition.tool_profile == AgentToolProfile::BoundedWatcher
        && (definition.poll_interval_ms.is_none() || definition.timeout_seconds.is_none())
    {
        return Err(AgentCatalogError::ContradictoryPolicy {
            slug: definition.slug.clone(),
            detail: "bounded watchers require poll_interval_ms and timeout_seconds".to_owned(),
        });
    }
    if definition.tool_profile == AgentToolProfile::CommandRunner
        && !definition.allowed_tools.iter().any(|tool| tool == "bash")
    {
        return Err(AgentCatalogError::ContradictoryPolicy {
            slug: definition.slug.clone(),
            detail: "command runners must explicitly allow bash".to_owned(),
        });
    }
    for model in definition.model.iter().chain(&definition.fallback_models) {
        if model
            .split_once('/')
            .is_none_or(|(provider, model)| provider.is_empty() || model.is_empty())
        {
            return Err(AgentCatalogError::InvalidModel {
                slug: definition.slug.clone(),
                model: model.clone(),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::{
        AgentCatalog, AgentCatalogError, AgentDefinition, AgentOwnership, AgentToolProfile,
    };

    #[test]
    fn loads_and_resolves_agent_definitions() {
        let directory = tempdir().expect("temp directory");
        fs::write(
            directory.path().join("explorer.toml"),
            r#"
slug = "explorer"
description = "Explores patches"
system_prompt = "Explore carefully."
first_message = "Inspect the requested context."
model = "openai-codex/gpt-5"
"#,
        )
        .expect("agent fixture");

        let catalog = AgentCatalog::load(directory.path()).expect("valid catalog");
        let agent = catalog.find("explorer").expect("explorer");
        assert_eq!(agent.provider("devin-acp"), "openai-codex");
        assert!(agent.initial_prompt("Check auth").contains("Check auth"));
    }

    /// Every definition already on disk was written before `reasoning_effort` existed, and has to go
    /// on meaning what it meant: run the model at its own default level.
    #[test]
    fn a_definition_written_without_an_effort_means_the_models_default() {
        let directory = tempdir().expect("temp directory");
        fs::write(
            directory.path().join("explorer.toml"),
            r#"
slug = "explorer"
description = "Explores patches"
model = "openai-codex/gpt-5"
"#,
        )
        .expect("agent fixture");

        let catalog = AgentCatalog::load(directory.path()).expect("valid catalog");
        let agent = catalog.find("explorer").expect("explorer");
        assert_eq!(agent.reasoning_effort, None);
    }

    #[test]
    fn a_definition_reads_back_the_effort_it_was_written_with() {
        let directory = tempdir().expect("temp directory");
        fs::write(
            directory.path().join("explorer.toml"),
            r#"
slug = "explorer"
description = "Explores patches"
model = "openai-codex/gpt-5"
reasoning_effort = "high"
"#,
        )
        .expect("agent fixture");

        let catalog = AgentCatalog::load(directory.path()).expect("valid catalog");
        assert_eq!(
            catalog.find("explorer").expect("explorer").reasoning_effort,
            Some("high".to_owned())
        );
    }

    /// A level belongs to a model, so one without a model is refused rather than quietly applied to
    /// whatever the parent session happens to be running.
    #[test]
    fn an_effort_without_a_model_is_refused() {
        let directory = tempdir().expect("temp directory");
        fs::write(
            directory.path().join("explorer.toml"),
            r#"
slug = "explorer"
description = "Explores patches"
reasoning_effort = "high"
"#,
        )
        .expect("agent fixture");

        let error = AgentCatalog::load(directory.path()).expect_err("effort with no model");
        assert!(
            error.to_string().contains("without a model"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn rejects_duplicate_slugs() {
        let directory = tempdir().expect("temp directory");
        let definition = r#"
slug = "explorer"
description = "Explores patches"
system_prompt = "Explore carefully."
first_message = "Inspect the requested context."
"#;
        fs::write(directory.path().join("one.toml"), definition).expect("first fixture");
        fs::write(directory.path().join("two.toml"), definition).expect("second fixture");

        assert!(AgentCatalog::load(directory.path()).is_err());
    }

    #[test]
    fn missing_directory_has_no_agents() {
        let directory = tempdir().expect("temp directory");
        let catalog = AgentCatalog::load(&directory.path().join("missing")).expect("catalog");

        assert!(catalog.definitions().is_empty());
        assert!(catalog.find("explorer").is_none());
    }

    #[test]
    fn workspace_definitions_are_loaded_without_presets() {
        let directory = tempdir().expect("temp directory");
        fs::write(
            directory.path().join("explorer.toml"),
            r#"
slug = "explorer"
description = "Explores database migrations"
system_prompt = "Inspect database migrations only."
first_message = "Explore the migration."
"#,
        )
        .expect("agent fixture");

        let catalog = AgentCatalog::load(directory.path()).expect("catalog");
        let explorer = catalog.find("explorer").expect("explorer");
        assert_eq!(explorer.description, "Explores database migrations");
        assert_eq!(
            catalog
                .definitions()
                .iter()
                .filter(|agent| agent.slug == "explorer")
                .count(),
            1
        );
    }

    #[test]
    fn deleting_from_an_empty_catalog_keeps_it_empty() {
        let parent = tempdir().expect("temp directory");
        let directory = parent.path().join("agents");
        let catalog = AgentCatalog::load(&directory).expect("empty catalog");

        catalog
            .delete(&directory, "explorer")
            .expect("delete explorer");

        assert!(
            AgentCatalog::load(&directory)
                .expect("configured catalog")
                .definitions()
                .is_empty()
        );
    }

    #[test]
    fn description_only_agent_uses_simple_delegation_defaults() {
        let directory = tempdir().expect("temp directory");
        fs::write(
            directory.path().join("researcher.toml"),
            r#"
slug = "researcher"
description = "Research the requested topic and report concrete findings"
"#,
        )
        .expect("agent fixture");

        let catalog = AgentCatalog::load(directory.path()).expect("description-only catalog");
        let agent = catalog.find("researcher").expect("researcher");

        assert!(agent.system_prompt.is_empty());
        assert!(agent.first_message.is_empty());
        assert_eq!(
            agent.instructions(),
            "Research the requested topic and report concrete findings"
        );
        assert_eq!(
            agent.initial_prompt("Inspect authentication"),
            "# Delegated task\n\nInspect authentication"
        );
    }

    #[test]
    fn restrictive_policy_rejects_unknown_or_capabilityless_tools() {
        let mut definition = AgentDefinition {
            slug: "reader".to_owned(),
            description: "Reads bounded files".to_owned(),
            tool_profile: AgentToolProfile::ReadOnly,
            allowed_tools: vec!["read".to_owned()],
            ..AgentDefinition::default()
        };
        let missing_capability = AgentCatalog::validate_definition(&definition)
            .expect_err("read requires filesystem_read");
        assert!(missing_capability.to_string().contains("filesystem_read"));

        definition.allowed_capabilities = vec!["filesystem_read".to_owned()];
        AgentCatalog::validate_definition(&definition).expect("read-only policy");
        assert_eq!(
            definition.builtin_tool_allowlist(),
            Some(vec!["read".to_owned()])
        );

        definition.allowed_tools = vec!["browser".to_owned()];
        let network = AgentCatalog::validate_definition(&definition)
            .expect_err("read-only cannot silently gain network");
        assert!(network.to_string().contains("tool_profile"));
    }

    #[test]
    fn custom_legacy_and_deny_only_policies_compute_native_allowlists() {
        let mut definition = AgentDefinition::default();
        assert_eq!(definition.builtin_tool_allowlist(), None);
        definition.denied_tools = vec!["bash".to_owned(), "write".to_owned(), "edit".to_owned()];
        let allowed = definition
            .builtin_tool_allowlist()
            .expect("deny-only custom policy is bounded");
        assert!(
            !allowed
                .iter()
                .any(|tool| tool == "bash" || tool == "write" || tool == "edit")
        );
        assert_eq!(allowed, vec!["todo".to_owned()]);
        definition.allowed_capabilities = vec!["filesystem_read".to_owned()];
        let allowed = definition
            .builtin_tool_allowlist()
            .expect("capability-filtered deny-only policy is bounded");
        assert!(allowed.iter().any(|tool| tool == "read"));
        assert_eq!(
            definition.effective_capabilities(),
            Some(vec!["filesystem_read".to_owned()])
        );
    }

    #[test]
    fn named_profiles_project_exact_capability_filtered_boundaries_and_warnings() {
        let no_tools = AgentDefinition {
            tool_profile: AgentToolProfile::None,
            ..AgentDefinition::default()
        };
        assert_eq!(no_tools.builtin_tool_allowlist(), Some(Vec::new()));
        assert_eq!(no_tools.effective_capabilities(), Some(Vec::new()));
        assert!(no_tools.policy_warnings().is_empty());

        let read_only = AgentDefinition {
            tool_profile: AgentToolProfile::ReadOnly,
            allowed_capabilities: vec!["filesystem_read".to_owned()],
            allowed_tools: vec!["read".to_owned(), "grep".to_owned()],
            ..AgentDefinition::default()
        };
        assert_eq!(
            read_only.builtin_tool_allowlist(),
            Some(vec!["read".to_owned(), "grep".to_owned()])
        );
        assert_eq!(
            read_only.effective_capabilities(),
            Some(vec!["filesystem_read".to_owned()])
        );

        let command_runner = AgentDefinition {
            tool_profile: AgentToolProfile::CommandRunner,
            allowed_capabilities: vec![
                "filesystem_read".to_owned(),
                "command_execution".to_owned(),
            ],
            allowed_tools: vec!["read".to_owned(), "bash".to_owned()],
            ..AgentDefinition::default()
        };
        assert_eq!(
            command_runner.builtin_tool_allowlist(),
            Some(vec!["read".to_owned(), "bash".to_owned()])
        );
        assert!(
            command_runner
                .policy_warnings()
                .iter()
                .any(|warning| warning.contains("general command execution"))
        );

        let watcher = AgentDefinition {
            tool_profile: AgentToolProfile::BoundedWatcher,
            allowed_capabilities: vec!["filesystem_read".to_owned()],
            allowed_tools: vec!["read".to_owned()],
            timeout_seconds: Some(30),
            poll_interval_ms: Some(500),
            ..AgentDefinition::default()
        };
        assert_eq!(
            watcher.builtin_tool_allowlist(),
            Some(vec!["read".to_owned()])
        );
        assert_eq!(
            watcher.effective_capabilities(),
            Some(vec!["filesystem_read".to_owned()])
        );
    }

    #[test]
    fn built_in_definitions_cannot_be_updated_or_deleted() {
        let directory = tempdir().expect("agent directory");
        let built_in = AgentDefinition {
            slug: "shipped".to_owned(),
            description: "Shipped policy".to_owned(),
            ownership: AgentOwnership::BuiltIn,
            ..AgentDefinition::default()
        };
        let catalog = AgentCatalog::from_definitions(vec![built_in.clone()]);
        let mut replacement = built_in.clone();
        replacement.description = "Changed".to_owned();
        assert!(matches!(
            catalog.save(directory.path(), &replacement, Some("shipped")),
            Err(AgentCatalogError::ImmutableBuiltIn { .. })
        ));
        assert!(matches!(
            catalog.delete(directory.path(), "shipped"),
            Err(AgentCatalogError::ImmutableBuiltIn { .. })
        ));
    }

    #[test]
    fn disposable_archetype_smoke_flows_through_authoritative_delete() {
        let directory = tempdir().expect("disposable agent directory");
        let empty = AgentCatalog::load(directory.path()).expect("empty catalogue");
        let mut disposable = AgentDefinition {
            slug: "dashboard-archetype-smoke".to_owned(),
            description: "Disposable dashboard verification".to_owned(),
            system_prompt: "Inspect only.".to_owned(),
            first_message: "Starting verification.".to_owned(),
            tool_profile: AgentToolProfile::ReadOnly,
            allowed_capabilities: vec!["filesystem_read".to_owned()],
            allowed_tools: vec!["read".to_owned()],
            timeout_seconds: Some(60),
            max_turns: Some(2),
            max_concurrency: 1,
            ..AgentDefinition::default()
        };
        empty
            .save(directory.path(), &disposable, None)
            .expect("create disposable");

        let created = AgentCatalog::load(directory.path()).expect("inspect created disposable");
        assert_eq!(created.find(&disposable.slug), Some(&disposable));

        disposable.description = "Updated disposable dashboard verification".to_owned();
        disposable.enabled = false;
        created
            .save(directory.path(), &disposable, Some(&disposable.slug))
            .expect("atomic update and disable");
        let disabled = AgentCatalog::load(directory.path()).expect("inspect disabled disposable");
        assert_eq!(disabled.find(&disposable.slug), Some(&disposable));

        disabled
            .delete(directory.path(), &disposable.slug)
            .expect("confirmed destructive delete path");
        let deleted = AgentCatalog::load(directory.path()).expect("catalogue after delete");
        assert!(deleted.find(&disposable.slug).is_none());
        assert!(
            fs::read_dir(directory.path())
                .expect("agent directory")
                .next()
                .is_none(),
            "the disposable definition must leave no file behind"
        );
    }

    #[test]
    fn saves_the_first_custom_agent() {
        let parent = tempdir().expect("temp directory");
        let directory = parent.path().join("agents");
        let catalog = AgentCatalog::load(&directory).expect("empty catalog");
        let definition = AgentDefinition {
            slug: "reviewer".to_owned(),
            description: "Reviews a bounded change".to_owned(),
            system_prompt: "Review carefully.".to_owned(),
            first_message: "Review the requested artifact.".to_owned(),
            model: Some("openai-codex/gpt-5".to_owned()),
            fallback_models: vec!["devin-acp/swe-1-7-lightning".to_owned()],
            fast_mode: true,
            reasoning_effort: Some("high".to_owned()),
            ..AgentDefinition::default()
        };

        catalog
            .save(&directory, &definition, None)
            .expect("save reviewer");

        let loaded = AgentCatalog::load(&directory).expect("configured catalog");
        assert_eq!(loaded.find("reviewer"), Some(&definition));
        assert_eq!(loaded.definitions(), [definition]);
    }
}
