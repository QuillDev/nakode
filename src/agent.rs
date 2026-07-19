use std::{collections::HashSet, fs, path::Path};

use serde::{Deserialize, Serialize};
use thiserror::Error;

const DEFAULT_AGENT_CONFIG_PATH: &str = "config/default-agents.toml";
const DEFAULT_AGENT_CONFIG: &str = include_str!("../config/default-agents.toml");

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DefaultAgentConfig {
    agents: Vec<AgentDefinition>,
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct AgentDefinition {
    pub slug: String,
    pub description: String,
    pub system_prompt: String,
    pub first_message: String,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub fallback_models: Vec<String>,
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
    pub fn initial_prompt(&self, task: &str) -> String {
        format!(
            "{}\n\n# Delegated task\n\n{}",
            self.first_message.trim(),
            task.trim()
        )
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
}

#[derive(Clone, Debug)]
pub struct AgentCatalog {
    definitions: Vec<AgentDefinition>,
}

impl Default for AgentCatalog {
    fn default() -> Self {
        Self::from_default_config().expect("shipped default agent configuration must be valid")
    }
}

impl AgentCatalog {
    /// Loads all TOML agent definitions from `directory` in filename order.
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
    /// If the catalog has not been customized yet, shipped definitions are
    /// materialized first so editing one archetype does not discard the rest.
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
            remove_if_present(&definition_path(directory, previous_slug))?;
        }
        write_definition(directory, definition)
    }

    /// Removes an archetype from the authoritative workspace catalog.
    ///
    /// # Errors
    /// Returns an error when the catalog cannot be materialized or the file removed.
    pub fn delete(&self, directory: &Path, slug: &str) -> Result<(), AgentCatalogError> {
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

    fn from_default_config() -> Result<Self, AgentCatalogError> {
        let config =
            toml::from_str::<DefaultAgentConfig>(DEFAULT_AGENT_CONFIG).map_err(|source| {
                AgentCatalogError::ParseDefinition {
                    path: DEFAULT_AGENT_CONFIG_PATH.to_owned(),
                    source,
                }
            })?;
        let mut slugs = HashSet::new();
        for definition in &config.agents {
            validate(definition, DEFAULT_AGENT_CONFIG_PATH)?;
            if !slugs.insert(definition.slug.clone()) {
                return Err(AgentCatalogError::DuplicateSlug {
                    slug: definition.slug.clone(),
                });
            }
        }
        Ok(Self {
            definitions: config.agents,
        })
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
    fs::write(&path, source).map_err(|source| AgentCatalogError::WriteDefinition {
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
    for (field, value) in [
        ("description", definition.description.as_str()),
        ("system_prompt", definition.system_prompt.as_str()),
        ("first_message", definition.first_message.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(AgentCatalogError::EmptyField {
                path: path.to_owned(),
                field,
            });
        }
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

    use super::{AgentCatalog, AgentDefinition};

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
    fn missing_directory_uses_the_shipped_agent_configuration() {
        let directory = tempdir().expect("temp directory");
        let catalog = AgentCatalog::load(&directory.path().join("missing")).expect("catalog");

        assert_eq!(catalog.definitions().len(), 1);
        let explorer = catalog.find("explorer").expect("configured explorer");
        assert_eq!(explorer.provider("openai-codex"), "devin-acp");
        assert_eq!(
            explorer.provider_model().as_deref(),
            Some("swe-1-7-lightning")
        );
        assert_eq!(explorer.fallback_models, ["openai-codex/gpt-5.6-luna"]);
    }

    #[test]
    fn workspace_definition_overrides_a_shipped_agent() {
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
    fn deleting_a_shipped_agent_materializes_an_authoritative_empty_catalog() {
        let parent = tempdir().expect("temp directory");
        let directory = parent.path().join("agents");
        let catalog = AgentCatalog::load(&directory).expect("default catalog");

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
    fn saves_a_custom_agent_without_discarding_initial_presets() {
        let parent = tempdir().expect("temp directory");
        let directory = parent.path().join("agents");
        let catalog = AgentCatalog::load(&directory).expect("default catalog");
        let definition = AgentDefinition {
            slug: "reviewer".to_owned(),
            description: "Reviews a bounded change".to_owned(),
            system_prompt: "Review carefully.".to_owned(),
            first_message: "Review the requested artifact.".to_owned(),
            model: Some("openai-codex/gpt-5".to_owned()),
            fallback_models: vec!["devin-acp/swe-1-7-lightning".to_owned()],
        };

        catalog
            .save(&directory, &definition, None)
            .expect("save reviewer");

        let loaded = AgentCatalog::load(&directory).expect("configured catalog");
        assert_eq!(loaded.find("reviewer"), Some(&definition));
        assert!(loaded.find("explorer").is_some());
    }
}
