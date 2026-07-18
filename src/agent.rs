use std::{collections::HashSet, fs, path::Path};

use serde::Deserialize;
use thiserror::Error;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
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
        Self {
            definitions: built_in_definitions(),
        }
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

        let mut definitions = built_in_definitions();
        definitions.reserve(paths.len());
        let mut custom_slugs = HashSet::new();
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
            if !custom_slugs.insert(definition.slug.clone()) {
                return Err(AgentCatalogError::DuplicateSlug {
                    slug: definition.slug,
                });
            }
            if let Some(existing) = definitions
                .iter_mut()
                .find(|existing| existing.slug == definition.slug)
            {
                *existing = definition;
            } else {
                definitions.push(definition);
            }
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
}

fn built_in_definitions() -> Vec<AgentDefinition> {
    vec![AgentDefinition {
        slug: "explorer".to_owned(),
        description: "Gathers relevant context and returns a concise, detailed report".to_owned(),
        system_prompt: "You are Nako Agent's read-only explorer. Investigate only the delegated question using the provider's native search and inspection tools. Gather the context the parent agent needs, including relevant architecture, behavior, constraints, code locations, and unresolved uncertainty. Do not modify files, implement changes, or delegate to other agents. Return a concise but detailed evidence-backed report with precise file locations where applicable. Separate established facts from inferences and identify anything you could not verify.".to_owned(),
        first_message: "Explore the delegated question and return the relevant context as a concise, detailed report for the parent agent.".to_owned(),
        model: Some("devin-acp/swe-1-7-lightning".to_owned()),
        fallback_models: vec!["openai-codex/gpt-5.6-luna".to_owned()],
    }]
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

    use super::AgentCatalog;

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
    fn missing_directory_uses_only_the_explorer_agent() {
        let directory = tempdir().expect("temp directory");
        let catalog = AgentCatalog::load(&directory.path().join("missing")).expect("catalog");

        assert_eq!(catalog.definitions().len(), 1);
        let explorer = catalog.find("explorer").expect("built-in explorer");
        assert_eq!(explorer.provider("openai-codex"), "devin-acp");
        assert_eq!(
            explorer.provider_model().as_deref(),
            Some("swe-1-7-lightning")
        );
        assert_eq!(explorer.fallback_models, ["openai-codex/gpt-5.6-luna"]);
    }

    #[test]
    fn workspace_definition_overrides_a_built_in_agent() {
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
}
