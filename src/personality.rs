use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
};

use directories::ProjectDirs;
use serde::Deserialize;
use thiserror::Error;

const PERSONALITIES_FILE: &str = "personalities.toml";
const SOUL_FILE: &str = "SOUL.md";

/// User-specific system-prompt addenda.
///
/// A model-specific personality replaces the global default personality. The
/// Soul is independent and is included whenever it exists.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PromptAddenda {
    default_personality: Option<String>,
    model_personalities: HashMap<String, String>,
    soul: Option<String>,
    personalities_path: Option<PathBuf>,
    personalities_required: bool,
    soul_path: Option<PathBuf>,
    soul_required: bool,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersonalityFile {
    default: Option<String>,
    #[serde(default)]
    models: HashMap<String, String>,
}

#[derive(Debug, Error)]
pub enum PromptAddendaError {
    #[error("the platform does not expose a user configuration directory")]
    MissingConfigDirectory,
    #[error("failed to read prompt addendum file {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid personality configuration {path}: {source}")]
    Parse {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },
    #[error("personality model key must use the provider/model form: {0}")]
    InvalidModel(String),
}

impl PromptAddenda {
    /// Loads addenda from explicit paths, or from Nakode's user configuration
    /// directory when a path is omitted. Missing default files are optional;
    /// an explicitly configured missing file is an error.
    ///
    /// # Errors
    ///
    /// Returns an error when an explicit file cannot be read, personality TOML
    /// is invalid, or a model key is not provider-qualified.
    pub fn load(
        personalities: Option<&Path>,
        soul: Option<&Path>,
    ) -> Result<Self, PromptAddendaError> {
        let needs_default_directory = personalities.is_none() || soul.is_none();
        let default_directory = needs_default_directory
            .then(Self::default_config_directory)
            .transpose()?;
        let personalities_path = personalities.map(Path::to_path_buf).or_else(|| {
            default_directory
                .as_ref()
                .map(|path| path.join(PERSONALITIES_FILE))
        });
        let soul_path = soul
            .map(Path::to_path_buf)
            .or_else(|| default_directory.as_ref().map(|path| path.join(SOUL_FILE)));

        Self::load_paths(
            personalities_path.as_deref(),
            personalities.is_some(),
            soul_path.as_deref(),
            soul.is_some(),
        )
    }

    fn default_config_directory() -> Result<PathBuf, PromptAddendaError> {
        ProjectDirs::from("dev", "nakode", "Nakode")
            .map(|project| project.config_dir().to_path_buf())
            .ok_or(PromptAddendaError::MissingConfigDirectory)
    }

    fn load_paths(
        personalities_path: Option<&Path>,
        personalities_required: bool,
        soul_path: Option<&Path>,
        soul_required: bool,
    ) -> Result<Self, PromptAddendaError> {
        let personality_file = personalities_path
            .and_then(|path| read_optional(path, personalities_required).transpose())
            .transpose()?
            .map(|contents| {
                toml::from_str::<PersonalityFile>(&contents).map_err(|source| {
                    PromptAddendaError::Parse {
                        path: personalities_path
                            .expect("path accompanies contents")
                            .to_path_buf(),
                        source,
                    }
                })
            })
            .transpose()?
            .unwrap_or_default();

        let mut model_personalities = HashMap::with_capacity(personality_file.models.len());
        for (model, personality) in personality_file.models {
            let model = model.trim().to_owned();
            if model
                .split_once('/')
                .is_none_or(|(provider, model)| provider.is_empty() || model.is_empty())
            {
                return Err(PromptAddendaError::InvalidModel(model));
            }
            if let Some(personality) = normalized(&personality) {
                model_personalities.insert(model, personality);
            }
        }

        let soul = soul_path
            .and_then(|path| read_optional(path, soul_required).transpose())
            .transpose()?
            .and_then(|contents| normalized(&contents));

        Ok(Self {
            default_personality: personality_file.default.as_deref().and_then(normalized),
            model_personalities,
            soul,
            personalities_path: personalities_path.map(Path::to_path_buf),
            personalities_required,
            soul_path: soul_path.map(Path::to_path_buf),
            soul_required,
        })
    }

    /// Reloads the same files used to create this set of addenda.
    ///
    /// # Errors
    ///
    /// Returns an error under the same conditions as [`Self::load`].
    pub fn reload(&self) -> Result<Self, PromptAddendaError> {
        Self::load_paths(
            self.personalities_path.as_deref(),
            self.personalities_required,
            self.soul_path.as_deref(),
            self.soul_required,
        )
    }

    #[must_use]
    pub fn personality_for(&self, qualified_model: Option<&str>) -> Option<&str> {
        qualified_model
            .and_then(|model| self.model_personalities.get(model))
            .map(String::as_str)
            .or(self.default_personality.as_deref())
    }

    #[must_use]
    pub fn soul(&self) -> Option<&str> {
        self.soul.as_deref()
    }

    /// Appends the effective personality and Soul to existing system
    /// instructions, preserving clear boundaries between hidden prompt layers.
    #[must_use]
    pub fn apply(&self, instructions: &str, qualified_model: Option<&str>) -> String {
        let mut result = instructions.trim().to_owned();
        if let Some(personality) = self.personality_for(qualified_model) {
            result.push_str("\n\n[Personality]\n");
            result.push_str(personality);
            result.push_str("\n[/Personality]");
        }
        if let Some(soul) = self.soul() {
            result.push_str("\n\n[Soul]\n");
            result.push_str(soul);
            result.push_str("\n[/Soul]");
        }
        result
    }
}

fn read_optional(path: &Path, required: bool) -> Result<Option<String>, PromptAddendaError> {
    match fs::read_to_string(path) {
        Ok(contents) => Ok(Some(contents)),
        Err(source) if !required && source.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(PromptAddendaError::Read {
            path: path.to_path_buf(),
            source,
        }),
    }
}

fn normalized(value: &str) -> Option<String> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_owned())
}

#[cfg(test)]
mod tests {
    use super::{PromptAddenda, PromptAddendaError};

    #[test]
    fn model_personality_overrides_default_and_soul_is_always_appended() {
        let directory = tempfile::tempdir().expect("tempdir");
        let personalities = directory.path().join("personalities.toml");
        let soul = directory.path().join("SOUL.md");
        std::fs::write(
            &personalities,
            "default = \"Globally warm.\"\n[models]\n\"openai-codex/gpt-test\" = \"Terse for this model.\"\n",
        )
        .expect("write personalities");
        std::fs::write(&soul, "I am Ada.\n").expect("write soul");

        let addenda = PromptAddenda::load(Some(&personalities), Some(&soul)).expect("load");
        let exact = addenda.apply("Base", Some("openai-codex/gpt-test"));
        assert!(exact.contains("[Personality]\nTerse for this model."));
        assert!(!exact.contains("Globally warm."));
        assert!(exact.contains("[Soul]\nI am Ada."));

        let fallback = addenda.apply("Base", Some("openai-codex/other"));
        assert!(fallback.contains("[Personality]\nGlobally warm."));
        assert!(fallback.contains("[Soul]\nI am Ada."));
    }

    #[test]
    fn reload_reads_updated_soul_and_personality_content() {
        let directory = tempfile::tempdir().expect("tempdir");
        let personalities = directory.path().join("personalities.toml");
        let soul = directory.path().join("SOUL.md");
        std::fs::write(&personalities, "default = \"First\"\n").expect("personality");
        std::fs::write(&soul, "Old identity").expect("soul");
        let addenda = PromptAddenda::load(Some(&personalities), Some(&soul)).expect("load");

        std::fs::write(&personalities, "default = \"Second\"\n").expect("personality");
        std::fs::write(&soul, "New identity").expect("soul");
        let reloaded = addenda.reload().expect("reload");
        let instructions = reloaded.apply("Base", None);
        assert!(instructions.contains("[Personality]\nSecond"));
        assert!(instructions.contains("[Soul]\nNew identity"));
        assert!(!instructions.contains("First"));
        assert!(!instructions.contains("Old identity"));
    }

    #[test]
    fn missing_optional_files_produce_no_addenda() {
        let directory = tempfile::tempdir().expect("tempdir");
        let addenda = PromptAddenda::load_paths(
            Some(&directory.path().join("personalities.toml")),
            false,
            Some(&directory.path().join("SOUL.md")),
            false,
        )
        .expect("missing defaults are optional");
        assert_eq!(addenda.apply("Base", None), "Base");
    }

    #[test]
    fn explicit_missing_file_is_reported() {
        let directory = tempfile::tempdir().expect("tempdir");
        let error = PromptAddenda::load(Some(&directory.path().join("missing.toml")), None)
            .expect_err("explicit path must exist");
        assert!(matches!(error, PromptAddendaError::Read { .. }));
    }

    #[test]
    fn invalid_model_key_is_rejected() {
        let directory = tempfile::tempdir().expect("tempdir");
        let personalities = directory.path().join("personalities.toml");
        std::fs::write(&personalities, "[models]\nunqualified = \"No\"\n")
            .expect("write personalities");
        let error = PromptAddenda::load(Some(&personalities), None).expect_err("invalid key");
        assert!(matches!(error, PromptAddendaError::InvalidModel(model) if model == "unqualified"));
    }
}
