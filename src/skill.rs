use std::{
    collections::{HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
};

use directories::BaseDirs;
use thiserror::Error;

use crate::controls::SKILL_PREFIX;

const SKILL_FILE: &str = "SKILL.md";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Skill {
    pub name: String,
    pub description: String,
    pub instructions: String,
    pub path: PathBuf,
}

#[derive(Clone, Debug, Default)]
pub struct SkillCatalog {
    skills: Vec<Skill>,
}

#[derive(Debug, Error)]
pub enum SkillCatalogError {
    #[error("failed to read skill directory {path}: {source}")]
    ReadDirectory {
        path: String,
        source: std::io::Error,
    },
    #[error("failed to read skill definition {path}: {source}")]
    ReadDefinition {
        path: String,
        source: std::io::Error,
    },
    #[error(
        "skill directory {path} must use a name containing lowercase letters, digits, or hyphens"
    )]
    InvalidName { path: String },
    #[error(
        "skill definition {path} declares name {declared:?}, but its directory is named {directory:?}"
    )]
    NameMismatch {
        path: String,
        declared: String,
        directory: String,
    },
    #[error("skill definition {path} is empty")]
    EmptyDefinition { path: String },
}

impl SkillCatalog {
    /// Discovers user skills first and workspace skills second. A workspace skill
    /// replaces a user skill with the same name.
    ///
    /// # Errors
    ///
    /// Returns an error when a skill directory or definition cannot be read, or
    /// when an installed skill has an invalid name or empty definition.
    pub fn load(workspace: &Path) -> Result<Self, SkillCatalogError> {
        let user_root = BaseDirs::new().map(|base| base.home_dir().join(".agents/skills"));
        Self::load_from_roots(
            user_root.as_deref(),
            Some(&workspace.join(".agents/skills")),
        )
    }

    fn load_from_roots(
        user_root: Option<&Path>,
        workspace_root: Option<&Path>,
    ) -> Result<Self, SkillCatalogError> {
        let mut skills = HashMap::new();
        if let Some(root) = user_root {
            discover_root(root, &mut skills)?;
        }
        if let Some(root) = workspace_root {
            discover_root(root, &mut skills)?;
        }
        let mut skills = skills.into_values().collect::<Vec<_>>();
        skills.sort_unstable_by(|left, right| left.name.cmp(&right.name));
        Ok(Self { skills })
    }

    #[must_use]
    pub fn definitions(&self) -> &[Skill] {
        &self.skills
    }

    #[must_use]
    pub fn find(&self, name: &str) -> Option<&Skill> {
        self.skills.iter().find(|skill| skill.name == name)
    }

    /// Resolves each distinct skill reference in a prompt.
    ///
    /// # Errors
    ///
    /// Returns the first referenced skill name that is not installed.
    pub fn referenced<'a>(&'a self, prompt: &str) -> Result<Vec<&'a Skill>, String> {
        let mut seen = HashSet::new();
        let mut skills = Vec::new();
        for name in referenced_skill_names(prompt) {
            if !seen.insert(name) {
                continue;
            }
            let Some(skill) = self.find(name) else {
                return Err(name.to_owned());
            };
            skills.push(skill);
        }
        Ok(skills)
    }

    /// Appends explicitly referenced skill instructions to a provider prompt.
    ///
    /// # Errors
    ///
    /// Returns the first referenced skill name that is not installed.
    pub fn render_prompt(&self, prompt: &str) -> Result<String, String> {
        let skills = self.referenced(prompt)?;
        if skills.is_empty() {
            return Ok(prompt.to_owned());
        }

        let mut rendered = prompt.to_owned();
        rendered.push_str(
            "\n\n# Nakode attached skills\n\nFollow the instructions from each explicitly referenced skill below.\n",
        );
        for skill in skills {
            rendered.push_str("\n## Skill: ");
            rendered.push_str(&skill.name);
            rendered.push('\n');
            rendered.push_str(&skill.instructions);
            if !skill.instructions.ends_with('\n') {
                rendered.push('\n');
            }
        }
        Ok(rendered)
    }
}

fn discover_root(
    root: &Path,
    skills: &mut HashMap<String, Skill>,
) -> Result<(), SkillCatalogError> {
    if !root.exists() {
        return Ok(());
    }
    let entries = fs::read_dir(root).map_err(|source| SkillCatalogError::ReadDirectory {
        path: root.display().to_string(),
        source,
    })?;
    for entry in entries {
        let entry = entry.map_err(|source| SkillCatalogError::ReadDirectory {
            path: root.display().to_string(),
            source,
        })?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let definition = path.join(SKILL_FILE);
        if !definition.is_file() {
            continue;
        }
        let skill = read_skill(&definition)?;
        skills.insert(skill.name.clone(), skill);
    }
    Ok(())
}

fn read_skill(path: &Path) -> Result<Skill, SkillCatalogError> {
    let directory = path
        .parent()
        .and_then(Path::file_name)
        .and_then(|name| name.to_str())
        .unwrap_or_default();
    if !valid_name(directory) {
        return Err(SkillCatalogError::InvalidName {
            path: path.display().to_string(),
        });
    }
    let instructions =
        fs::read_to_string(path).map_err(|source| SkillCatalogError::ReadDefinition {
            path: path.display().to_string(),
            source,
        })?;
    if instructions.trim().is_empty() {
        return Err(SkillCatalogError::EmptyDefinition {
            path: path.display().to_string(),
        });
    }
    let metadata = frontmatter(&instructions);
    if let Some(declared) = metadata.name.as_deref()
        && declared != directory
    {
        return Err(SkillCatalogError::NameMismatch {
            path: path.display().to_string(),
            declared: declared.to_owned(),
            directory: directory.to_owned(),
        });
    }
    Ok(Skill {
        name: directory.to_owned(),
        description: metadata
            .description
            .unwrap_or_else(|| format!("use the {directory} skill")),
        instructions,
        path: path.to_path_buf(),
    })
}

#[derive(Default)]
struct Frontmatter {
    name: Option<String>,
    description: Option<String>,
}

fn frontmatter(contents: &str) -> Frontmatter {
    let mut lines = contents.lines();
    if lines.next().map(str::trim) != Some("---") {
        return Frontmatter::default();
    }
    let mut metadata = Frontmatter::default();
    for line in lines {
        if line.trim() == "---" {
            break;
        }
        let Some((key, value)) = line.split_once(':') else {
            continue;
        };
        let value = value.trim().trim_matches(['\'', '"']);
        match key.trim() {
            "name" if !value.is_empty() => metadata.name = Some(value.to_owned()),
            "description" if !value.is_empty() => metadata.description = Some(value.to_owned()),
            _ => {}
        }
    }
    metadata
}

fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
}

#[must_use]
pub fn referenced_skill_names(prompt: &str) -> Vec<&str> {
    let mut names = Vec::new();
    let mut offset = 0;
    while let Some(relative) = prompt[offset..].find(SKILL_PREFIX) {
        let start = offset + relative + SKILL_PREFIX.len();
        let length = prompt[start..]
            .bytes()
            .take_while(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'-')
            .count();
        if length > 0 {
            names.push(&prompt[start..start + length]);
        }
        offset = start + length.max(1);
        if offset >= prompt.len() {
            break;
        }
    }
    names
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::*;

    fn write_skill(root: &Path, name: &str, description: &str, body: &str) {
        let directory = root.join(name);
        fs::create_dir_all(&directory).expect("create skill directory");
        fs::write(
            directory.join(SKILL_FILE),
            format!("---\nname: {name}\ndescription: {description}\n---\n\n{body}\n"),
        )
        .expect("write skill");
    }

    #[test]
    fn workspace_skills_override_user_skills() {
        let user = tempdir().expect("user root");
        let workspace = tempdir().expect("workspace root");
        write_skill(
            user.path(),
            "review",
            "global review",
            "Global instructions.",
        );
        write_skill(
            workspace.path(),
            "review",
            "project review",
            "Project instructions.",
        );
        write_skill(user.path(), "testing", "run tests", "Test instructions.");

        let catalog = SkillCatalog::load_from_roots(Some(user.path()), Some(workspace.path()))
            .expect("load skills");

        assert_eq!(catalog.definitions().len(), 2);
        assert_eq!(
            catalog.find("review").unwrap().description,
            "project review"
        );
        assert!(
            catalog
                .find("review")
                .unwrap()
                .instructions
                .contains("Project instructions.")
        );
    }

    #[test]
    fn references_are_discovered_and_rendered_once() {
        let root = tempdir().expect("skill root");
        write_skill(root.path(), "review", "review code", "Review carefully.");
        let catalog = SkillCatalog::load_from_roots(Some(root.path()), None).unwrap();

        let rendered = catalog
            .render_prompt("Use /skill:review, then /skill:review again.")
            .unwrap();

        assert!(rendered.starts_with("Use /skill:review"));
        assert_eq!(rendered.matches("## Skill: review").count(), 1);
        assert!(rendered.contains("Review carefully."));
    }

    #[test]
    fn unknown_references_are_reported() {
        let catalog = SkillCatalog::default();
        assert_eq!(
            catalog.render_prompt("Use /skill:missing").unwrap_err(),
            "missing"
        );
    }

    #[test]
    fn reference_parser_stops_at_punctuation() {
        assert_eq!(
            referenced_skill_names("/skill:first, /skill:second."),
            vec!["first", "second"]
        );
    }
}
