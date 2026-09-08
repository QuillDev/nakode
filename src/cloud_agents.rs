//! Profile-owned archetype replicas. The cloud owns definitions; this file is only a local cache.
use std::{collections::HashSet, fs, io::Write, path::Path};

use fs2::FileExt;
use serde::{Deserialize, Serialize};

use crate::agent::{AgentCatalog, AgentDefinition, AgentOwnership};

const CACHE: &str = "cloud-catalogue.json";
const MAX_BYTES: u64 = 4 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CloudAgentCatalogue {
    pub profile_id: String,
    pub revision: u64,
    pub definitions: Vec<AgentDefinition>,
}

impl CloudAgentCatalogue {
    fn validate(&self) -> Result<(), String> {
        if self.profile_id.is_empty() || self.profile_id.len() > 128 || self.revision == 0 {
            return Err(
                "cloud catalogue requires a profile identity and positive revision".to_owned(),
            );
        }
        if self.definitions.len() > 512 {
            return Err("cloud catalogue exceeds 512 archetypes".to_owned());
        }
        let mut slugs = HashSet::new();
        for definition in &self.definitions {
            AgentCatalog::validate_definition(definition).map_err(|error| error.to_string())?;
            if definition.ownership != AgentOwnership::OwnerDefined {
                return Err("cloud catalogues may contain only owner-defined archetypes".to_owned());
            }
            if !slugs.insert(&definition.slug) {
                return Err(format!("duplicate cloud archetype {:?}", definition.slug));
            }
            if definition.model.is_none() {
                return Err(format!(
                    "cloud archetype {:?} requires an explicit provider/model",
                    definition.slug
                ));
            }
        }
        Ok(())
    }
}

pub(crate) fn load(directory: &Path) -> Result<Option<CloudAgentCatalogue>, String> {
    let path = directory.join(CACHE);
    let metadata = match fs::metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(format!("read cloud archetype cache: {error}")),
    };
    if metadata.len() > MAX_BYTES {
        return Err("cloud archetype cache exceeds 4 MiB".to_owned());
    }
    let bytes = fs::read(path).map_err(|error| format!("read cloud archetype cache: {error}"))?;
    let catalogue: CloudAgentCatalogue = serde_json::from_slice(&bytes)
        .map_err(|error| format!("invalid cloud archetype cache; restore the last valid local cache before reconnecting: {error}"))?;
    catalogue.validate()?;
    Ok(Some(catalogue))
}

/// Validation and revision checks precede one replacement. Local custom TOMLs become inert recovery data.
pub(crate) fn apply(directory: &Path, mut incoming: CloudAgentCatalogue) -> Result<(), String> {
    incoming
        .definitions
        .sort_by(|left, right| left.slug.cmp(&right.slug));
    // Local telemetry identities cannot be supplied by a remote replica.
    for definition in &mut incoming.definitions {
        definition.id = format!("cloud:{}:{}", incoming.profile_id, definition.slug);
    }
    incoming.validate()?;
    fs::create_dir_all(directory).map_err(|error| error.to_string())?;
    let lock = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(directory.join(".cloud-catalogue.lock"))
        .map_err(|error| error.to_string())?;
    lock.lock_exclusive().map_err(|error| error.to_string())?;
    let current = load(directory)?;
    if let Some(current) = &current {
        if current.profile_id != incoming.profile_id {
            return Err("this machine's archetype cache belongs to another profile; use a separate Nakode home".to_owned());
        }
        if incoming.revision < current.revision {
            return Err(format!(
                "stale cloud archetype revision {}; machine has {}",
                incoming.revision, current.revision
            ));
        }
        if incoming.revision == current.revision {
            return if current == &incoming {
                Ok(())
            } else {
                Err("cloud archetype revision was reused with different definitions".to_owned())
            };
        }
    }
    let local = AgentCatalog::load_builtins(directory).map_err(|error| error.to_string())?;
    for builtin in local
        .definitions()
        .iter()
        .filter(|definition| definition.ownership == AgentOwnership::BuiltIn)
    {
        if incoming
            .definitions
            .iter()
            .any(|definition| definition.slug == builtin.slug)
        {
            return Err(format!(
                "cloud archetype {:?} conflicts with an immutable built-in on this machine",
                builtin.slug
            ));
        }
    }

    let bytes = serde_json::to_vec(&incoming).map_err(|error| error.to_string())?;
    if bytes.len() as u64 > MAX_BYTES {
        return Err("cloud archetype catalogue exceeds 4 MiB".to_owned());
    }
    let temporary = directory.join(format!(".cloud-catalogue-{}.tmp", uuid::Uuid::now_v7()));
    let result = (|| {
        let mut file = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
            .map_err(|error| error.to_string())?;
        file.write_all(&bytes).map_err(|error| error.to_string())?;
        file.sync_all().map_err(|error| error.to_string())?;
        fs::rename(&temporary, directory.join(CACHE)).map_err(|error| error.to_string())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn catalogue(revision: u64) -> CloudAgentCatalogue {
        CloudAgentCatalogue {
            profile_id: "profile-a".to_owned(),
            revision,
            definitions: vec![AgentDefinition {
                slug: "worker".to_owned(),
                description: "Work".to_owned(),
                model: Some("unavailable/model".to_owned()),
                ..AgentDefinition::default()
            }],
        }
    }

    #[test]
    fn replica_is_atomic_revisioned_and_profile_bound() {
        let directory = tempfile::tempdir().unwrap();
        apply(directory.path(), catalogue(2)).unwrap();
        apply(directory.path(), catalogue(2)).unwrap();
        assert!(
            apply(directory.path(), catalogue(1))
                .unwrap_err()
                .contains("stale")
        );
        let mut conflicting = catalogue(2);
        conflicting.definitions[0].enabled = false;
        assert!(apply(directory.path(), conflicting.clone()).is_err());
        conflicting.revision = 3;
        conflicting.profile_id = "profile-b".to_owned();
        assert!(apply(directory.path(), conflicting).is_err());
        assert!(
            AgentCatalog::load(directory.path())
                .unwrap()
                .find("worker")
                .unwrap()
                .enabled
        );
        let mut disabled = catalogue(3);
        disabled.definitions[0].enabled = false;
        apply(directory.path(), disabled).unwrap();
        assert!(
            !AgentCatalog::load(directory.path())
                .unwrap()
                .find("worker")
                .unwrap()
                .enabled
        );
        let mut empty = catalogue(4);
        empty.definitions.clear();
        apply(directory.path(), empty).unwrap();
        assert!(
            AgentCatalog::load(directory.path())
                .unwrap()
                .definitions()
                .is_empty()
        );
    }

    #[test]
    fn cloud_cutover_never_imports_or_rewrites_local_files() {
        let directory = tempfile::tempdir().unwrap();
        let local = "slug = 'local'\ndescription = 'local definition'\n";
        fs::write(directory.path().join("local.toml"), local).unwrap();
        fs::write(directory.path().join("broken-legacy.toml"), "invalid = [").unwrap();
        fs::write(
            directory.path().join("builtin.toml"),
            "slug = 'builtin'\ndescription = 'Builtin'\nownership = 'built_in'\n",
        )
        .unwrap();
        apply(directory.path(), catalogue(1)).unwrap();
        assert!(
            AgentCatalog::load(directory.path())
                .unwrap()
                .find("builtin")
                .is_some()
        );
        let bound = AgentCatalog::load(directory.path()).unwrap();
        assert!(
            bound
                .save(directory.path(), &catalogue(1).definitions[0], None)
                .is_err()
        );
        assert!(bound.delete(directory.path(), "worker").is_err());
        assert_eq!(
            fs::read_to_string(directory.path().join("local.toml")).unwrap(),
            local
        );
        assert!(
            AgentCatalog::load(directory.path())
                .unwrap()
                .find("local")
                .is_none()
        );
    }

    #[test]
    fn invalid_batch_and_builtin_conflicts_leave_cache_untouched() {
        let directory = tempfile::tempdir().unwrap();
        let mut invalid = catalogue(1);
        invalid.definitions.push(invalid.definitions[0].clone());
        assert!(apply(directory.path(), invalid).is_err());
        assert!(load(directory.path()).unwrap().is_none());
        fs::write(
            directory.path().join("builtin.toml"),
            "slug = 'worker'\ndescription = 'Builtin'\nownership = 'built_in'\n",
        )
        .unwrap();
        assert!(
            apply(directory.path(), catalogue(1))
                .unwrap_err()
                .contains("immutable built-in")
        );
    }
}
