//! Atomic storage for Nakode's single configured `SOUL.md`.
use directories::ProjectDirs;
use sha2::{Digest, Sha256};
use std::{fs, io, path::PathBuf};
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SoulSource {
    File,
    Missing,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SoulRead {
    pub content: Option<String>,
    pub digest: Option<String>,
    pub source: SoulSource,
    pub path: PathBuf,
    pub exists: bool,
}

#[derive(Debug, Error)]
pub enum SoulError {
    #[error("Nakode configuration directory is unavailable")]
    MissingDirectory,
    #[error("failed to read Soul at {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("failed to write Soul at {path}: {source}")]
    Write {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("Soul content changed (expected {expected}, found {actual:?})")]
    Conflict {
        expected: String,
        actual: Option<String>,
    },
    #[error("SOUL.md already exists; reload before replacing it")]
    Appeared,
}

#[derive(Clone, Debug)]
pub struct SoulStore {
    path: PathBuf,
}

impl SoulStore {
    /// Resolves Nakode's one default `SOUL.md`.
    ///
    /// # Errors
    /// Returns [`SoulError::MissingDirectory`] when platform directories are unavailable.
    pub fn user_default() -> Result<Self, SoulError> {
        let project =
            ProjectDirs::from("dev", "nakode", "Nakode").ok_or(SoulError::MissingDirectory)?;
        Ok(Self::new(project.config_dir().join("SOUL.md")))
    }

    /// Uses the configured Soul path, or Nakode's one default `SOUL.md` when omitted.
    ///
    /// # Errors
    /// Returns [`SoulError::MissingDirectory`] when the default path cannot be resolved.
    pub fn configured(path: Option<&std::path::Path>) -> Result<Self, SoulError> {
        path.map_or_else(Self::user_default, |path| Ok(Self::new(path)))
    }

    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    /// Reads the configured singleton Soul or returns an explicit missing state.
    ///
    /// # Errors
    /// Returns an error when the file is unreadable or is not valid UTF-8.
    pub fn read(&self) -> Result<SoulRead, SoulError> {
        match fs::read_to_string(&self.path) {
            Ok(content) => Ok(read_value(self.path.clone(), content)),
            Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(SoulRead {
                content: None,
                digest: None,
                source: SoulSource::Missing,
                path: self.path.clone(),
                exists: false,
            }),
            Err(source) => Err(SoulError::Read {
                path: self.path.clone(),
                source,
            }),
        }
    }

    /// Atomically creates or replaces Nakode's configured singleton Soul.
    ///
    /// A digest is required to replace an existing file. Omitting it is the deliberate create flow
    /// and conflicts if a file appeared after the caller observed the missing state.
    ///
    /// # Errors
    /// Returns an error for stale state, unreadable current content, or failed persistence.
    pub fn save(&self, content: &str, expected: Option<&str>) -> Result<SoulRead, SoulError> {
        let current_bytes = match fs::read(&self.path) {
            Ok(bytes) => Some(bytes),
            Err(source) if source.kind() == io::ErrorKind::NotFound => None,
            Err(source) => {
                return Err(SoulError::Read {
                    path: self.path.clone(),
                    source,
                });
            }
        };
        let current = current_bytes.as_deref().map(digest);
        match expected {
            Some(expected) if current.as_deref() != Some(expected) => {
                return Err(SoulError::Conflict {
                    expected: expected.to_owned(),
                    actual: current,
                });
            }
            None if current.is_some() => return Err(SoulError::Appeared),
            _ => {}
        }

        let bytes = current_bytes.as_deref().map_or_else(
            || content.as_bytes().to_vec(),
            |old| preserve_style(old, content),
        );
        let parent = self.path.parent().ok_or_else(|| SoulError::Write {
            path: self.path.clone(),
            source: io::Error::other("SOUL.md path has no parent"),
        })?;
        fs::create_dir_all(parent).map_err(|source| SoulError::Write {
            path: self.path.clone(),
            source,
        })?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(parent, fs::Permissions::from_mode(0o700)).map_err(|source| {
                SoulError::Write {
                    path: self.path.clone(),
                    source,
                }
            })?;
        }

        let temporary = self
            .path
            .with_extension(format!("tmp-{}", uuid::Uuid::now_v7()));
        let write = (|| {
            use std::io::Write;
            let mut file = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temporary)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let mode = fs::metadata(&self.path)
                    .map_or(0o600, |metadata| metadata.permissions().mode());
                file.set_permissions(fs::Permissions::from_mode(mode))?;
            }
            file.write_all(&bytes)?;
            file.sync_all()?;
            fs::rename(&temporary, &self.path)
        })();
        write.map_err(|source| {
            let _ = fs::remove_file(&temporary);
            SoulError::Write {
                path: self.path.clone(),
                source,
            }
        })?;

        String::from_utf8(bytes)
            .map(|content| read_value(self.path.clone(), content))
            .map_err(|source| SoulError::Write {
                path: self.path.clone(),
                source: io::Error::new(io::ErrorKind::InvalidData, source),
            })
    }
}

fn read_value(path: PathBuf, content: String) -> SoulRead {
    let digest = digest(content.as_bytes());
    SoulRead {
        content: Some(content),
        digest: Some(digest),
        source: SoulSource::File,
        path,
        exists: true,
    }
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes.iter().fold(
        String::with_capacity(bytes.len() * 2),
        |mut output, byte| {
            write!(output, "{byte:02x}").expect("writing to String cannot fail");
            output
        },
    )
}

fn digest(bytes: &[u8]) -> String {
    let mut hash = Sha256::new();
    hash.update(bytes);
    hex(&hash.finalize())
}

fn preserve_style(old: &[u8], new: &str) -> Vec<u8> {
    let crlf = old.windows(2).any(|window| window == b"\r\n");
    let final_newline = old.ends_with(b"\n");
    let mut value = new.replace("\r\n", "\n");
    if crlf {
        value = value.replace('\n', "\r\n");
    }
    if final_newline && !value.ends_with(if crlf { "\r\n" } else { "\n" }) {
        value.push_str(if crlf { "\r\n" } else { "\n" });
    }
    if !final_newline {
        value.truncate(value.trim_end_matches(['\r', '\n']).len());
    }
    value.into_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn singleton_create_replace_reopen_and_style() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("config/SOUL.md");
        let store = SoulStore::new(&path);
        assert_eq!(store.read().expect("missing").source, SoulSource::Missing);

        let created = store.save("one", None).expect("create");
        let first_digest = created.digest.as_deref().expect("digest");
        assert_eq!(
            SoulStore::new(&path)
                .read()
                .expect("reopen")
                .content
                .as_deref(),
            Some("one")
        );
        assert!(matches!(
            store.save("clobber", None),
            Err(SoulError::Appeared)
        ));
        store.save("two", Some(first_digest)).expect("replace");

        fs::write(&path, b"a\r\nb\r\n").expect("styled fixture");
        let styled = store.read().expect("styled read");
        store
            .save("x\ny", styled.digest.as_deref())
            .expect("styled replace");
        assert_eq!(fs::read_to_string(path).expect("read"), "x\r\ny\r\n");
    }

    #[test]
    fn stale_malformed_and_failed_writes_are_explicit() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("SOUL.md");
        let store = SoulStore::new(&path);
        let first = store.save("one", None).expect("first");
        let digest = first.digest.as_deref().expect("digest");
        store.save("two", Some(digest)).expect("second");
        assert!(matches!(
            store.save("stale", Some(digest)),
            Err(SoulError::Conflict { .. })
        ));
        fs::write(&path, [0xff, 0xfe]).expect("malformed UTF-8");
        assert!(matches!(store.read(), Err(SoulError::Read { .. })));

        let blocked = directory.path().join("blocked");
        fs::write(&blocked, "not a directory").expect("block parent");
        assert!(matches!(
            SoulStore::new(blocked.join("SOUL.md")).save("content", None),
            Err(SoulError::Read { .. } | SoulError::Write { .. })
        ));
    }
}
