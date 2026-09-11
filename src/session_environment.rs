//! Server-owned, ephemeral environment for independently dispatched tools. Values never enter
//! session snapshots, provider context, or persistence. Keys are canonical logical session IDs;
//! delegated provider sessions retain that same owner ID.
use nakode_protocol::CredentialInput;
use std::{
    collections::{BTreeMap, HashMap},
    sync::{OnceLock, RwLock},
};
type Environments = HashMap<String, HashMap<String, String>>;
static ENVIRONMENTS: OnceLock<RwLock<Environments>> = OnceLock::new();
fn store() -> &'static RwLock<Environments> {
    ENVIRONMENTS.get_or_init(RwLock::default)
}
pub fn replace(
    session_id: &str,
    variables: BTreeMap<String, CredentialInput>,
) -> Result<(), String> {
    if variables.len() > 128
        || variables.iter().any(|(name, value)| {
            let mut chars = name.bytes();
            !chars
                .next()
                .is_some_and(|c| c.is_ascii_alphabetic() || c == b'_')
                || !chars.all(|c| c.is_ascii_alphanumeric() || c == b'_')
                || name.len() > 128
                || name.starts_with("FSTACK_")
                || name.starts_with("NAKODE_")
                || value.0.len() > 16384
                || value.0.contains('\0')
        })
    {
        return Err("invalid session environment".to_owned());
    }
    let mut environments = store()
        .write()
        .map_err(|_| "session environment unavailable")?;
    if variables.is_empty() {
        environments.remove(session_id);
    } else {
        environments.insert(
            session_id.to_owned(),
            variables
                .into_iter()
                .map(|(name, value)| (name, value.0))
                .collect(),
        );
    }
    Ok(())
}
pub fn remove(session_id: &str) {
    if let Ok(mut environments) = store().write() {
        environments.remove(session_id);
    }
}
pub fn read(session_id: Option<&str>) -> HashMap<String, String> {
    let mut environment = crate::machine_path::environment();
    environment.extend(
        session_id
            .and_then(|id| store().read().ok()?.get(id).cloned())
            .unwrap_or_default(),
    );
    environment
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn scoped_replace_clear_and_validation() {
        let id = "environment-unit-test";
        replace(
            id,
            BTreeMap::from([("TOKEN".into(), CredentialInput("secret".into()))]),
        )
        .unwrap();
        assert_eq!(
            read(Some(id)).get("TOKEN").map(String::as_str),
            Some("secret")
        );
        assert!(read(Some("other-environment-unit-test")).is_empty());
        assert!(
            replace(
                id,
                BTreeMap::from([("BAD=NAME".into(), CredentialInput("secret".into()))])
            )
            .is_err()
        );
        replace(id, BTreeMap::new()).unwrap();
        assert!(read(Some(id)).is_empty());
    }
}
