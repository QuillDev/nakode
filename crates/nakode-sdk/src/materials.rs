//! Public locality and scoped material clients. This module never discovers an endpoint from prose,
//! chooses a different host after failure, or treats route metadata as authorization.
use nakode_api::v1 as api;

use crate::{NakodeClient, SdkError};

#[cfg(test)]
mod tests;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MachineLocality {
    SameMachine,
    Remote,
    Unknown,
}

/// Compare only complete authority-scoped machine identities returned by authenticated services.
#[must_use]
pub fn machine_locality(
    left: Option<&api::ExecutionLocation>,
    right: Option<&api::ExecutionLocation>,
) -> MachineLocality {
    let (Some(left), Some(right)) = (
        left.and_then(|value| value.machine.as_ref()),
        right.and_then(|value| value.machine.as_ref()),
    ) else {
        return MachineLocality::Unknown;
    };
    if !valid_machine(left) || !valid_machine(right) || left.authority != right.authority {
        return MachineLocality::Unknown;
    }
    if left.id == right.id {
        MachineLocality::SameMachine
    } else {
        MachineLocality::Remote
    }
}

fn valid_machine(machine: &api::ExecutionMachine) -> bool {
    [&machine.authority, &machine.id]
        .iter()
        .all(|value| !value.is_empty() && value.len() <= 200 && value.trim() == value.as_str())
}

/// A caller supplies already authorized clients, never an endpoint selected by model text.
/// Same-machine operations require the direct service; an unavailable direct path never falls back
/// to the cloud. Cross-runtime relationships are independently checked by the destination runtime.
pub struct MaterialClient {
    client: NakodeClient,
    route: api::SessionRouting,
}

impl MaterialClient {
    /// Selects a transport using trusted discovery and rechecks the exact destination incarnation.
    ///
    /// # Errors
    /// Refuses unknown locality, absent capability, unavailable selected transport or changed identity.
    pub async fn bind(
        caller: &api::ExecutionLocation,
        expected: &api::SessionRouting,
        local: Option<&NakodeClient>,
        remote: Option<&NakodeClient>,
    ) -> Result<Self, SdkError> {
        let (client, direct) = match machine_locality(Some(caller), expected.location.as_ref()) {
            MachineLocality::SameMachine => (
                local.ok_or_else(|| {
                    refused("direct local service is unavailable; no proxy fallback")
                })?,
                true,
            ),
            MachineLocality::Remote => (
                remote.ok_or_else(|| refused("authenticated remote transport is unavailable"))?,
                false,
            ),
            MachineLocality::Unknown => {
                return Err(refused(
                    "execution locality is unknown; refresh authorized routing",
                ));
            }
        };
        let route = client.get_session_routing(&expected.session_id).await?;
        if route.session_id != expected.session_id || route.location != expected.location {
            return Err(refused(
                "execution destination changed; refresh routing, do not switch hosts",
            ));
        }
        for operation in ["ListChildMaterials", "GetChildMaterial"] {
            let supported = route
                .operations
                .iter()
                .filter(|entry| entry.operation == operation)
                .collect::<Vec<_>>();
            if supported.len() != 1
                || !supported[0].relationship_scope.eq("same_runtime")
                || !(if direct {
                    supported[0].direct_local_service
                } else {
                    supported[0].authenticated_remote
                })
            {
                return Err(refused(
                    "selected transport does not support scoped child materials",
                ));
            }
        }
        Ok(Self {
            client: client.clone(),
            route,
        })
    }

    /// Retrieves one bounded metadata page without fetching image bytes.
    ///
    /// # Errors
    /// Refuses invalid bounds/source, stale destination, ownership or relationship failure.
    pub async fn list(
        &self,
        scope: api::MaterialScope,
        after: Option<String>,
        limit: u32,
    ) -> Result<api::MaterialPage, SdkError> {
        self.check_scope(&scope)?;
        if !(1..=64).contains(&limit) {
            return Err(refused("material page size must be 1–64"));
        }
        let page = self
            .client
            .transport
            .clone()
            .list_child_materials(api::ListChildMaterialsRequest {
                scope: Some(scope.clone()),
                after_artifact_id: after,
                limit,
                expected_runtime_epoch: self.epoch()?,
            })
            .await?
            .into_inner();
        if page.scope.as_ref() != Some(&scope) || page.items.len() > limit as usize {
            return Err(refused(
                "material response changed source or exceeded the requested bound",
            ));
        }
        Ok(page)
    }

    /// Retrieves just the selected original/crop with source attribution. This is not a Chat append.
    ///
    /// # Errors
    /// Missing/expired/inaccessible images and unsupported transforms remain explicit server errors.
    pub async fn get(
        &self,
        scope: api::MaterialScope,
        reference: String,
        transform: Option<api::ImageTransform>,
    ) -> Result<api::ChildMaterial, SdkError> {
        self.check_scope(&scope)?;
        let value = self
            .client
            .transport
            .clone()
            .get_child_material(api::GetChildMaterialRequest {
                scope: Some(scope.clone()),
                image_reference: reference,
                transform,
                expected_runtime_epoch: self.epoch()?,
            })
            .await?
            .into_inner();
        if value.scope.as_ref() != Some(&scope)
            || value.artifact.as_ref().is_none_or(|artifact| {
                artifact.data.is_empty()
                    || artifact.data.len() > 5 * 1024 * 1024
                    || artifact.byte_length != artifact.data.len() as u64
            })
        {
            return Err(refused(
                "material response changed source or violated image bounds",
            ));
        }
        Ok(value)
    }

    fn check_scope(&self, scope: &api::MaterialScope) -> Result<(), SdkError> {
        if scope.parent_session_id.is_empty()
            || scope
                .source
                .as_ref()
                .is_none_or(|source| source.session_id != self.route.session_id)
        {
            return Err(refused(
                "material scope does not match the selected destination",
            ));
        }
        Ok(())
    }

    fn epoch(&self) -> Result<String, SdkError> {
        self.route
            .location
            .as_ref()
            .filter(|location| !location.runtime_epoch.is_empty() && !location.server_id.is_empty())
            .map(|location| location.runtime_epoch.clone())
            .ok_or_else(|| refused("runtime identity is unavailable"))
    }
}

impl NakodeClient {
    /// Read authoritative session placement without opening or restoring provider work.
    ///
    /// # Errors
    /// Old servers return Unimplemented; missing sessions, disconnects and auth errors stay explicit.
    pub async fn get_session_routing(
        &self,
        session_id: impl Into<String>,
    ) -> Result<api::SessionRouting, SdkError> {
        Ok(self
            .transport
            .clone()
            .get_session_routing(api::GetSessionRoutingRequest {
                session_id: session_id.into(),
            })
            .await?
            .into_inner())
    }
}

fn refused(message: &str) -> SdkError {
    SdkError::Status(tonic::Status::failed_precondition(message))
}
