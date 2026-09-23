//! Listener-owned route facts. No endpoint URI or credential is ever projected.
use nakode_api::v1 as api;
use nakode_protocol as protocol;

use super::GrpcService;

#[derive(Clone, Default)]
pub(super) struct Routing {
    pub machine: Option<api::ExecutionMachine>,
    pub direct_local_service: bool,
    pub authenticated_remote: bool,
}

impl GrpcService {
    /// Binds machine identity and transport availability from the trusted service launcher, never
    /// from a request, model argument, hostname, or workspace path. An absent identity stays unknown.
    ///
    /// # Errors
    /// Rejects incomplete or oversized authority-scoped identities.
    pub fn with_execution_routing(
        mut self,
        machine: Option<api::ExecutionMachine>,
        direct_local_service: bool,
        authenticated_remote: bool,
    ) -> Result<Self, tonic::Status> {
        if machine.as_ref().is_some_and(|machine| {
            [&machine.authority, &machine.id].iter().any(|value| {
                value.is_empty() || value.len() > 200 || value.trim() != value.as_str()
            })
        }) {
            return Err(tonic::Status::invalid_argument(
                "invalid execution machine identity",
            ));
        }
        self.routing = Routing {
            machine,
            direct_local_service,
            authenticated_remote,
        };
        Ok(self)
    }

    pub(super) fn execution_location(&self) -> api::ExecutionLocation {
        api::ExecutionLocation {
            machine: self.routing.machine.clone(),
            server_id: self.server_id.clone(),
            runtime_epoch: self.endpoint.epoch().to_string(),
        }
    }

    pub(super) fn operation_routing(&self) -> Vec<api::OperationRouting> {
        let capabilities = self.endpoint.capabilities();
        if !capabilities.supports(protocol::ServiceCapability::ChildMaterials)
            || !capabilities.supports(protocol::ServiceCapability::ArtifactTransfer)
        {
            return Vec::new();
        }
        ["ListChildMaterials", "GetChildMaterial"]
            .into_iter()
            .map(|operation| api::OperationRouting {
                operation: operation.to_owned(),
                direct_local_service: self.routing.direct_local_service,
                authenticated_remote: self.routing.authenticated_remote,
                relationship_scope: "same_runtime".to_owned(),
            })
            .collect()
    }

    pub(super) fn check_material_epoch(&self, epoch: &str) -> Result<(), tonic::Status> {
        if epoch != self.endpoint.epoch().as_str() {
            return Err(tonic::Status::failed_precondition(
                "runtime identity is missing or stale; refresh routing, do not switch hosts",
            ));
        }
        Ok(())
    }
}

pub(super) fn material_scope(
    value: Option<api::MaterialScope>,
) -> Result<protocol::MaterialScope, tonic::Status> {
    let value =
        value.ok_or_else(|| tonic::Status::invalid_argument("material scope is required"))?;
    let source = value
        .source
        .ok_or_else(|| tonic::Status::invalid_argument("material source is required"))?;
    Ok(protocol::MaterialScope {
        parent_session_id: value.parent_session_id.into(),
        source: protocol::MaterialSource {
            session_id: source.session_id.into(),
            run_id: source.run_id.map(Into::into),
        },
    })
}

pub(super) fn scope_view(scope: protocol::MaterialScope) -> api::MaterialScope {
    api::MaterialScope {
        parent_session_id: scope.parent_session_id.to_string(),
        source: Some(api::MaterialSource {
            session_id: scope.source.session_id.to_string(),
            run_id: scope.source.run_id.map(|id| id.to_string()),
        }),
    }
}

pub(super) fn material_page(page: protocol::MaterialPage) -> api::MaterialPage {
    api::MaterialPage {
        scope: Some(scope_view(page.scope)),
        session_title: page.session_title,
        run_title: page.run_title,
        items: page
            .items
            .into_iter()
            .map(|item| api::MaterialMetadata {
                artifact_id: item.artifact_id.to_string(),
                entry_id: item.entry_id.to_string(),
                label: item.label,
                media_type: item.media_type,
                byte_length: item.byte_length,
            })
            .collect(),
        next_after_artifact_id: page.next_after.map(|id| id.to_string()),
    }
}
