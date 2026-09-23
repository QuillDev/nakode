//! The same runtime path serves local SDK calls and authenticated remote gRPC reads.
use super::{NativeServerRuntime, Query, QueryResult, ServiceCapability, ServiceError};
use nakode_protocol::{ErrorCode, MaterialScope};

impl NativeServerRuntime {
    pub(super) fn handle_native_material(&self, request: crate::backend::NativeMaterialRequest) {
        let result = self
            .native_material(&request)
            .map_err(|error| error.message);
        let _ = request.respond.send(result);
    }

    fn native_material(
        &self,
        request: &crate::backend::NativeMaterialRequest,
    ) -> Result<QueryResult, ServiceError> {
        use crate::backend::NativeMaterialOperation;
        // A delegated tool invocation cannot promote itself to the primary parent's supervision
        // authority. It may inspect only its own exact run through this surface.
        if let Some(run) = &request.requester_run_id
            && (request.source.session_id.as_str() != request.owner_session_id
                || request
                    .source
                    .run_id
                    .as_ref()
                    .map(nakode_protocol::RunId::as_str)
                    != Some(run.as_str()))
        {
            return Err(refuse(
                ErrorCode::NotFound,
                "material source is outside this delegated run",
            ));
        }
        let scope = MaterialScope {
            parent_session_id: request.owner_session_id.clone().into(),
            source: request.source.clone(),
        };
        let query = match &request.operation {
            NativeMaterialOperation::List { after, limit } => Query::ListChildMaterials {
                scope,
                after: after.clone(),
                limit: *limit,
            },
            NativeMaterialOperation::Image { reference } => Query::GetChildMaterial {
                scope,
                image_reference: reference.clone(),
                transform: None,
            },
        };
        self.read_child_materials(query)
    }

    pub(super) fn read_child_materials(&self, query: Query) -> Result<QueryResult, ServiceError> {
        if !self
            .endpoint
            .capabilities()
            .supports(ServiceCapability::ChildMaterials)
            || !self
                .endpoint
                .capabilities()
                .supports(ServiceCapability::ArtifactTransfer)
        {
            return Err(refuse(
                ErrorCode::CapabilityUnsupported,
                "child material reads are unavailable",
            ));
        }
        let (Query::ListChildMaterials { scope, .. } | Query::GetChildMaterial { scope, .. }) =
            &query
        else {
            return Err(refuse(ErrorCode::InvalidRequest, "not a material query"));
        };
        validate_scope(scope)?;
        let store = crate::child_reports::ReportStore::open(&self.effects.persistence.database)?;
        // This also validates that the persisted parent is present and open, including own-run reads.
        let children = store.question_children(scope.parent_session_id.as_str())?;
        if scope.source.session_id != scope.parent_session_id
            && !children
                .iter()
                .any(|(id, _, _)| id == scope.source.session_id.as_str())
        {
            return Err(refuse(
                ErrorCode::NotFound,
                "authorized material source is unavailable",
            ));
        }
        // Closed children can be inspected as retained evidence; this does not reopen them.
        if let Some(engine) = self.core.engine_for(&scope.source.session_id) {
            crate::state::projection::materials::project(engine.state(), query)
        } else {
            self.read_retained_query(query)
        }
    }
}

fn validate_scope(scope: &MaterialScope) -> Result<(), ServiceError> {
    for id in std::iter::once(scope.parent_session_id.as_str())
        .chain(std::iter::once(scope.source.session_id.as_str()))
        .chain(
            scope
                .source
                .run_id
                .as_ref()
                .map(nakode_protocol::RunId::as_str),
        )
    {
        if id.is_empty() || id.len() > 200 {
            return Err(refuse(
                ErrorCode::InvalidRequest,
                "material identities must contain 1–200 bytes",
            ));
        }
    }
    Ok(())
}

fn refuse(code: ErrorCode, message: &str) -> ServiceError {
    ServiceError {
        code,
        message: message.to_owned(),
        retryable: false,
    }
}
