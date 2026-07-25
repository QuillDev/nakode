use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::backend::{ApprovalDecision, ApprovalKind};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationClass {
    ReadWorkspace,
    MutateWorkspace,
    ExecuteProcess,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalMode {
    Interactive,
    Unattended,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PermissionEnvelope {
    pub allowed_operations: BTreeSet<OperationClass>,
    pub approval_mode: ApprovalMode,
    pub child_runs: u16,
}

impl PermissionEnvelope {
    #[must_use]
    pub fn unattended_workspace() -> Self {
        Self {
            allowed_operations: [
                OperationClass::ReadWorkspace,
                OperationClass::MutateWorkspace,
                OperationClass::ExecuteProcess,
            ]
            .into_iter()
            .collect(),
            approval_mode: ApprovalMode::Unattended,
            child_runs: 0,
        }
    }

    #[must_use]
    pub fn read_only() -> Self {
        Self {
            allowed_operations: [OperationClass::ReadWorkspace].into_iter().collect(),
            approval_mode: ApprovalMode::Unattended,
            child_runs: 0,
        }
    }

    #[must_use]
    pub fn allows(&self, operation: OperationClass) -> bool {
        self.allowed_operations.contains(&operation)
    }

    #[must_use]
    pub fn provider_decision(&self, kind: ApprovalKind) -> ApprovalDecision {
        let allowed = match kind {
            ApprovalKind::Command => self
                .allowed_operations
                .contains(&OperationClass::ExecuteProcess),
            ApprovalKind::FileChange => self
                .allowed_operations
                .contains(&OperationClass::MutateWorkspace),
            ApprovalKind::Other => false,
        };
        if allowed && matches!(self.approval_mode, ApprovalMode::Unattended) {
            ApprovalDecision::AcceptOnce
        } else {
            ApprovalDecision::Decline
        }
    }
}

impl Default for PermissionEnvelope {
    fn default() -> Self {
        Self::unattended_workspace()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_only_policy_fails_closed_for_processes_and_mutation() {
        let envelope = PermissionEnvelope::read_only();
        assert!(envelope.allows(OperationClass::ReadWorkspace));
        assert!(!envelope.allows(OperationClass::MutateWorkspace));
        assert!(!envelope.allows(OperationClass::ExecuteProcess));
        assert_eq!(
            envelope.provider_decision(ApprovalKind::Command),
            ApprovalDecision::Decline
        );
    }

    #[test]
    fn unattended_policy_grants_each_request_once() {
        let envelope = PermissionEnvelope::unattended_workspace();
        assert_eq!(
            envelope.provider_decision(ApprovalKind::Command),
            ApprovalDecision::AcceptOnce
        );
        assert_eq!(
            envelope.provider_decision(ApprovalKind::Other),
            ApprovalDecision::Decline
        );
    }
}
