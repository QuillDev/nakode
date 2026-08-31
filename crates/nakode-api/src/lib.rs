//! Generated Nakode API v1 types and gRPC service stubs.
//!
//! The `.proto` contract is the authority. This crate is one generated SDK
//! target; other languages generate from the same source without depending on
//! Nakode's Rust implementation crates.

pub mod v1 {
    tonic::include_proto!("nakode.v1");

    /// Compiled API descriptor used by SDK generation and conformance tests.
    pub const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("nakode-v1-descriptor");
}

/// Maximum encoded request or response accepted by generated Nakode clients.
pub const MAX_API_MESSAGE_BYTES: usize = 32 * 1024 * 1024;

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use prost::Message;
    use prost_types::FileDescriptorSet;

    use super::v1::FILE_DESCRIPTOR_SET;

    #[test]
    fn queue_redirect_reservation_round_trips_on_field_five() {
        let item = super::v1::QueueItem {
            id: "prompt-1".to_owned(),
            summary: "reserved follow-up".to_owned(),
            attachment_count: 1,
            text: "run next".to_owned(),
            redirecting: true,
        };
        let encoded = item.encode_to_vec();
        let decoded = super::v1::QueueItem::decode(encoded.as_slice())
            .expect("generated queue item must decode");

        assert_eq!(decoded, item);
        assert!(decoded.redirecting);
        assert!(encoded.windows(2).any(|field| field == [0x28, 0x01]));
    }

    #[test]
    fn server_info_build_revision_is_additive_and_presence_aware() {
        let legacy = super::v1::ServerInfo {
            server_version: "0.3.0".to_owned(),
            api_version: "nakode.v1".to_owned(),
            capabilities: Vec::new(),
            server_id: "server-1".to_owned(),
            build_revision: None,
        };
        let legacy_wire = legacy.encode_to_vec();
        assert_eq!(
            super::v1::ServerInfo::decode(legacy_wire.as_slice())
                .expect("legacy server info must decode")
                .build_revision,
            None
        );

        let current = super::v1::ServerInfo {
            build_revision: Some("0123456789abcdef0123456789abcdef01234567".to_owned()),
            ..legacy
        };
        let current_wire = current.encode_to_vec();
        assert_eq!(
            super::v1::ServerInfo::decode(current_wire.as_slice())
                .expect("current server info must decode")
                .build_revision
                .as_deref(),
            Some("0123456789abcdef0123456789abcdef01234567")
        );
    }

    #[test]
    fn activation_build_revisions_are_additive_and_presence_aware() {
        let legacy_executable = super::v1::ActivationExecutableIdentity::default();
        let legacy_running = super::v1::ActivationRunningService::default();
        assert_eq!(legacy_executable.build_revision, None);
        assert_eq!(legacy_running.build_revision, None);

        let revision = "0123456789abcdef0123456789abcdef01234567";
        let executable = super::v1::ActivationExecutableIdentity {
            build_revision: Some(revision.to_owned()),
            ..legacy_executable
        };
        let running = super::v1::ActivationRunningService {
            executable: Some(executable.clone()),
            build_revision: Some(revision.to_owned()),
            ..legacy_running
        };
        let executable_wire = executable.encode_to_vec();
        let running_wire = running.encode_to_vec();

        assert_eq!(
            super::v1::ActivationExecutableIdentity::decode(executable_wire.as_slice())
                .expect("activation executable must decode")
                .build_revision
                .as_deref(),
            Some(revision)
        );
        assert_eq!(
            super::v1::ActivationRunningService::decode(running_wire.as_slice())
                .expect("activation running service must decode")
                .build_revision
                .as_deref(),
            Some(revision)
        );
    }

    #[test]
    fn public_descriptor_exposes_the_complete_frontend_edge_inventory() {
        let descriptor = FileDescriptorSet::decode(FILE_DESCRIPTOR_SET)
            .expect("generated descriptor must decode");
        let methods = descriptor
            .file
            .iter()
            .flat_map(|file| &file.service)
            .flat_map(|service| &service.method)
            .filter_map(|method| method.name.clone())
            .collect::<BTreeSet<_>>();
        let required = [
            "GetWorkspace",
            "InspectWorkspacePath",
            "WatchWorkspace",
            "ReloadWorkspace",
            "GetSoul",
            "SaveSoul",
            "GetMcpManagement",
            "SaveMcpServer",
            "DeleteMcpServer",
            "SetMcpServerEnabled",
            "RefreshMcpServer",
            "SetMcpServerCredential",
            "ClearMcpServerCredential",
            "SetMcpServerGrants",
            "CreateSession",
            "OpenSession",
            "SetSessionBridgeLifecycle",
            "SetWorkspaceBridgeLifecycle",
            "BindSessionBridgeThread",
            "ClearSessionBridgeThread",
            "PrepareBridgeDelivery",
            "CompleteBridgeDeliveryPart",
            "FinalizeBridgeDelivery",
            "SetBridgeLiveMessage",
            "ContinueSessionFromBridge",
            "ListSessions",
            "DeleteSession",
            "GetSession",
            "WatchSession",
            "SendPrompt",
            "EnqueuePrompt",
            "RemoveQueuedPrompt",
            "SteerQueuedPrompt",
            "SteerTurn",
            "CancelTurn",
            "CancelSessionWork",
            "CompactContext",
            "ResolveInteraction",
            "SetSessionCodeMode",
            "ConfigureSessionTools",
            "SubmitExternalToolResult",
            "RunShell",
            "SelectModel",
            "SetProviderModelFilter",
            "AddProviderAccount",
            "BeginProviderAccountAuthentication",
            "SetProviderAccountCredential",
            "ClearProviderAccountCredential",
            "ReloadProviderAccount",
            "SetProviderAccountLabel",
            "SetProviderAccountEnabled",
            "SetProviderAccountDefault",
            "RemoveProviderAccount",
            "ListSkills",
            "SetSkillEnabled",
            "PruneSkill",
            "SetProviderEnabled",
            "BeginProviderAuthentication",
            "SetProviderCredential",
            "ClearProviderCredential",
            "ReloadProvider",
            "SaveAgent",
            "DeleteAgent",
            "UpdateSettings",
            "CheckAgentBrowser",
            "Delegate",
            "ListRuns",
            "GetRun",
            "WatchRun",
            "CancelRun",
            "ContinueRun",
            "GetTranscriptPage",
            "GetTranscriptBodyWindow",
            "GetRunTextWindow",
            "GetArtifact",
            "GetDiagnostics",
            "GetInvocationSummary",
            "GetInvocationTimeline",
            "GetServerInfo",
            "GetActivationStatus",
            "WatchActivationStatus",
            "ForceActivationRecheck",
            "ForceActivate",
        ];
        assert_eq!(methods, required.into_iter().map(str::to_owned).collect());

        let build_revision = descriptor
            .file
            .iter()
            .flat_map(|file| &file.message_type)
            .find(|message| message.name.as_deref() == Some("ServerInfo"))
            .and_then(|message| {
                message
                    .field
                    .iter()
                    .find(|field| field.name.as_deref() == Some("build_revision"))
            })
            .expect("ServerInfo.build_revision must remain public");
        assert_eq!(build_revision.number, Some(5));
        assert_eq!(build_revision.proto3_optional, Some(true));
    }
}
