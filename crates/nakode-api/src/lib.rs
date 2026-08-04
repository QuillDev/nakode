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
            "WatchWorkspace",
            "ReloadWorkspace",
            "CreateSession",
            "OpenSession",
            "ListSessions",
            "GetSession",
            "WatchSession",
            "SendPrompt",
            "EnqueuePrompt",
            "RemoveQueuedPrompt",
            "SteerTurn",
            "CancelTurn",
            "CancelSessionWork",
            "CompactContext",
            "ResolveInteraction",
            "ConfigureSessionTools",
            "SubmitExternalToolResult",
            "RunShell",
            "SelectModel",
            "SetProviderEnabled",
            "BeginProviderAuthentication",
            "SetProviderCredential",
            "ClearProviderCredential",
            "SaveAgent",
            "DeleteAgent",
            "UpdateSettings",
            "CheckAgentBrowser",
            "Delegate",
            "ListRuns",
            "GetRun",
            "WatchRun",
            "CancelRun",
            "GetTranscriptPage",
            "GetTranscriptBodyWindow",
            "GetRunTextWindow",
            "GetArtifact",
            "GetDiagnostics",
            "GetServerInfo",
        ];
        assert_eq!(methods, required.into_iter().map(str::to_owned).collect());
    }
}
