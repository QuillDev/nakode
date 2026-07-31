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
    use std::{collections::BTreeSet, process::Command};

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

    #[test]
    fn stock_protoc_generates_a_typed_non_rust_model() {
        let output = tempfile::tempdir().expect("create generation directory");
        let proto_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../proto");
        let schema = proto_root.join("nakode/v1/nakode.proto");
        let status = Command::new(protoc_bin_vendored::protoc_bin_path().expect("vendored protoc"))
            .arg(format!("--proto_path={}", proto_root.display()))
            .arg(format!(
                "--proto_path={}",
                protoc_bin_vendored::include_path()
                    .expect("vendored include path")
                    .display()
            ))
            .arg(format!("--python_out={}", output.path().display()))
            .arg(format!("--pyi_out={}", output.path().display()))
            .arg(schema)
            .status()
            .expect("run Python generator");
        assert!(status.success(), "Python code generation must succeed");

        let type_stubs = std::fs::read_to_string(output.path().join("nakode/v1/nakode_pb2.pyi"))
            .expect("read generated Python type stubs");
        assert!(type_stubs.contains("class SessionState"));
        assert!(type_stubs.contains("class CreateSessionRequest"));
        assert!(type_stubs.contains("class WorkspaceSnapshot"));
    }
}
