use std::{
    collections::BTreeMap,
    fmt::Write as _,
    path::{Path, PathBuf},
    sync::Mutex,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use directories::ProjectDirs;
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

pub use crate::backend::{CODEX_PROVIDER, DEVIN_PROVIDER};
use crate::{
    backend::{ModelInfo, ModelOptions, TurnOutcome},
    credential::CredentialMetadata,
    domain_transcript::{EntryKind, EntryStatus, TranscriptEntry},
    mcp::McpServerRecord,
    memory::{MemoryBackend, MemoryConfig},
    settings::TerminalImageMode,
    skill::SkillPreference,
    vision::VisionConfig,
    web::{WebBackend, WebConfig},
};

const PROVIDER_CATALOG_PATH: &str = "config/providers.toml";
const PROVIDER_CATALOG: &str = include_str!("../config/providers.toml");
const MAX_BRIDGE_REPLAY_EVENTS_PER_SESSION: usize = 4 * 1_024;
const INVOCATION_TELEMETRY_MIGRATION_VERSION: i64 = 1;
const SUBAGENT_TRANSCRIPT_BOUNDARY_MIGRATION_VERSION: i64 = 2;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InvocationRecord {
    pub invocation_key: String,
    pub kind: nakode_protocol::InvocationKind,
    pub identity: String,
    pub display_label: String,
    pub occurred_at_ms: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderCatalog {
    providers: Vec<ProviderCatalogEntry>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderCatalogEntry {
    slug: String,
    display_name: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistedTurnConfiguration {
    pub id: String,
    pub model: Option<String>,
    pub options: ModelOptions,
    pub outcome: TurnOutcome,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistedOwnerPrompt {
    pub prompt_id: String,
    pub raw_text: String,
    /// Immutable transport provenance captured when the owner prompt is accepted. This remains
    /// independent of provider item/turn identity so synthesized transcript rows retain provenance.
    pub source_transport: Option<String>,
    /// True until a normal owner turn reaches the backend started boundary. Local command acceptance
    /// alone does not clear the replay fence. Bridge inbox records use their own durable pending state
    /// and therefore never set this flag.
    pub dispatch_pending: bool,
}

pub const PENDING_PROVIDER_SESSION_PREFIX: &str = "nakode:pending-session:";

#[must_use]
pub fn pending_provider_session_id(session_id: &str) -> String {
    format!("{PENDING_PROVIDER_SESSION_PREFIX}{session_id}")
}

#[must_use]
pub fn is_pending_provider_session_id(provider_session_id: &str) -> bool {
    provider_session_id.starts_with(PENDING_PROVIDER_SESSION_PREFIX)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionRecord {
    pub id: String,
    pub provider: String,
    /// Stable credential account pinned to this provider-native session.
    pub account_id: Option<String>,
    pub provider_session_id: String,
    /// Logical workspace/service ownership path.
    pub workspace: String,
    /// Canonical filesystem/provider process root. Legacy rows inherit `workspace`.
    pub working_directory: String,
    pub title: String,
    pub model: Option<String>,
    /// Authoritative session-local configuration for the next owner turn.
    pub model_options: ModelOptions,
    /// Immutable attribution for the latest terminal owner turn.
    pub last_turn: Option<PersistedTurnConfiguration>,
    /// Immutable terminal owner turns retained for historical transcript attribution.
    pub owner_turns: Vec<PersistedTurnConfiguration>,
    /// Raw accepted owner prompts, ordered by durable acceptance for role-safe transcript replay.
    pub owner_prompts: Vec<PersistedOwnerPrompt>,
    /// Unix epoch seconds at initial persistence; converted exactly once at API projection.
    pub created_at: i64,
    /// Unix epoch seconds at the latest persistence touch; converted exactly once at API projection.
    pub updated_at: i64,
    /// Latest accepted or terminal owner-turn boundary. Generic persistence touches never change it.
    pub last_owner_activity_at: Option<i64>,
    /// Whether the next owner turn exposes only the synthesized Code Mode tool.
    pub code_mode: bool,
    /// Immutable stable skill identities authorized for this logical session. `None` is a legacy
    /// row and defaults to all installed skills once; `Some([])` intentionally authorizes none.
    pub enabled_skill_ids: Option<Vec<String>>,
    /// Additional provider-native resources owned by delegated runs beneath this session.
    pub owned_provider_sessions: Vec<(String, String)>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BridgeProjectionRecord {
    pub kind: nakode_protocol::BridgeProjectionKind,
    pub turn_id: String,
}

fn default_bridge_projection_kind() -> nakode_protocol::BridgeProjectionKind {
    nakode_protocol::BridgeProjectionKind::Assistant
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BridgeDeliveryRecord {
    #[serde(default = "default_bridge_projection_kind")]
    pub projection_kind: nakode_protocol::BridgeProjectionKind,
    pub turn_id: String,
    #[serde(default)]
    pub previous_projection: Option<BridgeProjectionRecord>,
    pub body_sha256: String,
    pub part_count: u64,
    pub completed_parts: u64,
    pub last_external_message_id: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BridgeInboundTurnOriginRecord {
    pub turn_id: String,
    pub transport: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BridgePendingInboundRecord {
    pub external_event_id: String,
    pub source_message_id: String,
    /// Stable provider-neutral turn client id reused by crash recovery.
    pub client_prompt_id: String,
    pub text: String,
    pub attachments: Vec<crate::backend::PromptAttachment>,
}

/// Durable provider-neutral thread pairing and delivery state.
///
/// Records deliberately outlive authoritative session deletion so a transport can finish
/// best-effort archival after the logical session and its transcript are gone.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionBridgeRecord {
    pub session_id: String,
    pub workspace: String,
    pub kind: nakode_protocol::OrchestratorKind,
    pub lifecycle: nakode_protocol::BridgeLifecycle,
    pub display_title: String,
    pub revision: u64,
    pub transport: Option<String>,
    pub external_parent_id: Option<String>,
    pub external_thread_id: Option<String>,
    pub last_projected: Option<BridgeProjectionRecord>,
    pub delivery: Option<BridgeDeliveryRecord>,
    pub live_turn_id: Option<String>,
    pub live_external_message_id: Option<String>,
    pub active_source_message_id: Option<String>,
    pub recent_inbound_event_ids: Vec<String>,
    /// Durable at-least-once inbox item, cleared only after the backend acknowledges this client id.
    pub pending_inbound: Option<BridgePendingInboundRecord>,
    /// Durable source provenance keyed by provider turn identity for resume-time user history.
    pub inbound_turn_origins: Vec<BridgeInboundTurnOriginRecord>,
    pub updated_at_ms: i64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SessionPurgeReport {
    pub sessions: usize,
    pub orchestration_runs: usize,
    pub agent_turns: usize,
    pub native_runtime_sessions: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderAccountRecord {
    pub account_id: String,
    pub provider: String,
    pub label: String,
    pub enabled: bool,
    pub is_default: bool,
    pub identity: Option<String>,
    pub credential: Option<CredentialMetadata>,
    pub created_at: i64,
    pub updated_at: i64,
    pub routing_mode: nakode_protocol::ProviderAccountRoutingMode,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderRecord {
    pub provider: String,
    pub display_name: String,
    pub enabled: bool,
    pub credential: Option<CredentialMetadata>,
    pub accounts: Vec<ProviderAccountRecord>,
    pub model_filter_enabled: bool,
    pub selected_model_ids: Vec<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SubagentStatus {
    Starting,
    Working,
    Completed,
    Partial,
    Interrupted,
    Failed,
}

impl SubagentStatus {
    const fn database_value(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Working => "working",
            Self::Completed => "completed",
            Self::Partial => "partial",
            Self::Interrupted => "interrupted",
            Self::Failed => "failed",
        }
    }

    fn from_database(value: &str) -> Result<Self, SessionError> {
        match value {
            "starting" => Ok(Self::Starting),
            "working" => Ok(Self::Working),
            "completed" => Ok(Self::Completed),
            "partial" => Ok(Self::Partial),
            "interrupted" => Ok(Self::Interrupted),
            "failed" => Ok(Self::Failed),
            _ => Err(SessionError::InvalidStoredValue {
                field: "orchestration_runs.status",
                value: value.to_owned(),
            }),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct SalvagedEvidence {
    pub entry_id: String,
    pub title: String,
    pub body: String,
    pub truncated: bool,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ContinuationProposition {
    pub verified_findings: Vec<String>,
    pub unresolved_boundary: String,
    pub why_it_matters: String,
    pub recommended_archetype: String,
    pub follow_up_objective: String,
    pub inherited_evidence: Vec<String>,
    pub can_proceed_independently: bool,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct SubagentSalvage {
    pub terminal_reason: String,
    pub original_objective: String,
    pub completed_work: Vec<String>,
    pub verified_evidence: Vec<SalvagedEvidence>,
    pub last_successful_evidence: Option<SalvagedEvidence>,
    pub unresolved_questions: Vec<String>,
    pub continuation: ContinuationProposition,
    pub can_resume: bool,
    pub redacted: bool,
    pub truncated: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SubagentObservability {
    pub parent_run_id: Option<String>,
    /// Stable source invocation identity in the parent transcript.
    pub invocation_turn_id: Option<String>,
    pub invocation_call_id: Option<String>,
    /// Owner transcript entry that originated the parent turn, when known.
    pub originating_owner_entry_id: Option<String>,
    pub archetype_purpose: String,
    /// Exact delegated archetype definition used for this run, serialized at acceptance time.
    pub policy_json: String,
    pub remaining_delegation_depth: u32,
    pub started_at_ms: u64,
    pub ended_at_ms: Option<u64>,
    pub termination_kind: Option<String>,
    pub termination_detail: Option<String>,
    pub objective_mismatch_handoff: Option<String>,
    pub salvage: Option<SubagentSalvage>,
    pub continued_from_run_id: Option<String>,
    pub continued_by_run_id: Option<String>,
    pub continuation_depth: u32,
    pub additional_turns: Option<u32>,
    pub inherited_evidence: Vec<SalvagedEvidence>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubagentRecord {
    pub parent_session_id: String,
    pub id: String,
    pub agent: String,
    pub provider: String,
    pub model: Option<String>,
    pub provider_session_id: Option<String>,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cached_input_tokens: u64,
    pub cache_write_tokens: u64,
    pub objective: String,
    pub status: SubagentStatus,
    pub latest_activity: String,
    pub observability: SubagentObservability,
    pub transcript: Vec<TranscriptEntry>,
    pub transcript_has_earlier: bool,
}

#[derive(Debug, Error)]
pub enum SessionError {
    #[error("could not determine Nakode's application-data directory")]
    MissingDataDirectory,
    #[error("failed to create session database directory {path}: {source}")]
    CreateDirectory {
        path: String,
        source: std::io::Error,
    },
    #[error("failed to protect credential-bearing session storage {path}: {source}")]
    ProtectStorage {
        path: String,
        source: std::io::Error,
    },
    #[error("session database error: {0}")]
    Database(#[from] rusqlite::Error),
    #[error("session {0:?} is ambiguous; use a longer id")]
    Ambiguous(String),
    #[error("session {0:?} was not found")]
    SessionNotFound(String),
    #[error("provider {0:?} was not found")]
    ProviderNotFound(String),
    #[error("invalid persisted value for {field}: {value:?}")]
    InvalidStoredValue { field: &'static str, value: String },
    #[error("invalid persisted JSON in {field}: {source}")]
    InvalidStoredJson {
        field: &'static str,
        source: serde_json::Error,
    },
    #[error("provider account {account_id} was not found for {provider}")]
    ProviderAccountNotFound {
        provider: String,
        account_id: String,
    },
    #[error("provider account {account_id} is pinned to persisted sessions")]
    ProviderAccountInUse { account_id: String },
    #[error("session {session_id} is already pinned to provider account {account_id}")]
    ProviderAccountAffinityConflict {
        session_id: String,
        account_id: String,
    },
    #[error("owner prompt {prompt_id:?} for session {session_id:?} was reused with different text")]
    OwnerPromptConflict {
        session_id: String,
        prompt_id: String,
    },
    #[error(
        "session bridge {session_id:?} already has a different unresolved inbound prompt {prompt_id:?}"
    )]
    BridgePendingInboundConflict {
        session_id: String,
        prompt_id: String,
    },
    #[error("session bridge {session_id:?} revision {revision} is stale")]
    BridgeRevisionConflict { session_id: String, revision: u64 },
    #[error("provider {0} has no configured credentials")]
    MissingProviderCredential(String),
    #[error("invalid provider catalog {path}: {source}")]
    InvalidProviderCatalog {
        path: &'static str,
        source: toml::de::Error,
    },
}

pub trait SessionRepository: Send + Sync {
    /// Returns every persisted skill choice for one client profile, including unavailable skills.
    ///
    /// # Errors
    /// Returns an error when persistence cannot be queried or decoded.
    fn list_skill_preferences(
        &self,
        _profile_id: &str,
    ) -> Result<Vec<SkillPreference>, SessionError> {
        Ok(Vec::new())
    }

    /// Returns all persisted profile skill choices for service-start cache hydration.
    ///
    /// # Errors
    /// Returns an error when persistence cannot be queried or decoded.
    fn list_all_skill_preferences(&self) -> Result<Vec<SkillPreference>, SessionError> {
        Ok(Vec::new())
    }

    /// Upserts one stable skill identity for one client profile.
    ///
    /// # Errors
    /// Returns an error when persistence cannot be updated.
    fn set_skill_preference(&self, _preference: &SkillPreference) -> Result<(), SessionError> {
        Ok(())
    }

    /// Upserts a set of profile skill choices as one durable reconciliation where supported.
    ///
    /// # Errors
    /// Returns an error when any preference cannot be updated.
    fn set_skill_preferences(&self, preferences: &[SkillPreference]) -> Result<(), SessionError> {
        for preference in preferences {
            self.set_skill_preference(preference)?;
        }
        Ok(())
    }

    /// Removes one retained unavailable skill preference for one client profile.
    ///
    /// Invocation telemetry is installation-wide and has no profile ownership key, so profile-local
    /// pruning must preserve it.
    ///
    /// # Errors
    /// Returns an error when persistence cannot be updated.
    fn prune_unavailable_skill(
        &self,
        _profile_id: &str,
        _skill_id: &str,
    ) -> Result<crate::skill::SkillPruneReport, SessionError> {
        Ok(crate::skill::SkillPruneReport::default())
    }

    /// Returns the durable profile governing one logical session, when profile-managed.
    ///
    /// # Errors
    /// Returns an error when persistence cannot be queried.
    fn session_skill_profile(&self, _session_id: &str) -> Result<Option<String>, SessionError> {
        Ok(None)
    }

    /// Durably associates one logical session with its governing profile.
    ///
    /// # Errors
    /// Returns an error when persistence cannot be updated or an existing association differs.
    fn bind_session_skill_profile(
        &self,
        _session_id: &str,
        _profile_id: &str,
    ) -> Result<(), SessionError> {
        Ok(())
    }

    /// Lists bridge state for logical sessions in one canonical workspace.
    ///
    /// # Errors
    /// Returns an error when persistence cannot be queried or decoded.
    fn list_session_bridges(
        &self,
        _workspace: &str,
    ) -> Result<Vec<SessionBridgeRecord>, SessionError> {
        Ok(Vec::new())
    }
    /// Lists bridge state across the installation-wide session inventory.
    ///
    /// # Errors
    /// Returns an error when persistence cannot be queried or decoded.
    fn list_session_bridges_all(&self) -> Result<Vec<SessionBridgeRecord>, SessionError> {
        Ok(Vec::new())
    }
    /// Replaces one server-serialized bridge record. Implementations must persist atomically.
    ///
    /// # Errors
    /// Returns an error when the record cannot be encoded or persisted.
    fn save_session_bridge(&self, _bridge: &SessionBridgeRecord) -> Result<(), SessionError> {
        Ok(())
    }
    /// Replaces bridge records in one repository transaction. The default preserves compatibility
    /// for non-SQLite repositories; durable implementations should override this atomically.
    ///
    /// # Errors
    /// Returns an error when any bridge record cannot be persisted.
    fn save_session_bridges(&self, bridges: &[SessionBridgeRecord]) -> Result<(), SessionError> {
        for bridge in bridges {
            self.save_session_bridge(bridge)?;
        }
        Ok(())
    }
    /// Atomically replaces bridge records and claims one newly consumed external event.
    ///
    /// # Errors
    /// Returns an error when the transaction fails or the event identity was already claimed.
    fn save_session_bridges_with_inbound_event(
        &self,
        bridges: &[SessionBridgeRecord],
        _session_id: &str,
        _external_event_id: &str,
        _disposition: nakode_protocol::BridgeContinuationDisposition,
    ) -> Result<(), SessionError> {
        self.save_session_bridges(bridges)
    }
    /// Looks up the durable, never-expiring inbound replay result without hydrating the ledger.
    /// `None` means the external identity has never been claimed; `Duplicate` represents an older
    /// identity-only row whose original Accepted/Busy status is unavailable.
    ///
    /// # Errors
    /// Returns an error when the replay ledger cannot be queried or decoded.
    fn find_session_bridge_inbound_event(
        &self,
        _session_id: &str,
        _external_event_id: &str,
    ) -> Result<Option<nakode_protocol::BridgeContinuationDisposition>, SessionError> {
        Ok(None)
    }
    /// Lists the most recently used sessions in a workspace.
    ///
    /// # Errors
    /// Returns an error when persistence cannot be queried.
    fn list_recent(
        &self,
        workspace: &str,
        limit: usize,
    ) -> Result<Vec<SessionRecord>, SessionError>;
    /// Lists the most recently used sessions across the installation.
    ///
    /// A single Nakode service owns every workspace. Workspace paths remain per-session access
    /// roots rather than persistence or process partitions.
    ///
    /// # Errors
    /// Returns an error when persistence cannot be queried.
    fn list_recent_all(&self) -> Result<Vec<SessionRecord>, SessionError>;
    /// Finds a session by its full id or unambiguous prefix.
    ///
    /// # Errors
    /// Returns an error when persistence cannot be queried or the prefix is ambiguous.
    fn find(&self, id: &str) -> Result<Option<SessionRecord>, SessionError>;
    /// Creates a logical session record.
    ///
    /// # Errors
    /// Returns an error when the record cannot be persisted.
    fn create(
        &self,
        provider: &str,
        provider_session_id: &str,
        workspace: &str,
        title: &str,
        model: Option<&str>,
    ) -> Result<SessionRecord, SessionError> {
        self.create_with_id(
            &Uuid::now_v7().to_string(),
            provider,
            provider_session_id,
            workspace,
            workspace,
            title,
            model,
            &ModelOptions::default(),
            None,
        )
    }
    /// Creates a logical session using an identity assigned before provider work begins.
    ///
    /// # Errors
    /// Returns an error when the record cannot be persisted.
    #[allow(clippy::too_many_arguments)]
    fn create_with_id(
        &self,
        id: &str,
        provider: &str,
        provider_session_id: &str,
        workspace: &str,
        working_directory: &str,
        title: &str,
        model: Option<&str>,
        options: &ModelOptions,
        enabled_skill_ids: Option<&[String]>,
    ) -> Result<SessionRecord, SessionError>;

    /// Creates a session with its credential affinity in the same durable write.
    ///
    /// # Errors
    /// Returns an error when the account is invalid or the session cannot be persisted.
    #[allow(clippy::too_many_arguments)]
    fn create_with_account_id(
        &self,
        id: &str,
        provider: &str,
        account_id: Option<&str>,
        provider_session_id: &str,
        workspace: &str,
        working_directory: &str,
        title: &str,
        model: Option<&str>,
        options: &ModelOptions,
        enabled_skill_ids: Option<&[String]>,
    ) -> Result<SessionRecord, SessionError> {
        let mut record = self.create_with_id(
            id,
            provider,
            provider_session_id,
            workspace,
            working_directory,
            title,
            model,
            options,
            enabled_skill_ids,
        )?;
        self.set_session_account(id, account_id)?;
        record.account_id = account_id.map(str::to_owned);
        Ok(record)
    }

    /// Creates a session with credential affinity, optional governing skill profile, and optional
    /// Code Mode state as one repository operation. Transactional implementations must override
    /// this default so no partially managed row becomes visible.
    ///
    /// # Errors
    /// Returns an error when the account, session, profile association, or Code Mode state cannot
    /// be persisted.
    #[allow(clippy::too_many_arguments)]
    fn create_with_account_id_and_skill_profile(
        &self,
        id: &str,
        provider: &str,
        account_id: Option<&str>,
        provider_session_id: &str,
        workspace: &str,
        working_directory: &str,
        title: &str,
        model: Option<&str>,
        options: &ModelOptions,
        enabled_skill_ids: Option<&[String]>,
        profile_id: Option<&str>,
        code_mode: Option<bool>,
        owner_prompt: Option<&PersistedOwnerPrompt>,
    ) -> Result<SessionRecord, SessionError> {
        let record = self.create_with_account_id(
            id,
            provider,
            account_id,
            provider_session_id,
            workspace,
            working_directory,
            title,
            model,
            options,
            enabled_skill_ids,
        )?;
        if let Some(profile_id) = profile_id {
            self.bind_session_skill_profile(&record.id, profile_id)?;
        }
        if let Some(code_mode) = code_mode {
            self.set_session_code_mode(&record.id, code_mode)?;
        }
        if let Some(prompt) = owner_prompt {
            self.record_owner_prompt(&record.id, prompt)?;
        }
        Ok(record)
    }

    /// Binds or validates the account used by one persisted session.
    ///
    /// # Errors
    /// Returns an error when the session is absent, already bound differently, or cannot be updated.
    fn set_session_account(
        &self,
        _id: &str,
        _account_id: Option<&str>,
    ) -> Result<(), SessionError> {
        Ok(())
    }

    /// Persists the model-facing Code Mode selection for the next owner turn.
    ///
    /// # Errors
    /// Returns an error when the session is unknown or cannot be updated.
    fn set_session_code_mode(&self, _id: &str, _enabled: bool) -> Result<(), SessionError> {
        Ok(())
    }

    /// Binds a legacy logical session to the stable skill identities resolved at first open.
    ///
    /// # Errors
    /// Returns an error when the session is unknown or the snapshot cannot be persisted.
    fn set_session_skill_snapshot(
        &self,
        _id: &str,
        _enabled_skill_ids: &[String],
    ) -> Result<(), SessionError> {
        Ok(())
    }

    /// Atomically replaces one logical session's primary provider-native resource while retaining
    /// the previous primary as owned history for restart cleanup and deletion.
    ///
    /// # Errors
    /// Returns an error when the logical session is unknown or persistence cannot be updated.
    #[allow(clippy::too_many_arguments)]
    fn transition_primary(
        &self,
        id: &str,
        provider: &str,
        provider_session_id: &str,
        model: Option<&str>,
        options: &ModelOptions,
    ) -> Result<(), SessionError>;

    #[allow(clippy::too_many_arguments)]
    /// Atomically transitions provider-native identity while preserving account affinity.
    ///
    /// # Errors
    /// Returns an error when the account conflicts with durable affinity or persistence fails.
    fn transition_primary_with_account(
        &self,
        id: &str,
        provider: &str,
        account_id: Option<&str>,
        provider_session_id: &str,
        model: Option<&str>,
        options: &ModelOptions,
    ) -> Result<(), SessionError> {
        self.transition_primary(id, provider, provider_session_id, model, options)?;
        self.set_session_account(id, account_id)
    }
    /// Marks a session as recently used.
    ///
    /// # Errors
    /// Returns an error when persistence cannot be updated.
    fn touch(&self, id: &str) -> Result<(), SessionError>;
    /// Deletes a logical session and every record persisted beneath it.
    ///
    /// Takes the session row, its runs and their turns (by cascade), and the native runtime history
    /// keyed by the provider session it resolves to. That last table carries no foreign key back to
    /// `sessions`, so it is deleted explicitly — a cascade cannot reach it, and it is where the bulk of
    /// a session's bytes actually live. Any retained orchestrator bridge is archived in the same
    /// transaction so a failed delete cannot partially settle its lifecycle.
    ///
    /// Deletion is idempotent when the logical session is already absent. An independently retained
    /// bridge with that exact session id is still archived.
    ///
    /// # Errors
    /// Returns an error when persistence cannot be queried or the transaction cannot be committed.
    fn delete(&self, id: &str) -> Result<(), SessionError>;
    /// Deletes every logical session and every session-scoped persistence row.
    ///
    /// Unlike repeated single-session deletion, this also removes orphaned native runtime histories
    /// left by dead or partially initialized sessions. Provider credentials, provider/model
    /// preferences, and global add-on configuration are outside this boundary and remain untouched.
    ///
    /// # Errors
    /// Returns an error when the atomic purge transaction cannot be committed.
    fn purge_all(&self) -> Result<SessionPurgeReport, SessionError>;
    /// Updates the model associated with a session.
    ///
    /// # Errors
    /// Returns an error when persistence cannot be updated.
    fn update_model(
        &self,
        id: &str,
        model: Option<&str>,
        options: &ModelOptions,
    ) -> Result<(), SessionError>;
    /// Records an accepted owner activity without allowing an older observation to move it backward.
    ///
    /// # Errors
    /// Returns an error when persistence cannot be updated.
    fn record_owner_activity(&self, id: &str) -> Result<(), SessionError>;
    /// Idempotently persists the raw accepted owner text before provider dispatch.
    ///
    /// # Errors
    /// Returns an error when the durable transcript authority cannot be updated.
    fn record_owner_prompt(
        &self,
        session_id: &str,
        prompt: &PersistedOwnerPrompt,
    ) -> Result<(), SessionError>;
    /// Marks a durably accepted normal owner prompt as provider-acknowledged.
    ///
    /// # Errors
    /// Returns an error when the acknowledgement cannot be persisted.
    fn acknowledge_owner_prompt_dispatch(
        &self,
        session_id: &str,
        prompt_id: &str,
    ) -> Result<(), SessionError>;
    /// Atomically persists accepted owner rows with every bridge checkpoint required before dispatch.
    ///
    /// # Errors
    /// Returns an error without committing any owner or bridge row when one checkpoint fails.
    fn save_prompt_dispatch_checkpoint(
        &self,
        owner_prompts: &[(String, PersistedOwnerPrompt)],
        bridges: &[SessionBridgeRecord],
        inbound_event: Option<(&str, &str, nakode_protocol::BridgeContinuationDisposition)>,
    ) -> Result<(), SessionError>;
    /// Persists immutable attribution for the latest terminal owner turn and its activity boundary.
    ///
    /// # Errors
    /// Returns an error when persistence cannot be updated.
    fn update_last_turn(
        &self,
        id: &str,
        turn: &PersistedTurnConfiguration,
    ) -> Result<(), SessionError>;
    /// Lists cached models for a provider.
    ///
    /// # Errors
    /// Returns an error when persistence cannot be queried.
    fn list_models(&self, provider: &str) -> Result<Vec<ModelInfo>, SessionError>;
    /// Replaces the cached model catalog for a provider.
    ///
    /// # Errors
    /// Returns an error when the transaction cannot be committed.
    fn replace_models(&self, provider: &str, models: &[ModelInfo]) -> Result<(), SessionError>;
    /// Sets the model used by default for new sessions on a provider.
    ///
    /// # Errors
    /// Returns an error when the preference cannot be persisted.
    fn set_default_model(&self, provider: &str, model: &str) -> Result<(), SessionError>;
    /// Lists model-specific inference options saved through `/models`.
    ///
    /// # Errors
    /// Returns an error when preference storage cannot be read.
    fn list_model_options(
        &self,
        provider: &str,
    ) -> Result<Vec<(String, ModelOptions)>, SessionError>;
    /// Saves model-specific inference options selected through `/models`.
    ///
    /// # Errors
    /// Returns an error when preference storage cannot be updated.
    fn save_model_options(
        &self,
        provider: &str,
        model: &str,
        options: &ModelOptions,
    ) -> Result<(), SessionError>;
    /// Saves a Nakode-owned MCP server definition and its redacted runtime snapshot.
    ///
    /// # Errors
    /// Returns an error when the definition cannot be encoded or persisted.
    fn save_mcp_server(&self, server: &McpServerRecord) -> Result<(), SessionError>;
    /// Lists MCP servers scoped to one workspace.
    ///
    /// # Errors
    /// Returns an error when persistence cannot be queried or decoded.
    fn list_mcp_servers(&self, workspace: &str) -> Result<Vec<McpServerRecord>, SessionError>;
    /// Deletes one MCP server, grants, discoveries, audits, and credential metadata reference.
    ///
    /// # Errors
    /// Returns an error when persistence cannot be updated.
    fn delete_mcp_server(&self, workspace: &str, server_id: &str) -> Result<(), SessionError>;
    /// Appends a bounded invocation audit record without credentials.
    ///
    /// # Errors
    /// Returns an error when the audit record cannot be persisted.
    fn audit_mcp_invocation(&self, audit: &McpInvocationAudit) -> Result<(), SessionError>;
    /// Lists configured providers.
    ///
    /// # Errors
    /// Returns an error when persistence cannot be queried.
    fn list_providers(&self) -> Result<Vec<ProviderRecord>, SessionError>;
    /// Creates one credential account and returns its stable identity.
    ///
    /// # Errors
    /// Returns an error when the provider is absent, the label conflicts, or persistence fails.
    fn add_provider_account(
        &self,
        provider: &str,
        label: &str,
    ) -> Result<ProviderAccountRecord, SessionError>;
    /// Updates safe durable account metadata.
    ///
    /// # Errors
    /// Returns an error when the account is absent, the label conflicts, or persistence fails.
    fn set_provider_account_label(
        &self,
        provider: &str,
        account_id: &str,
        label: &str,
    ) -> Result<(), SessionError>;
    /// Enables or disables an account for new work.
    ///
    /// # Errors
    /// Returns an error when the account is absent or persistence fails.
    fn set_provider_account_enabled(
        &self,
        provider: &str,
        account_id: &str,
        enabled: bool,
    ) -> Result<(), SessionError>;
    /// Makes one account the provider default.
    ///
    /// # Errors
    /// Returns an error when the account is absent or persistence fails.
    fn set_provider_account_default(
        &self,
        provider: &str,
        account_id: &str,
    ) -> Result<(), SessionError>;
    /// Stores a safe, non-secret display identity for an account.
    ///
    /// # Errors
    /// Returns an error when the account is absent or persistence fails.
    fn set_provider_account_identity(
        &self,
        provider: &str,
        account_id: &str,
        identity: Option<&str>,
    ) -> Result<(), SessionError>;
    /// Removes unused account metadata and its cascading secret.
    ///
    /// # Errors
    /// Returns an error when the account is absent, pinned to a session, or persistence fails.
    fn remove_provider_account(&self, provider: &str, account_id: &str)
    -> Result<(), SessionError>;
    /// Changes whether a provider accepts new work.
    ///
    /// # Errors
    /// Returns an error when persistence cannot be updated.
    fn set_provider_enabled(&self, provider: &str, enabled: bool) -> Result<(), SessionError>;
    /// Updates one provider's discoverability filter without changing model authorization.
    ///
    /// # Errors
    /// Returns an error when persistence cannot be updated.
    fn set_provider_model_filter(
        &self,
        provider: &str,
        enabled: bool,
        selected_model_ids: &[String],
    ) -> Result<(), SessionError>;
    /// Saves the current durable projection of a sub-agent run and its transcript.
    ///
    /// # Errors
    /// Returns an error when the transaction cannot be committed.
    fn save_subagent(&self, record: &SubagentRecord) -> Result<(), SessionError>;
    /// Atomically saves the linked source and successor of one authorized continuation.
    ///
    /// # Errors
    /// Returns an error when either record cannot be persisted in the shared transaction.
    fn save_subagent_continuation(
        &self,
        source: &SubagentRecord,
        successor: &SubagentRecord,
    ) -> Result<(), SessionError>;
    /// Lists the sub-agent runs associated with a logical parent session.
    ///
    /// # Errors
    /// Returns an error when persistence cannot be queried or contains invalid data.
    fn list_subagents(&self, parent_session_id: &str) -> Result<Vec<SubagentRecord>, SessionError>;
    /// Loads optional web-browser add-on preferences.
    ///
    /// # Errors
    /// Returns an error when preference storage cannot be read.
    fn load_web_config(&self) -> Result<WebConfig, SessionError>;
    /// Saves optional web-browser add-on preferences.
    ///
    /// # Errors
    /// Returns an error when preference storage cannot be updated.
    fn save_web_config(&self, config: &WebConfig) -> Result<(), SessionError>;
    /// Loads optional semantic-memory add-on preferences.
    ///
    /// # Errors
    /// Returns an error when preference storage cannot be read.
    fn load_memory_config(&self) -> Result<MemoryConfig, SessionError>;
    /// Saves optional semantic-memory add-on preferences.
    ///
    /// # Errors
    /// Returns an error when preference storage cannot be updated.
    fn save_memory_config(&self, config: &MemoryConfig) -> Result<(), SessionError>;
    /// Loads optional vision add-on preferences.
    ///
    /// # Errors
    /// Returns an error when preference storage cannot be read.
    fn load_vision_config(&self) -> Result<VisionConfig, SessionError>;
    /// Saves optional vision add-on preferences.
    ///
    /// # Errors
    /// Returns an error when preference storage cannot be updated.
    fn save_vision_config(&self, config: &VisionConfig) -> Result<(), SessionError>;
    /// Loads the terminal image-preview preference.
    ///
    /// # Errors
    /// Returns an error when preference storage cannot be read.
    fn load_terminal_image_mode(&self) -> Result<TerminalImageMode, SessionError>;
    /// Saves the terminal image-preview preference.
    ///
    /// # Errors
    /// Returns an error when preference storage cannot be updated.
    fn save_terminal_image_mode(&self, mode: TerminalImageMode) -> Result<(), SessionError>;
    /// Loads the installation-level local invocation telemetry consent flag. Missing means disabled.
    ///
    /// # Errors
    /// Returns an error when consent storage cannot be read.
    fn load_invocation_telemetry_enabled(&self) -> Result<bool, SessionError>;
    /// Atomically changes the installation-level local invocation telemetry consent flag.
    ///
    /// # Errors
    /// Returns an error when consent storage cannot be updated.
    fn save_invocation_telemetry_enabled(&self, enabled: bool) -> Result<(), SessionError>;
    /// Idempotently appends one accepted invocation and updates its aggregate in one transaction.
    ///
    /// # Errors
    /// Returns an error when the atomic event and aggregate transaction cannot be committed.
    fn record_invocation(&self, invocation: &InvocationRecord) -> Result<bool, SessionError>;
    /// Reads the cheap aggregate projection without scanning event history.
    ///
    /// # Errors
    /// Returns an error when aggregate storage cannot be read or contains invalid values.
    fn invocation_summary(&self) -> Result<nakode_protocol::InvocationSummary, SessionError>;
    /// Reads a bounded, server-bucketed half-open event range.
    ///
    /// # Errors
    /// Returns an error when the range is invalid or event storage cannot be read.
    fn invocation_timeline(
        &self,
        start_at_ms: u64,
        end_at_ms: u64,
        bucket_width_ms: u64,
    ) -> Result<nakode_protocol::InvocationTimeline, SessionError>;
}

pub struct McpInvocationAudit {
    pub id: String,
    pub workspace: String,
    pub session_id: String,
    pub run_id: Option<String>,
    pub server_id: String,
    pub tool_name: String,
    pub arguments_json: String,
    pub result_json: String,
    pub status: String,
    pub started_at_ms: u64,
    pub duration_ms: u64,
}
pub struct SqliteSessionRepository {
    connection: Mutex<Connection>,
    path: PathBuf,
}

impl SqliteSessionRepository {
    /// Returns Nakode's platform-specific application data directory.
    ///
    /// # Errors
    /// Returns an error when the platform does not expose an application data directory.
    pub fn default_data_directory() -> Result<PathBuf, SessionError> {
        ProjectDirs::from("dev", "nakode", "Nakode")
            .map(|project| project.data_local_dir().to_path_buf())
            .ok_or(SessionError::MissingDataDirectory)
    }

    /// Opens the repository in Nakode's platform-specific data directory.
    ///
    /// # Errors
    ///
    /// Returns an error when the data directory or database cannot be opened.
    pub fn open_default() -> Result<Self, SessionError> {
        let directory = Self::default_data_directory()?;
        let new_database = directory.join("sessions.sqlite3");
        let legacy_database = [
            ProjectDirs::from("dev", "nako-agent", "Nako Agent"),
            ProjectDirs::from("dev", "flock", "Flock"),
        ]
        .into_iter()
        .flatten()
        .map(|legacy| legacy.data_local_dir().join("sessions.sqlite3"))
        .find(|legacy| legacy.exists());
        if !new_database.exists()
            && let Some(legacy_database) = legacy_database
        {
            return Self::open(legacy_database);
        }
        std::fs::create_dir_all(&directory).map_err(|source| SessionError::CreateDirectory {
            path: directory.display().to_string(),
            source,
        })?;
        protect_path(&directory, 0o700)?;
        Self::open(new_database)
    }

    /// Opens or creates a repository at `path` and applies its schema.
    ///
    /// # Errors
    ///
    /// Returns an error when the database cannot be opened or migrated.
    #[allow(clippy::too_many_lines)]
    pub fn open(path: impl AsRef<Path>) -> Result<Self, SessionError> {
        let path = path.as_ref();
        let mut connection = Connection::open(path)?;
        configure_connection(&connection)?;
        protect_path(path, 0o600)?;
        execute_batch_with_busy_retry(
            &connection,
            "PRAGMA foreign_keys = ON;
             CREATE TABLE IF NOT EXISTS sessions (
               id TEXT PRIMARY KEY,
               provider TEXT NOT NULL,
               account_id TEXT,
               provider_session_id TEXT NOT NULL,
               workspace TEXT NOT NULL,
               working_directory TEXT,
               title TEXT NOT NULL,
               model TEXT,
               model_reasoning_effort TEXT,
               model_fast_mode INTEGER NOT NULL DEFAULT 0,
               last_turn_id TEXT,
               last_turn_model TEXT,
               last_turn_reasoning_effort TEXT,
               last_turn_fast_mode INTEGER NOT NULL DEFAULT 0,
               last_turn_outcome TEXT,
               created_at INTEGER NOT NULL,
               updated_at INTEGER NOT NULL,
               last_owner_activity_at INTEGER,
               enabled_skill_ids_json TEXT,
               code_mode INTEGER NOT NULL DEFAULT 0,
               UNIQUE(provider, provider_session_id)
             );
             CREATE INDEX IF NOT EXISTS sessions_workspace_updated
               ON sessions(workspace, updated_at DESC);
             CREATE TABLE IF NOT EXISTS session_bridges (
               session_id TEXT PRIMARY KEY,
               workspace TEXT NOT NULL,
               kind TEXT NOT NULL,
               lifecycle TEXT NOT NULL,
               display_title TEXT NOT NULL,
               revision INTEGER NOT NULL,
               transport TEXT,
               external_parent_id TEXT,
               external_thread_id TEXT,
               last_delivered_turn_id TEXT,
               last_delivered_kind TEXT NOT NULL DEFAULT 'assistant',
               delivery_json TEXT,
               live_turn_id TEXT,
               live_external_message_id TEXT,
               active_source_message_id TEXT,
               pending_inbound_json TEXT,
               inbound_turn_origins_json TEXT NOT NULL DEFAULT '[]',
               updated_at_ms INTEGER NOT NULL
             );
             CREATE INDEX IF NOT EXISTS session_bridges_workspace
               ON session_bridges(workspace, updated_at_ms DESC);
             CREATE UNIQUE INDEX IF NOT EXISTS session_bridges_external_thread
               ON session_bridges(transport, external_thread_id)
               WHERE transport IS NOT NULL AND external_thread_id IS NOT NULL;
             CREATE TABLE IF NOT EXISTS session_bridge_inbound_events (
               session_id TEXT NOT NULL,
               external_event_id TEXT NOT NULL,
               disposition TEXT NOT NULL DEFAULT 'duplicate',
               recorded_at_ms INTEGER NOT NULL,
               PRIMARY KEY(session_id, external_event_id)
             );
             CREATE INDEX IF NOT EXISTS session_bridge_inbound_events_recorded
               ON session_bridge_inbound_events(session_id, recorded_at_ms DESC);
             CREATE TABLE IF NOT EXISTS skill_preferences (
               profile_id TEXT NOT NULL,
               skill_id TEXT NOT NULL,
               last_name TEXT NOT NULL,
               last_description TEXT NOT NULL,
               enabled INTEGER NOT NULL DEFAULT 1,
               updated_at INTEGER NOT NULL,
               PRIMARY KEY(profile_id, skill_id)
             );
             CREATE INDEX IF NOT EXISTS skill_preferences_profile_name
               ON skill_preferences(profile_id, last_name COLLATE NOCASE);
             CREATE TABLE IF NOT EXISTS session_skill_profiles (
               session_id TEXT PRIMARY KEY REFERENCES sessions(id) ON DELETE CASCADE,
               profile_id TEXT NOT NULL
             );
             CREATE INDEX IF NOT EXISTS session_skill_profiles_profile
               ON session_skill_profiles(profile_id);
             CREATE TABLE IF NOT EXISTS provider_models (
               provider TEXT NOT NULL,
               model_id TEXT NOT NULL,
               is_default INTEGER NOT NULL,
               cached_at INTEGER NOT NULL,
               capabilities TEXT NOT NULL DEFAULT '{}',
               PRIMARY KEY(provider, model_id)
             );
             CREATE TABLE IF NOT EXISTS provider_model_preferences (
               provider TEXT PRIMARY KEY,
               model_id TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS provider_model_options (
               provider TEXT NOT NULL,
               model_id TEXT NOT NULL,
               reasoning_effort TEXT,
               fast_mode INTEGER NOT NULL DEFAULT 0,
               PRIMARY KEY(provider, model_id)
             );
             CREATE TABLE IF NOT EXISTS providers (
               provider TEXT PRIMARY KEY,
               display_name TEXT NOT NULL,
               enabled INTEGER NOT NULL,
               updated_at INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS provider_model_filters (
               provider TEXT PRIMARY KEY REFERENCES providers(provider) ON DELETE CASCADE,
               enabled INTEGER NOT NULL DEFAULT 0,
               selected_model_ids TEXT NOT NULL DEFAULT '[]'
             );
             CREATE TABLE IF NOT EXISTS provider_credentials (
               provider TEXT PRIMARY KEY REFERENCES providers(provider) ON DELETE CASCADE,
               credential_kind TEXT NOT NULL,
               credential_json TEXT NOT NULL,
               updated_at INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS provider_accounts (
               account_id TEXT PRIMARY KEY,
               provider TEXT NOT NULL REFERENCES providers(provider) ON DELETE CASCADE,
               label TEXT NOT NULL,
               enabled INTEGER NOT NULL DEFAULT 1,
               is_default INTEGER NOT NULL DEFAULT 0,
               safe_identity TEXT,
               routing_mode TEXT NOT NULL DEFAULT 'explicit_only',
               created_at INTEGER NOT NULL,
               updated_at INTEGER NOT NULL,
               UNIQUE(provider, label)
             );
             CREATE UNIQUE INDEX IF NOT EXISTS provider_accounts_one_default
               ON provider_accounts(provider) WHERE is_default = 1;
             CREATE INDEX IF NOT EXISTS provider_accounts_provider
               ON provider_accounts(provider, enabled, account_id);
             CREATE TABLE IF NOT EXISTS provider_account_credentials (
               account_id TEXT PRIMARY KEY REFERENCES provider_accounts(account_id) ON DELETE CASCADE,
               credential_kind TEXT NOT NULL,
               credential_json TEXT NOT NULL,
               updated_at INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS mcp_servers (
               workspace TEXT NOT NULL,
               id TEXT NOT NULL,
               display_name TEXT NOT NULL,
               endpoint TEXT NOT NULL,
               transport TEXT NOT NULL,
               enabled INTEGER NOT NULL,
               auth_kind TEXT NOT NULL,
               credential_required INTEGER NOT NULL,
               protocol_version TEXT NOT NULL,
               provenance_url TEXT NOT NULL,
               provenance_version TEXT NOT NULL,
               provenance_commit TEXT NOT NULL,
               provenance_sha256 TEXT NOT NULL,
               license_evidence TEXT NOT NULL,
               timeout_ms INTEGER NOT NULL,
               max_response_bytes INTEGER NOT NULL,
               artifact_semantics TEXT NOT NULL,
               template_id TEXT,
               health TEXT NOT NULL,
               server_name TEXT,
               server_version TEXT,
               last_error TEXT,
               last_connected_at_ms INTEGER,
               updated_at_ms INTEGER NOT NULL,
               credential_kind TEXT,
               tools_json TEXT NOT NULL DEFAULT '[]',
               grants_json TEXT NOT NULL DEFAULT '{}',
               PRIMARY KEY(workspace, id)
             );
             CREATE INDEX IF NOT EXISTS mcp_servers_workspace_name
               ON mcp_servers(workspace, display_name COLLATE NOCASE);
             CREATE TABLE IF NOT EXISTS mcp_credentials (
               workspace TEXT NOT NULL,
               server_id TEXT NOT NULL,
               credential_kind TEXT NOT NULL,
               credential_json TEXT NOT NULL,
               updated_at_ms INTEGER NOT NULL,
               PRIMARY KEY(workspace, server_id)
             );
             CREATE TABLE IF NOT EXISTS mcp_invocation_audit (
               id TEXT PRIMARY KEY,
               workspace TEXT NOT NULL,
               session_id TEXT NOT NULL,
               run_id TEXT,
               server_id TEXT NOT NULL,
               tool_name TEXT NOT NULL,
               arguments_json TEXT NOT NULL,
               result_json TEXT NOT NULL,
               status TEXT NOT NULL,
               started_at_ms INTEGER NOT NULL,
               duration_ms INTEGER NOT NULL
             );
             CREATE INDEX IF NOT EXISTS mcp_invocation_audit_session
               ON mcp_invocation_audit(session_id, started_at_ms DESC);
             CREATE TABLE IF NOT EXISTS native_runtime_sessions (
               provider TEXT NOT NULL,
               session_id TEXT NOT NULL,
               session_json TEXT NOT NULL,
               updated_at INTEGER NOT NULL,
               PRIMARY KEY(provider, session_id)
             );
             CREATE TABLE IF NOT EXISTS owner_turns (
               session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
               turn_id TEXT NOT NULL,
               model TEXT,
               reasoning_effort TEXT,
               fast_mode INTEGER NOT NULL DEFAULT 0,
               outcome INTEGER NOT NULL,
               PRIMARY KEY(session_id, turn_id)
             );
             CREATE INDEX IF NOT EXISTS owner_turns_session ON owner_turns(session_id);
             CREATE TABLE IF NOT EXISTS accepted_owner_prompts (
               acceptance_order INTEGER PRIMARY KEY AUTOINCREMENT,
               session_id TEXT NOT NULL,
               prompt_id TEXT NOT NULL,
               raw_text TEXT NOT NULL,
               source_transport TEXT,
               dispatch_pending INTEGER NOT NULL DEFAULT 0,
               UNIQUE(session_id, prompt_id)
             );
             CREATE INDEX IF NOT EXISTS accepted_owner_prompts_session
               ON accepted_owner_prompts(session_id, acceptance_order);
             CREATE TABLE IF NOT EXISTS session_native_history (
               parent_session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
               provider TEXT NOT NULL,
               provider_session_id TEXT NOT NULL,
               PRIMARY KEY(parent_session_id, provider, provider_session_id)
             );
             CREATE TABLE IF NOT EXISTS orchestration_runs (
               parent_session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
               id TEXT NOT NULL,
               agent_slug TEXT NOT NULL,
               provider TEXT NOT NULL,
               model TEXT,
               provider_session_id TEXT,
               input_tokens INTEGER NOT NULL DEFAULT 0,
               output_tokens INTEGER NOT NULL DEFAULT 0,
               cached_input_tokens INTEGER NOT NULL DEFAULT 0,
               cache_write_tokens INTEGER NOT NULL DEFAULT 0,
               objective TEXT NOT NULL,
               status TEXT NOT NULL,
               latest_activity TEXT NOT NULL,
               parent_run_id TEXT,
               invocation_turn_id TEXT,
               invocation_call_id TEXT,
               originating_owner_entry_id TEXT,
               archetype_purpose TEXT NOT NULL DEFAULT '',
               policy_json TEXT NOT NULL DEFAULT '{}',
               remaining_delegation_depth INTEGER NOT NULL DEFAULT 0,
               started_at_ms INTEGER NOT NULL DEFAULT 0,
               ended_at_ms INTEGER,
               termination_kind TEXT,
               termination_detail TEXT,
               objective_mismatch_handoff TEXT,
               salvage_json TEXT,
               continued_from_run_id TEXT,
               continued_by_run_id TEXT,
               continuation_depth INTEGER NOT NULL DEFAULT 0,
               additional_turns INTEGER,
               inherited_evidence_json TEXT NOT NULL DEFAULT '[]',
               transcript_has_earlier INTEGER NOT NULL DEFAULT 0,
               created_at INTEGER NOT NULL,
               updated_at INTEGER NOT NULL,
               PRIMARY KEY(parent_session_id, id)
             );
             CREATE INDEX IF NOT EXISTS orchestration_runs_parent_created
               ON orchestration_runs(parent_session_id, created_at, id);
             CREATE TABLE IF NOT EXISTS agent_turns (
               parent_session_id TEXT NOT NULL,
               run_id TEXT NOT NULL,
               sequence INTEGER NOT NULL,
               entry_id TEXT NOT NULL,
               item_key TEXT,
               kind TEXT NOT NULL,
               title TEXT NOT NULL,
               body TEXT NOT NULL,
               status TEXT NOT NULL,
               provider_id TEXT,
               model_id TEXT,
               tool_audit_json TEXT,
               created_at_ms INTEGER,
               owner_turn_id TEXT,
               reasoning_effort TEXT,
               fast_mode INTEGER,
               source_transport TEXT,
               PRIMARY KEY(parent_session_id, run_id, sequence),
               FOREIGN KEY(parent_session_id, run_id)
                 REFERENCES orchestration_runs(parent_session_id, id) ON DELETE CASCADE
             );",
        )?;
        apply_invocation_telemetry_migration(&mut connection)?;
        let owner_prompt_columns = {
            let mut statement = connection.prepare("PRAGMA table_info(accepted_owner_prompts)")?;
            statement
                .query_map([], |row| row.get::<_, String>(1))?
                .collect::<Result<Vec<_>, _>>()?
        };
        if !owner_prompt_columns
            .iter()
            .any(|column| column == "source_transport")
        {
            execute_batch_with_busy_retry(
                &connection,
                "BEGIN IMMEDIATE;
                 ALTER TABLE accepted_owner_prompts ADD COLUMN source_transport TEXT;
                 COMMIT;",
            )?;
        }
        if !owner_prompt_columns
            .iter()
            .any(|column| column == "dispatch_pending")
        {
            execute_batch_with_busy_retry(
                &connection,
                "BEGIN IMMEDIATE;
                 ALTER TABLE accepted_owner_prompts
                   ADD COLUMN dispatch_pending INTEGER NOT NULL DEFAULT 0;
                 COMMIT;",
            )?;
        }
        let bridge_columns = {
            let mut statement = connection.prepare("PRAGMA table_info(session_bridges)")?;
            statement
                .query_map([], |row| row.get::<_, String>(1))?
                .collect::<Result<Vec<_>, _>>()?
        };
        if !bridge_columns
            .iter()
            .any(|column| column == "pending_inbound_json")
        {
            execute_batch_with_busy_retry(
                &connection,
                "BEGIN IMMEDIATE;
                 ALTER TABLE session_bridges ADD COLUMN pending_inbound_json TEXT;
                 COMMIT;",
            )?;
        }
        if !bridge_columns
            .iter()
            .any(|column| column == "last_delivered_kind")
        {
            execute_batch_with_busy_retry(
                &connection,
                "BEGIN IMMEDIATE;
                 ALTER TABLE session_bridges ADD COLUMN last_delivered_kind TEXT NOT NULL DEFAULT 'assistant';
                 COMMIT;",
            )?;
        }
        if !bridge_columns
            .iter()
            .any(|column| column == "inbound_turn_origins_json")
        {
            execute_batch_with_busy_retry(
                &connection,
                "BEGIN IMMEDIATE;
                 ALTER TABLE session_bridges ADD COLUMN inbound_turn_origins_json TEXT NOT NULL DEFAULT '[]';
                 COMMIT;",
            )?;
        }
        let inbound_event_columns = {
            let mut statement =
                connection.prepare("PRAGMA table_info(session_bridge_inbound_events)")?;
            statement
                .query_map([], |row| row.get::<_, String>(1))?
                .collect::<Result<Vec<_>, _>>()?
        };
        if !inbound_event_columns
            .iter()
            .any(|column| column == "disposition")
        {
            execute_batch_with_busy_retry(
                &connection,
                "BEGIN IMMEDIATE;
                 ALTER TABLE session_bridge_inbound_events
                   ADD COLUMN disposition TEXT NOT NULL DEFAULT 'duplicate';
                 COMMIT;",
            )?;
        }
        if bridge_columns
            .iter()
            .any(|column| column == "recent_inbound_event_ids_json")
        {
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let legacy_rows = {
                let mut statement = transaction.prepare(
                    "SELECT session_id, recent_inbound_event_ids_json, updated_at_ms
                     FROM session_bridges
                     WHERE recent_inbound_event_ids_json <> '[]'",
                )?;
                statement
                    .query_map([], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, i64>(2)?,
                        ))
                    })?
                    .collect::<Result<Vec<_>, _>>()?
            };
            for (session_id, encoded_ids, recorded_at_ms) in legacy_rows {
                let event_ids: Vec<String> =
                    serde_json::from_str(&encoded_ids).map_err(|source| {
                        SessionError::InvalidStoredJson {
                            field: "session_bridges.recent_inbound_event_ids_json",
                            source,
                        }
                    })?;
                for external_event_id in event_ids {
                    transaction.execute(
                        "INSERT OR IGNORE INTO session_bridge_inbound_events
                         (session_id, external_event_id, recorded_at_ms)
                         VALUES (?1, ?2, ?3)",
                        params![session_id, external_event_id, recorded_at_ms],
                    )?;
                }
            }
            transaction.execute(
                "UPDATE session_bridges SET recent_inbound_event_ids_json = '[]'
                 WHERE recent_inbound_event_ids_json <> '[]'",
                [],
            )?;
            transaction.commit()?;
        }
        let session_columns = {
            let mut statement = connection.prepare("PRAGMA table_info(sessions)")?;
            statement
                .query_map([], |row| row.get::<_, String>(1))?
                .collect::<Result<Vec<_>, _>>()?
        };
        let mut session_migration = String::from("BEGIN IMMEDIATE;\n");
        for (column, definition) in [
            ("model_reasoning_effort", "TEXT"),
            ("model_fast_mode", "INTEGER NOT NULL DEFAULT 0"),
            ("last_turn_id", "TEXT"),
            ("last_turn_model", "TEXT"),
            ("last_turn_reasoning_effort", "TEXT"),
            ("last_turn_fast_mode", "INTEGER NOT NULL DEFAULT 0"),
            ("last_turn_outcome", "TEXT"),
            ("working_directory", "TEXT"),
            ("last_owner_activity_at", "INTEGER"),
            ("enabled_skill_ids_json", "TEXT"),
            ("account_id", "TEXT"),
            ("code_mode", "INTEGER NOT NULL DEFAULT 0"),
        ] {
            if !session_columns.iter().any(|existing| existing == column) {
                writeln!(
                    session_migration,
                    "ALTER TABLE sessions ADD COLUMN {column} {definition};"
                )
                .expect("writing to a String cannot fail");
            }
        }
        session_migration.push_str("COMMIT;");
        execute_batch_with_busy_retry(&connection, &session_migration)?;
        execute_batch_with_busy_retry(
            &connection,
            "BEGIN IMMEDIATE;
             INSERT OR IGNORE INTO provider_accounts
               (account_id, provider, label, enabled, is_default, routing_mode, created_at, updated_at)
             SELECT 'legacy-' || provider, provider, 'Default', 1, 1,
                    CASE WHEN provider = 'openai-codex' THEN 'automatic' ELSE 'explicit_only' END,
                    updated_at, updated_at
             FROM provider_credentials;
             INSERT OR IGNORE INTO provider_account_credentials
               (account_id, credential_kind, credential_json, updated_at)
             SELECT 'legacy-' || provider, credential_kind, credential_json, updated_at
             FROM provider_credentials;
             UPDATE sessions
             SET account_id = 'legacy-' || provider
             WHERE account_id IS NULL
               AND EXISTS (
                 SELECT 1 FROM provider_accounts a
                 WHERE a.provider = sessions.provider
                   AND a.account_id = 'legacy-' || sessions.provider
               );
             CREATE INDEX IF NOT EXISTS sessions_account ON sessions(account_id);
             COMMIT;",
        )?;
        let orchestration_columns = {
            let mut statement = connection.prepare("PRAGMA table_info(orchestration_runs)")?;
            statement
                .query_map([], |row| row.get::<_, String>(1))?
                .collect::<Result<Vec<_>, _>>()?
        };
        let mut orchestration_migration = String::from("BEGIN IMMEDIATE;\n");
        if !orchestration_columns.iter().any(|column| column == "model") {
            orchestration_migration
                .push_str("ALTER TABLE orchestration_runs ADD COLUMN model TEXT;\n");
        }
        for (column, definition) in [
            ("input_tokens", "INTEGER NOT NULL DEFAULT 0"),
            ("output_tokens", "INTEGER NOT NULL DEFAULT 0"),
            ("cached_input_tokens", "INTEGER NOT NULL DEFAULT 0"),
            ("cache_write_tokens", "INTEGER NOT NULL DEFAULT 0"),
            ("parent_run_id", "TEXT"),
            ("invocation_turn_id", "TEXT"),
            ("invocation_call_id", "TEXT"),
            ("originating_owner_entry_id", "TEXT"),
            ("archetype_purpose", "TEXT NOT NULL DEFAULT ''"),
            ("policy_json", "TEXT NOT NULL DEFAULT '{}'"),
            ("remaining_delegation_depth", "INTEGER NOT NULL DEFAULT 0"),
            ("started_at_ms", "INTEGER NOT NULL DEFAULT 0"),
            ("ended_at_ms", "INTEGER"),
            ("termination_kind", "TEXT"),
            ("termination_detail", "TEXT"),
            ("objective_mismatch_handoff", "TEXT"),
            ("salvage_json", "TEXT"),
            ("continued_from_run_id", "TEXT"),
            ("continued_by_run_id", "TEXT"),
            ("continuation_depth", "INTEGER NOT NULL DEFAULT 0"),
            ("additional_turns", "INTEGER"),
            ("inherited_evidence_json", "TEXT NOT NULL DEFAULT '[]'"),
            ("transcript_has_earlier", "INTEGER NOT NULL DEFAULT 0"),
        ] {
            if !orchestration_columns
                .iter()
                .any(|existing| existing == column)
            {
                writeln!(
                    orchestration_migration,
                    "ALTER TABLE orchestration_runs ADD COLUMN {column} {definition};"
                )
                .expect("writing to a String cannot fail");
            }
        }
        orchestration_migration.push_str("COMMIT;");
        execute_batch_with_busy_retry(&connection, &orchestration_migration)?;
        apply_subagent_transcript_boundary_migration(&mut connection)?;
        let agent_turn_columns = {
            let mut statement = connection.prepare("PRAGMA table_info(agent_turns)")?;
            statement
                .query_map([], |row| row.get::<_, String>(1))?
                .collect::<Result<Vec<_>, _>>()?
        };
        if agent_turn_columns.iter().any(|column| column == "entry_id") {
            execute_batch_with_busy_retry(
                &connection,
                "BEGIN IMMEDIATE;
                 UPDATE agent_turns
                 SET entry_id = parent_session_id || ':' || run_id || ':' || sequence
                 WHERE entry_id IS NULL;
                 COMMIT;",
            )?;
        } else {
            execute_batch_with_busy_retry(
                &connection,
                "BEGIN IMMEDIATE;
                 ALTER TABLE agent_turns ADD COLUMN entry_id TEXT;
                 UPDATE agent_turns
                 SET entry_id = parent_session_id || ':' || run_id || ':' || sequence
                 WHERE entry_id IS NULL;
                 COMMIT;",
            )?;
        }
        for (column, definition) in [
            ("provider_id", "TEXT"),
            ("model_id", "TEXT"),
            ("tool_audit_json", "TEXT"),
            ("owner_turn_id", "TEXT"),
            ("reasoning_effort", "TEXT"),
            ("fast_mode", "INTEGER"),
            ("source_transport", "TEXT"),
        ] {
            if !agent_turn_columns.iter().any(|existing| existing == column) {
                execute_batch_with_busy_retry(
                    &connection,
                    &format!("ALTER TABLE agent_turns ADD COLUMN {column} {definition};"),
                )?;
            }
        }
        if !agent_turn_columns
            .iter()
            .any(|existing| existing == "created_at_ms")
        {
            execute_batch_with_busy_retry(
                &connection,
                "ALTER TABLE agent_turns ADD COLUMN created_at_ms INTEGER;",
            )?;
        }
        let has_model_specific_options = {
            let mut statement = connection.prepare("PRAGMA table_info(provider_model_options)")?;
            let columns = statement
                .query_map([], |row| row.get::<_, String>(1))?
                .collect::<Result<Vec<_>, _>>()?;
            columns.iter().any(|column| column == "model_id")
        };
        if !has_model_specific_options {
            execute_batch_with_busy_retry(
                &connection,
                "BEGIN IMMEDIATE;
                 ALTER TABLE provider_model_options RENAME TO provider_model_options_legacy;
                 CREATE TABLE provider_model_options (
                   provider TEXT NOT NULL,
                   model_id TEXT NOT NULL,
                   reasoning_effort TEXT,
                   fast_mode INTEGER NOT NULL DEFAULT 0,
                   PRIMARY KEY(provider, model_id)
                 );
                 INSERT INTO provider_model_options
                   (provider, model_id, reasoning_effort, fast_mode)
                 SELECT provider, '*', reasoning_effort, fast_mode
                 FROM provider_model_options_legacy;
                 DROP TABLE provider_model_options_legacy;
                 COMMIT;",
            )?;
        }
        let has_model_capabilities = {
            let mut statement = connection.prepare("PRAGMA table_info(provider_models)")?;
            let columns = statement
                .query_map([], |row| row.get::<_, String>(1))?
                .collect::<Result<Vec<_>, _>>()?;
            columns.iter().any(|column| column == "capabilities")
        };
        if !has_model_capabilities {
            execute_batch_with_busy_retry(
                &connection,
                "BEGIN IMMEDIATE;
                 ALTER TABLE provider_models
                   ADD COLUMN capabilities TEXT NOT NULL DEFAULT '{}';
                 COMMIT;",
            )?;
        }
        let has_legacy_models = connection
            .query_row(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'backend_models'",
                [],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if has_legacy_models {
            connection.execute_batch(
                "INSERT OR IGNORE INTO provider_models
                   (provider, model_id, is_default, cached_at)
                 SELECT provider, model_id, is_default, cached_at FROM backend_models;
                 DROP TABLE backend_models;",
            )?;
        }
        seed_provider_catalog(&connection)?;
        Ok(Self {
            connection: Mutex::new(connection),
            path: path.to_path_buf(),
        })
    }

    #[must_use]
    pub fn database_path(&self) -> &Path {
        &self.path
    }

    fn bridge_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<SessionBridgeRecord> {
        let kind_value = row.get::<_, String>(2)?;
        let kind = orchestrator_kind_from_database(&kind_value)
            .map_err(|error| stored_bridge_conversion_error(2, error))?;
        let lifecycle_value = row.get::<_, String>(3)?;
        let lifecycle = bridge_lifecycle_from_database(&lifecycle_value)
            .map_err(|error| stored_bridge_conversion_error(3, error))?;
        let revision = row.get::<_, i64>(5)?;
        let revision = u64::try_from(revision).map_err(|error| {
            stored_bridge_conversion_error(
                5,
                SessionError::InvalidStoredValue {
                    field: "session_bridges.revision",
                    value: error.to_string(),
                },
            )
        })?;
        let last_projected_turn_id = row.get::<_, Option<String>>(9)?;
        let last_projected_kind = row.get::<_, String>(10)?;
        let last_projected = last_projected_turn_id
            .map(|turn_id| {
                bridge_projection_kind_from_database(&last_projected_kind)
                    .map(|kind| BridgeProjectionRecord { kind, turn_id })
            })
            .transpose()
            .map_err(|error| stored_bridge_conversion_error(10, error))?;
        let delivery_json = row.get::<_, Option<String>>(11)?;
        let delivery = delivery_json
            .map(|value| {
                serde_json::from_str(&value).map_err(|source| SessionError::InvalidStoredJson {
                    field: "session_bridges.delivery_json",
                    source,
                })
            })
            .transpose()
            .map_err(|error| stored_bridge_conversion_error(11, error))?;
        let pending_json = row.get::<_, Option<String>>(15)?;
        let pending_inbound = pending_json
            .map(|value| {
                serde_json::from_str(&value).map_err(|source| SessionError::InvalidStoredJson {
                    field: "session_bridges.pending_inbound_json",
                    source,
                })
            })
            .transpose()
            .map_err(|error| stored_bridge_conversion_error(15, error))?;
        let inbound_turn_origins_json = row.get::<_, String>(16)?;
        let inbound_turn_origins = serde_json::from_str(&inbound_turn_origins_json)
            .map_err(|source| SessionError::InvalidStoredJson {
                field: "session_bridges.inbound_turn_origins_json",
                source,
            })
            .map_err(|error| stored_bridge_conversion_error(16, error))?;
        Ok(SessionBridgeRecord {
            session_id: row.get(0)?,
            workspace: row.get(1)?,
            kind,
            lifecycle,
            display_title: row.get(4)?,
            revision,
            transport: row.get(6)?,
            external_parent_id: row.get(7)?,
            external_thread_id: row.get(8)?,
            last_projected,
            delivery,
            live_turn_id: row.get(12)?,
            live_external_message_id: row.get(13)?,
            active_source_message_id: row.get(14)?,
            recent_inbound_event_ids: Vec::new(),
            pending_inbound,
            inbound_turn_origins,
            updated_at_ms: row.get(17)?,
        })
    }

    fn row(row: &rusqlite::Row<'_>) -> rusqlite::Result<SessionRecord> {
        let last_turn_id = row.get::<_, Option<String>>(8)?;
        let last_turn_outcome = row.get::<_, Option<String>>(12)?;
        let last_turn = match (last_turn_id, last_turn_outcome.as_deref()) {
            (Some(id), Some("completed")) => Some(PersistedTurnConfiguration {
                id,
                model: row.get(9)?,
                options: ModelOptions {
                    reasoning_effort: row.get(10)?,
                    fast_mode: row.get::<_, i64>(11)? != 0,
                },
                outcome: TurnOutcome::Completed,
            }),
            (Some(id), Some("interrupted")) => Some(PersistedTurnConfiguration {
                id,
                model: row.get(9)?,
                options: ModelOptions {
                    reasoning_effort: row.get(10)?,
                    fast_mode: row.get::<_, i64>(11)? != 0,
                },
                outcome: TurnOutcome::Interrupted,
            }),
            (Some(id), Some("failed")) => Some(PersistedTurnConfiguration {
                id,
                model: row.get(9)?,
                options: ModelOptions {
                    reasoning_effort: row.get(10)?,
                    fast_mode: row.get::<_, i64>(11)? != 0,
                },
                outcome: TurnOutcome::Failed,
            }),
            _ => None,
        };
        let enabled_skill_ids_json = row.get::<_, Option<String>>(17)?;
        let enabled_skill_ids = enabled_skill_ids_json
            .map(|json| {
                serde_json::from_str(&json).map_err(|source| SessionError::InvalidStoredJson {
                    field: "sessions.enabled_skill_ids_json",
                    source,
                })
            })
            .transpose()
            .map_err(|error| stored_session_conversion_error(17, error))?;
        Ok(SessionRecord {
            id: row.get(0)?,
            provider: row.get(1)?,
            account_id: row.get(18)?,
            provider_session_id: row.get(2)?,
            workspace: row.get(3)?,
            working_directory: row.get(15)?,
            title: row.get(4)?,
            model: row.get(5)?,
            model_options: ModelOptions {
                reasoning_effort: row.get(6)?,
                fast_mode: row.get::<_, i64>(7)? != 0,
            },
            last_turn,
            owner_turns: Vec::new(),
            owner_prompts: Vec::new(),
            created_at: row.get(13)?,
            updated_at: row.get(14)?,
            last_owner_activity_at: row.get(16)?,
            code_mode: row.get::<_, i64>(19)? != 0,
            enabled_skill_ids,
            owned_provider_sessions: Vec::new(),
        })
    }
}

fn record_owner_prompt_on(
    connection: &Connection,
    session_id: &str,
    prompt: &PersistedOwnerPrompt,
) -> Result<(), SessionError> {
    let existing = connection
        .query_row(
            "SELECT raw_text, source_transport, dispatch_pending FROM accepted_owner_prompts
             WHERE session_id = ?1 AND prompt_id = ?2",
            params![session_id, prompt.prompt_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, i64>(2)? != 0,
                ))
            },
        )
        .optional()?;
    if let Some((existing_text, existing_source_transport, _existing_pending)) = existing {
        return if existing_text == prompt.raw_text
            && existing_source_transport == prompt.source_transport
        {
            Ok(())
        } else {
            Err(SessionError::OwnerPromptConflict {
                session_id: session_id.to_owned(),
                prompt_id: prompt.prompt_id.clone(),
            })
        };
    }
    connection.execute(
        "INSERT INTO accepted_owner_prompts
           (session_id, prompt_id, raw_text, source_transport, dispatch_pending)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            session_id,
            prompt.prompt_id,
            prompt.raw_text,
            prompt.source_transport,
            i64::from(prompt.dispatch_pending)
        ],
    )?;
    Ok(())
}

fn protect_bridge_pending_inbound_on(
    connection: &Connection,
    bridges: &[SessionBridgeRecord],
) -> Result<(), SessionError> {
    for bridge in bridges {
        let existing = connection
            .query_row(
                "SELECT pending_inbound_json FROM session_bridges WHERE session_id = ?1",
                params![bridge.session_id],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()?
            .flatten()
            .map(|value| {
                serde_json::from_str::<BridgePendingInboundRecord>(&value).map_err(|source| {
                    SessionError::InvalidStoredJson {
                        field: "session_bridges.pending_inbound_json",
                        source,
                    }
                })
            })
            .transpose()?;
        let Some(existing) = existing else {
            continue;
        };
        if bridge
            .pending_inbound
            .as_ref()
            .is_some_and(|pending| pending.client_prompt_id == existing.client_prompt_id)
        {
            continue;
        }
        return Err(SessionError::BridgePendingInboundConflict {
            session_id: bridge.session_id.clone(),
            prompt_id: existing.client_prompt_id,
        });
    }
    Ok(())
}

fn save_bridge_inbound_event_on(
    connection: &Connection,
    bridges: &[SessionBridgeRecord],
    session_id: &str,
    external_event_id: &str,
    disposition: nakode_protocol::BridgeContinuationDisposition,
) -> Result<(), SessionError> {
    if !bridges.iter().any(|bridge| bridge.session_id == session_id) {
        return Err(SessionError::InvalidStoredValue {
            field: "session_bridge_inbound_events.session_id",
            value: session_id.to_owned(),
        });
    }
    let protected_pending_event = bridges
        .iter()
        .find(|bridge| bridge.session_id == session_id)
        .and_then(|bridge| bridge.pending_inbound.as_ref())
        .map(|pending| pending.external_event_id.as_str());
    // Deliberately not INSERT OR IGNORE: a concurrent process that lost the event claim must
    // roll back before dispatch and retry into the durable Duplicate path.
    connection.execute(
        "INSERT INTO session_bridge_inbound_events
         (session_id, external_event_id, disposition, recorded_at_ms)
         VALUES (?1, ?2, ?3, ?4)",
        params![
            session_id,
            external_event_id,
            bridge_continuation_disposition_value(disposition),
            unix_timestamp().saturating_mul(1000)
        ],
    )?;
    connection.execute(
        "DELETE FROM session_bridge_inbound_events
         WHERE rowid IN (
           SELECT rowid FROM session_bridge_inbound_events
           WHERE session_id = ?1
             AND (?2 IS NULL OR external_event_id != ?2)
           ORDER BY recorded_at_ms DESC, rowid DESC
           LIMIT -1 OFFSET ?3
         )",
        params![
            session_id,
            protected_pending_event,
            i64::try_from(MAX_BRIDGE_REPLAY_EVENTS_PER_SESSION).unwrap_or(i64::MAX)
        ],
    )?;
    Ok(())
}

#[allow(clippy::too_many_lines)]
fn save_session_bridge_on(
    connection: &Connection,
    bridge: &SessionBridgeRecord,
) -> Result<(), SessionError> {
    let delivery_json = bridge
        .delivery
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .map_err(|source| SessionError::InvalidStoredJson {
            field: "session_bridges.delivery_json",
            source,
        })?;
    let pending_json = bridge
        .pending_inbound
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .map_err(|source| SessionError::InvalidStoredJson {
            field: "session_bridges.pending_inbound_json",
            source,
        })?;
    let inbound_turn_origins_json =
        serde_json::to_string(&bridge.inbound_turn_origins).map_err(|source| {
            SessionError::InvalidStoredJson {
                field: "session_bridges.inbound_turn_origins_json",
                source,
            }
        })?;
    let (last_projected_turn_id, last_projected_kind) = bridge.last_projected.as_ref().map_or(
        (
            None,
            bridge_projection_kind_database_value(nakode_protocol::BridgeProjectionKind::Assistant),
        ),
        |projection| {
            (
                Some(projection.turn_id.as_str()),
                bridge_projection_kind_database_value(projection.kind),
            )
        },
    );
    let revision =
        i64::try_from(bridge.revision).map_err(|_| SessionError::InvalidStoredValue {
            field: "session_bridges.revision",
            value: bridge.revision.to_string(),
        })?;
    let current = connection
        .query_row(
            "SELECT session_id, workspace, kind, lifecycle, display_title, revision, transport,
                    external_parent_id, external_thread_id, last_delivered_turn_id,
                    last_delivered_kind, delivery_json, live_turn_id, live_external_message_id,
                    active_source_message_id, pending_inbound_json, inbound_turn_origins_json,
                    updated_at_ms
             FROM session_bridges WHERE session_id = ?1",
            params![bridge.session_id],
            SqliteSessionRepository::bridge_row,
        )
        .optional()?;
    let mut comparable_bridge = bridge.clone();
    // Replay ids live in the normalized inbound-event table and are intentionally absent from this row.
    comparable_bridge.recent_inbound_event_ids.clear();
    if let Some(current) = current
        && (current.revision > bridge.revision
            || (current.revision == bridge.revision && current != comparable_bridge))
    {
        return Err(SessionError::BridgeRevisionConflict {
            session_id: bridge.session_id.clone(),
            revision: bridge.revision,
        });
    }
    let updated = connection.execute(
        "INSERT INTO session_bridges
         (session_id, workspace, kind, lifecycle, display_title, revision, transport,
          external_parent_id, external_thread_id, last_delivered_turn_id, last_delivered_kind,
          delivery_json, live_turn_id, live_external_message_id, active_source_message_id,
          pending_inbound_json, inbound_turn_origins_json, updated_at_ms)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18)
         ON CONFLICT(session_id) DO UPDATE SET
           workspace = excluded.workspace,
           kind = excluded.kind,
           lifecycle = excluded.lifecycle,
           display_title = excluded.display_title,
           revision = excluded.revision,
           transport = excluded.transport,
           external_parent_id = excluded.external_parent_id,
           external_thread_id = excluded.external_thread_id,
           last_delivered_turn_id = excluded.last_delivered_turn_id,
           last_delivered_kind = excluded.last_delivered_kind,
           delivery_json = excluded.delivery_json,
           live_turn_id = excluded.live_turn_id,
           live_external_message_id = excluded.live_external_message_id,
           active_source_message_id = excluded.active_source_message_id,
           pending_inbound_json = excluded.pending_inbound_json,
           inbound_turn_origins_json = excluded.inbound_turn_origins_json,
           updated_at_ms = excluded.updated_at_ms
         WHERE excluded.revision >= session_bridges.revision",
        params![
            bridge.session_id,
            bridge.workspace,
            orchestrator_kind_database_value(bridge.kind),
            bridge_lifecycle_database_value(bridge.lifecycle),
            bridge.display_title,
            revision,
            bridge.transport,
            bridge.external_parent_id,
            bridge.external_thread_id,
            last_projected_turn_id,
            last_projected_kind,
            delivery_json,
            bridge.live_turn_id,
            bridge.live_external_message_id,
            bridge.active_source_message_id,
            pending_json,
            inbound_turn_origins_json,
            bridge.updated_at_ms,
        ],
    )?;
    if updated == 0 {
        return Err(SessionError::BridgeRevisionConflict {
            session_id: bridge.session_id.clone(),
            revision: bridge.revision,
        });
    }
    Ok(())
}

fn stored_session_conversion_error(column: usize, error: SessionError) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(column, rusqlite::types::Type::Text, Box::new(error))
}

fn stored_bridge_conversion_error(column: usize, error: SessionError) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(column, rusqlite::types::Type::Text, Box::new(error))
}

fn orchestrator_kind_database_value(kind: nakode_protocol::OrchestratorKind) -> &'static str {
    match kind {
        nakode_protocol::OrchestratorKind::Chat => "chat",
        nakode_protocol::OrchestratorKind::Agent => "agent",
    }
}

fn orchestrator_kind_from_database(
    value: &str,
) -> Result<nakode_protocol::OrchestratorKind, SessionError> {
    match value {
        "chat" => Ok(nakode_protocol::OrchestratorKind::Chat),
        "agent" => Ok(nakode_protocol::OrchestratorKind::Agent),
        _ => Err(SessionError::InvalidStoredValue {
            field: "session_bridges.kind",
            value: value.to_owned(),
        }),
    }
}

fn bridge_projection_kind_database_value(
    kind: nakode_protocol::BridgeProjectionKind,
) -> &'static str {
    match kind {
        nakode_protocol::BridgeProjectionKind::User => "user",
        nakode_protocol::BridgeProjectionKind::Assistant => "assistant",
    }
}

fn bridge_projection_kind_from_database(
    value: &str,
) -> Result<nakode_protocol::BridgeProjectionKind, SessionError> {
    match value {
        "user" => Ok(nakode_protocol::BridgeProjectionKind::User),
        "assistant" => Ok(nakode_protocol::BridgeProjectionKind::Assistant),
        _ => Err(SessionError::InvalidStoredValue {
            field: "session_bridges.last_delivered_kind",
            value: value.to_owned(),
        }),
    }
}

fn bridge_lifecycle_database_value(lifecycle: nakode_protocol::BridgeLifecycle) -> &'static str {
    match lifecycle {
        nakode_protocol::BridgeLifecycle::Open => "open",
        nakode_protocol::BridgeLifecycle::Archived => "archived",
    }
}

fn bridge_lifecycle_from_database(
    value: &str,
) -> Result<nakode_protocol::BridgeLifecycle, SessionError> {
    match value {
        "open" => Ok(nakode_protocol::BridgeLifecycle::Open),
        "archived" => Ok(nakode_protocol::BridgeLifecycle::Archived),
        _ => Err(SessionError::InvalidStoredValue {
            field: "session_bridges.lifecycle",
            value: value.to_owned(),
        }),
    }
}

fn bridge_continuation_disposition_value(
    disposition: nakode_protocol::BridgeContinuationDisposition,
) -> &'static str {
    match disposition {
        nakode_protocol::BridgeContinuationDisposition::Accepted => "accepted",
        nakode_protocol::BridgeContinuationDisposition::Duplicate => "duplicate",
        nakode_protocol::BridgeContinuationDisposition::Busy => "busy",
    }
}

fn parse_bridge_continuation_disposition(
    value: &str,
) -> Result<nakode_protocol::BridgeContinuationDisposition, SessionError> {
    match value {
        "accepted" => Ok(nakode_protocol::BridgeContinuationDisposition::Accepted),
        "duplicate" => Ok(nakode_protocol::BridgeContinuationDisposition::Duplicate),
        "busy" => Ok(nakode_protocol::BridgeContinuationDisposition::Busy),
        _ => Err(SessionError::InvalidStoredValue {
            field: "session_bridge_inbound_events.disposition",
            value: value.to_owned(),
        }),
    }
}

fn load_owner_prompts(
    connection: &Connection,
    session_id: &str,
) -> Result<Vec<PersistedOwnerPrompt>, SessionError> {
    let mut statement = connection.prepare(
        "SELECT prompt_id, raw_text, source_transport, dispatch_pending
         FROM accepted_owner_prompts
         WHERE session_id = ?1 ORDER BY acceptance_order",
    )?;
    statement
        .query_map([session_id], |row| {
            Ok(PersistedOwnerPrompt {
                prompt_id: row.get(0)?,
                raw_text: row.get(1)?,
                source_transport: row.get(2)?,
                dispatch_pending: row.get::<_, i64>(3)? != 0,
            })
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn load_owner_turns(
    connection: &Connection,
    session_id: &str,
) -> Result<Vec<PersistedTurnConfiguration>, SessionError> {
    let mut statement = connection.prepare(
        "SELECT turn_id, model, reasoning_effort, fast_mode, outcome
         FROM owner_turns WHERE session_id = ?1 ORDER BY rowid",
    )?;
    statement
        .query_map([session_id], |row| {
            let outcome = match row.get::<_, i64>(4)? {
                1 => TurnOutcome::Completed,
                2 => TurnOutcome::Interrupted,
                _ => TurnOutcome::Failed,
            };
            Ok(PersistedTurnConfiguration {
                id: row.get(0)?,
                model: row.get(1)?,
                options: ModelOptions {
                    reasoning_effort: row.get(2)?,
                    fast_mode: row.get::<_, i64>(3)? != 0,
                },
                outcome,
            })
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn load_owned_provider_sessions(
    connection: &Connection,
    parent_session_id: &str,
) -> Result<Vec<(String, String)>, SessionError> {
    let mut statement = connection.prepare(
        "SELECT provider, provider_session_id, ordinal
         FROM (
           SELECT provider, provider_session_id, 0 AS ordinal
           FROM session_native_history
           WHERE parent_session_id = ?1
           UNION
           SELECT provider, provider_session_id, 1 AS ordinal
           FROM orchestration_runs
           WHERE parent_session_id = ?1 AND provider_session_id IS NOT NULL
         )
         GROUP BY provider, provider_session_id
         ORDER BY MIN(ordinal), provider, provider_session_id",
    )?;
    statement
        .query_map([parent_session_id], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn apply_invocation_telemetry_migration(connection: &mut Connection) -> Result<(), SessionError> {
    connection.execute_batch(
        "CREATE TABLE IF NOT EXISTS nakode_schema_migrations (
           version INTEGER PRIMARY KEY,
           name TEXT NOT NULL,
           applied_at INTEGER NOT NULL
         );",
    )?;
    let applied = connection
        .query_row(
            "SELECT 1 FROM nakode_schema_migrations WHERE version = ?1",
            [INVOCATION_TELEMETRY_MIGRATION_VERSION],
            |row| row.get::<_, i64>(0),
        )
        .optional()?
        .is_some();
    if applied {
        return Ok(());
    }
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    transaction.execute_batch(
        "CREATE TABLE IF NOT EXISTS invocation_telemetry_settings (
           singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
           enabled INTEGER NOT NULL CHECK (enabled IN (0, 1))
         );
         CREATE TABLE IF NOT EXISTS invocation_events (
           invocation_key TEXT PRIMARY KEY,
           kind TEXT NOT NULL CHECK (kind IN ('archetype', 'skill')),
           identity TEXT NOT NULL,
           display_label TEXT NOT NULL,
           occurred_at_ms INTEGER NOT NULL
         );
         CREATE INDEX IF NOT EXISTS invocation_events_kind_time
           ON invocation_events(occurred_at_ms, kind);
         CREATE TABLE IF NOT EXISTS invocation_aggregates (
           kind TEXT NOT NULL CHECK (kind IN ('archetype', 'skill')),
           identity TEXT NOT NULL,
           display_label TEXT NOT NULL,
           invocation_count INTEGER NOT NULL,
           first_used_at_ms INTEGER NOT NULL,
           last_used_at_ms INTEGER NOT NULL,
           PRIMARY KEY(kind, identity)
         );",
    )?;
    transaction.execute(
        "INSERT OR IGNORE INTO nakode_schema_migrations (version, name, applied_at)
         VALUES (?1, 'opt-in invocation telemetry', unixepoch())",
        [INVOCATION_TELEMETRY_MIGRATION_VERSION],
    )?;
    transaction.commit()?;
    Ok(())
}

fn apply_subagent_transcript_boundary_migration(
    connection: &mut Connection,
) -> Result<(), SessionError> {
    let applied = connection
        .query_row(
            "SELECT 1 FROM nakode_schema_migrations WHERE version = ?1",
            [SUBAGENT_TRANSCRIPT_BOUNDARY_MIGRATION_VERSION],
            |row| row.get::<_, i64>(0),
        )
        .optional()?
        .is_some();
    if applied {
        return Ok(());
    }
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    // Runs written before the boundary bit was introduced may already contain only a retained
    // suffix. Mark every pre-migration row conservatively; new rows persist their exact flag.
    transaction.execute(
        "UPDATE orchestration_runs SET transcript_has_earlier = 1",
        [],
    )?;
    transaction.execute(
        "INSERT INTO nakode_schema_migrations (version, name, applied_at)
         VALUES (?1, 'subagent transcript completeness boundary', unixepoch())",
        [SUBAGENT_TRANSCRIPT_BOUNDARY_MIGRATION_VERSION],
    )?;
    transaction.commit()?;
    Ok(())
}

fn configure_connection(connection: &Connection) -> rusqlite::Result<()> {
    connection.busy_timeout(Duration::from_secs(5))?;
    execute_batch_with_busy_retry(connection, "PRAGMA journal_mode = WAL;")
}

fn execute_batch_with_busy_retry(
    connection: &Connection,
    statements: &str,
) -> rusqlite::Result<()> {
    const ATTEMPTS: usize = 100;
    const RETRY_DELAY: Duration = Duration::from_millis(25);

    for attempt in 0..ATTEMPTS {
        match connection.execute_batch(statements) {
            Ok(()) => return Ok(()),
            Err(error) if is_database_busy(&error) && attempt + 1 < ATTEMPTS => {
                std::thread::sleep(RETRY_DELAY);
            }
            Err(error) => return Err(error),
        }
    }
    unreachable!("the final database initialization attempt always returns")
}

fn table_row_count(
    transaction: &rusqlite::Transaction<'_>,
    table: &str,
) -> Result<usize, SessionError> {
    debug_assert!(matches!(
        table,
        "sessions" | "orchestration_runs" | "agent_turns" | "native_runtime_sessions"
    ));
    let count = transaction.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
        row.get::<_, i64>(0)
    })?;
    Ok(usize::try_from(count).unwrap_or(usize::MAX))
}

fn is_database_busy(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(details, _)
            if matches!(
                details.code,
                rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked
            )
    )
}

#[cfg(unix)]
fn protect_path(path: &Path, mode: u32) -> Result<(), SessionError> {
    use std::os::unix::fs::PermissionsExt;

    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).map_err(|source| {
        SessionError::ProtectStorage {
            path: path.display().to_string(),
            source,
        }
    })
}

#[cfg(not(unix))]
fn protect_path(_path: &Path, _mode: u32) -> Result<(), SessionError> {
    Ok(())
}

fn seed_provider_catalog(connection: &Connection) -> Result<(), SessionError> {
    connection.execute_batch(
        "CREATE TABLE IF NOT EXISTS addon_web_settings (
           singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
           backend TEXT NOT NULL,
           firecrawl_api_key TEXT NOT NULL
         );
         CREATE TABLE IF NOT EXISTS addon_memory_settings (
           singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
           backend TEXT NOT NULL,
           executable TEXT NOT NULL,
           global_bank TEXT NOT NULL,
           data_directory TEXT NOT NULL
         );
         CREATE TABLE IF NOT EXISTS addon_vision_settings (
           singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
           model TEXT
         );
         CREATE TABLE IF NOT EXISTS terminal_image_settings (
           singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
           mode TEXT NOT NULL
         );",
    )?;
    let provider_catalog =
        toml::from_str::<ProviderCatalog>(PROVIDER_CATALOG).map_err(|source| {
            SessionError::InvalidProviderCatalog {
                path: PROVIDER_CATALOG_PATH,
                source,
            }
        })?;
    for provider in provider_catalog.providers {
        connection.execute(
            "INSERT OR IGNORE INTO providers (provider, display_name, enabled, updated_at)
             VALUES (?1, ?2, 0, ?3)",
            params![provider.slug, provider.display_name, unix_timestamp()],
        )?;
    }
    connection.execute(
        "UPDATE providers SET enabled = 0
         WHERE enabled = 1
           AND provider NOT IN (SELECT provider FROM provider_credentials)",
        [],
    )?;
    Ok(())
}

#[allow(clippy::too_many_lines)]
fn save_subagent_transaction(
    transaction: &Transaction<'_>,
    record: &SubagentRecord,
    now: i64,
) -> Result<(), SessionError> {
    transaction.execute(
        "INSERT INTO orchestration_runs
           (parent_session_id, id, agent_slug, provider, model, provider_session_id,
            input_tokens, output_tokens, cached_input_tokens, cache_write_tokens, objective,
            status, latest_activity, parent_run_id, invocation_turn_id, invocation_call_id,
            originating_owner_entry_id, archetype_purpose, policy_json,
            remaining_delegation_depth, started_at_ms, ended_at_ms, termination_kind,
            termination_detail, objective_mismatch_handoff, salvage_json,
            continued_from_run_id, continued_by_run_id, continuation_depth, additional_turns,
            inherited_evidence_json, transcript_has_earlier, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15,
                 ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27, ?28, ?29,
                 ?30, ?31, ?32, ?33, ?33)
         ON CONFLICT(parent_session_id, id) DO UPDATE SET
           agent_slug = excluded.agent_slug,
           provider = excluded.provider,
           model = excluded.model,
           provider_session_id = excluded.provider_session_id,
           input_tokens = excluded.input_tokens,
           output_tokens = excluded.output_tokens,
           cached_input_tokens = excluded.cached_input_tokens,
           cache_write_tokens = excluded.cache_write_tokens,
           objective = excluded.objective,
           status = excluded.status,
           latest_activity = excluded.latest_activity,
           parent_run_id = excluded.parent_run_id,
           invocation_turn_id = excluded.invocation_turn_id,
           invocation_call_id = excluded.invocation_call_id,
           originating_owner_entry_id = excluded.originating_owner_entry_id,
           archetype_purpose = excluded.archetype_purpose,
           policy_json = excluded.policy_json,
           remaining_delegation_depth = excluded.remaining_delegation_depth,
           started_at_ms = excluded.started_at_ms,
           ended_at_ms = excluded.ended_at_ms,
           termination_kind = excluded.termination_kind,
           termination_detail = excluded.termination_detail,
           objective_mismatch_handoff = excluded.objective_mismatch_handoff,
           salvage_json = excluded.salvage_json,
           continued_from_run_id = excluded.continued_from_run_id,
           continued_by_run_id = excluded.continued_by_run_id,
           continuation_depth = excluded.continuation_depth,
           additional_turns = excluded.additional_turns,
           inherited_evidence_json = excluded.inherited_evidence_json,
           transcript_has_earlier = excluded.transcript_has_earlier,
           updated_at = excluded.updated_at",
        params![
            record.parent_session_id,
            record.id,
            record.agent,
            record.provider,
            record.model,
            record.provider_session_id,
            i64::try_from(record.input_tokens).unwrap_or(i64::MAX),
            i64::try_from(record.output_tokens).unwrap_or(i64::MAX),
            i64::try_from(record.cached_input_tokens).unwrap_or(i64::MAX),
            i64::try_from(record.cache_write_tokens).unwrap_or(i64::MAX),
            record.objective,
            record.status.database_value(),
            record.latest_activity,
            record.observability.parent_run_id,
            record.observability.invocation_turn_id,
            record.observability.invocation_call_id,
            record.observability.originating_owner_entry_id,
            record.observability.archetype_purpose,
            record.observability.policy_json,
            i64::from(record.observability.remaining_delegation_depth),
            i64::try_from(record.observability.started_at_ms).unwrap_or(i64::MAX),
            record
                .observability
                .ended_at_ms
                .map(|value| i64::try_from(value).unwrap_or(i64::MAX)),
            record.observability.termination_kind,
            record.observability.termination_detail,
            record.observability.objective_mismatch_handoff,
            record
                .observability
                .salvage
                .as_ref()
                .map(|salvage| serde_json::to_string(salvage).expect("salvage serializes")),
            record.observability.continued_from_run_id,
            record.observability.continued_by_run_id,
            i64::from(record.observability.continuation_depth),
            record.observability.additional_turns.map(i64::from),
            serde_json::to_string(&record.observability.inherited_evidence)
                .expect("inherited evidence serializes"),
            i64::from(record.transcript_has_earlier),
            now,
        ],
    )?;
    transaction.execute(
        "DELETE FROM agent_turns WHERE parent_session_id = ?1 AND run_id = ?2",
        params![record.parent_session_id, record.id],
    )?;
    let mut statement = transaction.prepare(
        "INSERT INTO agent_turns
           (parent_session_id, run_id, sequence, entry_id, item_key, kind, title, body, status,
            provider_id, model_id, tool_audit_json, created_at_ms, owner_turn_id, reasoning_effort,
            fast_mode, source_transport)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15,
                 ?16, ?17)",
    )?;
    for (sequence, entry) in record.transcript.iter().enumerate() {
        let sequence = i64::try_from(sequence).unwrap_or(i64::MAX);
        statement.execute(params![
            record.parent_session_id,
            record.id,
            sequence,
            entry.id,
            entry.key,
            entry_kind_value(entry.kind),
            entry.title,
            entry.body,
            entry_status_value(entry.status),
            entry.provider_id,
            entry.model_id,
            entry.tool_audit_json,
            entry
                .created_at_ms
                .and_then(|value| i64::try_from(value).ok()),
            entry.owner_turn_id,
            entry.reasoning_effort,
            entry.fast_mode.map(i64::from),
            entry.source_transport,
        ])?;
    }
    Ok(())
}

fn bind_session_skill_profile_in_transaction(
    transaction: &Transaction<'_>,
    session_id: &str,
    profile_id: &str,
) -> Result<(), SessionError> {
    transaction.execute(
        "INSERT INTO session_skill_profiles (session_id, profile_id) VALUES (?1, ?2)
         ON CONFLICT(session_id) DO UPDATE SET profile_id = excluded.profile_id
         WHERE session_skill_profiles.profile_id = excluded.profile_id",
        params![session_id, profile_id],
    )?;
    let persisted = transaction.query_row(
        "SELECT profile_id FROM session_skill_profiles WHERE session_id = ?1",
        [session_id],
        |row| row.get::<_, String>(0),
    )?;
    if persisted != profile_id {
        return Err(SessionError::InvalidStoredValue {
            field: "session_skill_profiles.profile_id",
            value: persisted,
        });
    }
    Ok(())
}

impl SessionRepository for SqliteSessionRepository {
    fn session_skill_profile(&self, session_id: &str) -> Result<Option<String>, SessionError> {
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        connection
            .query_row(
                "SELECT profile_id FROM session_skill_profiles WHERE session_id = ?1",
                [session_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(Into::into)
    }

    fn bind_session_skill_profile(
        &self,
        session_id: &str,
        profile_id: &str,
    ) -> Result<(), SessionError> {
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        connection.execute(
            "INSERT INTO session_skill_profiles (session_id, profile_id) VALUES (?1, ?2)
             ON CONFLICT(session_id) DO UPDATE SET profile_id = excluded.profile_id
             WHERE session_skill_profiles.profile_id = excluded.profile_id",
            params![session_id, profile_id],
        )?;
        let persisted = connection.query_row(
            "SELECT profile_id FROM session_skill_profiles WHERE session_id = ?1",
            [session_id],
            |row| row.get::<_, String>(0),
        )?;
        if persisted != profile_id {
            return Err(SessionError::InvalidStoredValue {
                field: "session_skill_profiles.profile_id",
                value: persisted,
            });
        }
        Ok(())
    }

    fn list_skill_preferences(
        &self,
        profile_id: &str,
    ) -> Result<Vec<SkillPreference>, SessionError> {
        let connection = self.connection.lock().expect("session database lock");
        let mut statement = connection.prepare(
            "SELECT profile_id, skill_id, last_name, last_description, enabled
             FROM skill_preferences WHERE profile_id = ?1
             ORDER BY last_name COLLATE NOCASE, skill_id",
        )?;
        let rows = statement.query_map(params![profile_id], |row| {
            Ok(SkillPreference {
                profile_id: row.get(0)?,
                skill_id: row.get(1)?,
                last_name: row.get(2)?,
                last_description: row.get(3)?,
                enabled: row.get::<_, i64>(4)? != 0,
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(SessionError::from)
    }

    fn list_all_skill_preferences(&self) -> Result<Vec<SkillPreference>, SessionError> {
        let connection = self.connection.lock().expect("session database lock");
        let mut statement = connection.prepare(
            "SELECT profile_id, skill_id, last_name, last_description, enabled
             FROM skill_preferences ORDER BY profile_id, last_name COLLATE NOCASE, skill_id",
        )?;
        let rows = statement.query_map([], |row| {
            Ok(SkillPreference {
                profile_id: row.get(0)?,
                skill_id: row.get(1)?,
                last_name: row.get(2)?,
                last_description: row.get(3)?,
                enabled: row.get::<_, i64>(4)? != 0,
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(SessionError::from)
    }

    fn set_skill_preference(&self, preference: &SkillPreference) -> Result<(), SessionError> {
        let connection = self.connection.lock().expect("session database lock");
        connection.execute(
            "INSERT INTO skill_preferences
               (profile_id, skill_id, last_name, last_description, enabled, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, unixepoch())
             ON CONFLICT(profile_id, skill_id) DO UPDATE SET
               last_name = excluded.last_name,
               last_description = excluded.last_description,
               enabled = excluded.enabled,
               updated_at = excluded.updated_at",
            params![
                preference.profile_id,
                preference.skill_id,
                preference.last_name,
                preference.last_description,
                i64::from(preference.enabled),
            ],
        )?;
        Ok(())
    }

    fn set_skill_preferences(&self, preferences: &[SkillPreference]) -> Result<(), SessionError> {
        let mut connection = self.connection.lock().expect("session database lock");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        for preference in preferences {
            transaction.execute(
                "INSERT INTO skill_preferences
                   (profile_id, skill_id, last_name, last_description, enabled, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, unixepoch())
                 ON CONFLICT(profile_id, skill_id) DO UPDATE SET
                   last_name = excluded.last_name,
                   last_description = excluded.last_description,
                   enabled = excluded.enabled,
                   updated_at = excluded.updated_at",
                params![
                    preference.profile_id,
                    preference.skill_id,
                    preference.last_name,
                    preference.last_description,
                    i64::from(preference.enabled),
                ],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    fn prune_unavailable_skill(
        &self,
        profile_id: &str,
        skill_id: &str,
    ) -> Result<crate::skill::SkillPruneReport, SessionError> {
        let connection = self.connection.lock().expect("session database lock");
        let preference_count = connection.execute(
            "DELETE FROM skill_preferences WHERE profile_id = ?1 AND skill_id = ?2",
            params![profile_id, skill_id],
        )?;
        Ok(crate::skill::SkillPruneReport { preference_count })
    }

    fn list_session_bridges(
        &self,
        workspace: &str,
    ) -> Result<Vec<SessionBridgeRecord>, SessionError> {
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let mut statement = connection.prepare(
            "SELECT session_id, workspace, kind, lifecycle, display_title, revision, transport,
                    external_parent_id, external_thread_id, last_delivered_turn_id,
                    last_delivered_kind, delivery_json, live_turn_id, live_external_message_id,
                    active_source_message_id, pending_inbound_json, inbound_turn_origins_json,
                    updated_at_ms
             FROM session_bridges WHERE workspace = ?1 ORDER BY updated_at_ms, session_id",
        )?;
        statement
            .query_map([workspace], Self::bridge_row)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    fn list_session_bridges_all(&self) -> Result<Vec<SessionBridgeRecord>, SessionError> {
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let mut statement = connection.prepare(
            "SELECT session_id, workspace, kind, lifecycle, display_title, revision, transport,
                    external_parent_id, external_thread_id, last_delivered_turn_id,
                    last_delivered_kind, delivery_json, live_turn_id, live_external_message_id,
                    active_source_message_id, pending_inbound_json, inbound_turn_origins_json,
                    updated_at_ms
             FROM session_bridges ORDER BY updated_at_ms, session_id",
        )?;
        statement
            .query_map([], Self::bridge_row)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    fn save_session_bridge(&self, bridge: &SessionBridgeRecord) -> Result<(), SessionError> {
        let mut connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let transaction = connection.transaction()?;
        save_session_bridge_on(&transaction, bridge)?;
        transaction.commit()?;
        Ok(())
    }

    fn save_session_bridges(&self, bridges: &[SessionBridgeRecord]) -> Result<(), SessionError> {
        if bridges.is_empty() {
            return Ok(());
        }
        let mut connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let transaction = connection.transaction()?;
        for bridge in bridges {
            save_session_bridge_on(&transaction, bridge)?;
        }
        transaction.commit()?;
        Ok(())
    }

    fn save_session_bridges_with_inbound_event(
        &self,
        bridges: &[SessionBridgeRecord],
        session_id: &str,
        external_event_id: &str,
        disposition: nakode_protocol::BridgeContinuationDisposition,
    ) -> Result<(), SessionError> {
        let mut connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        protect_bridge_pending_inbound_on(&transaction, bridges)?;
        for bridge in bridges {
            save_session_bridge_on(&transaction, bridge)?;
        }
        save_bridge_inbound_event_on(
            &transaction,
            bridges,
            session_id,
            external_event_id,
            disposition,
        )?;
        transaction.commit()?;
        Ok(())
    }

    fn find_session_bridge_inbound_event(
        &self,
        session_id: &str,
        external_event_id: &str,
    ) -> Result<Option<nakode_protocol::BridgeContinuationDisposition>, SessionError> {
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let value = connection
            .query_row(
                "SELECT disposition FROM session_bridge_inbound_events
                 WHERE session_id = ?1 AND external_event_id = ?2",
                params![session_id, external_event_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        value
            .map(|value| parse_bridge_continuation_disposition(&value))
            .transpose()
    }

    fn list_recent(
        &self,
        workspace: &str,
        limit: usize,
    ) -> Result<Vec<SessionRecord>, SessionError> {
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let mut statement = connection.prepare(
            "SELECT id, provider, provider_session_id, workspace, title, model, model_reasoning_effort, model_fast_mode, last_turn_id, last_turn_model, last_turn_reasoning_effort, last_turn_fast_mode, last_turn_outcome, created_at, updated_at, COALESCE(working_directory, workspace), last_owner_activity_at, enabled_skill_ids_json, account_id, code_mode
             FROM sessions WHERE workspace = ?1 ORDER BY updated_at DESC LIMIT ?2",
        )?;
        let bounded_limit = i64::try_from(limit.min(500)).expect("limit is at most 500");
        let rows = statement.query_map(params![workspace, bounded_limit], Self::row)?;
        let mut records = rows.collect::<Result<Vec<_>, _>>()?;
        drop(statement);
        for record in &mut records {
            record.owner_turns = load_owner_turns(&connection, &record.id)?;
            record.owner_prompts = load_owner_prompts(&connection, &record.id)?;
            record.owned_provider_sessions = load_owned_provider_sessions(&connection, &record.id)?;
        }
        Ok(records)
    }

    fn list_recent_all(&self) -> Result<Vec<SessionRecord>, SessionError> {
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let mut statement = connection.prepare(
            "SELECT id, provider, provider_session_id, workspace, title, model, model_reasoning_effort, model_fast_mode, last_turn_id, last_turn_model, last_turn_reasoning_effort, last_turn_fast_mode, last_turn_outcome, created_at, updated_at, COALESCE(working_directory, workspace), last_owner_activity_at, enabled_skill_ids_json, account_id, code_mode
             FROM sessions ORDER BY updated_at DESC",
        )?;
        let rows = statement.query_map([], Self::row)?;
        let mut records = rows.collect::<Result<Vec<_>, _>>()?;
        drop(statement);
        for record in &mut records {
            record.owner_turns = load_owner_turns(&connection, &record.id)?;
            record.owner_prompts = load_owner_prompts(&connection, &record.id)?;
            record.owned_provider_sessions = load_owned_provider_sessions(&connection, &record.id)?;
        }
        Ok(records)
    }

    fn find(&self, id: &str) -> Result<Option<SessionRecord>, SessionError> {
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let exact = connection
            .query_row(
                "SELECT id, provider, provider_session_id, workspace, title, model, model_reasoning_effort, model_fast_mode, last_turn_id, last_turn_model, last_turn_reasoning_effort, last_turn_fast_mode, last_turn_outcome, created_at, updated_at, COALESCE(working_directory, workspace), last_owner_activity_at, enabled_skill_ids_json, account_id, code_mode
                 FROM sessions WHERE id = ?1",
                [id],
                Self::row,
            )
            .optional()?;
        if let Some(mut exact) = exact {
            exact.owner_turns = load_owner_turns(&connection, &exact.id)?;
            exact.owner_prompts = load_owner_prompts(&connection, &exact.id)?;
            exact.owned_provider_sessions = load_owned_provider_sessions(&connection, &exact.id)?;
            return Ok(Some(exact));
        }
        let mut statement = connection.prepare(
            "SELECT id, provider, provider_session_id, workspace, title, model, model_reasoning_effort, model_fast_mode, last_turn_id, last_turn_model, last_turn_reasoning_effort, last_turn_fast_mode, last_turn_outcome, created_at, updated_at, COALESCE(working_directory, workspace), last_owner_activity_at, enabled_skill_ids_json, account_id, code_mode
             FROM sessions
             WHERE substr(id, 1, length(?1)) = ?1
             ORDER BY updated_at DESC LIMIT 2",
        )?;
        let matches = statement
            .query_map([id], Self::row)?
            .collect::<Result<Vec<_>, _>>()?;
        match matches.as_slice() {
            [] => Ok(None),
            [record] => {
                let mut record = record.clone();
                record.owner_turns = load_owner_turns(&connection, &record.id)?;
                record.owner_prompts = load_owner_prompts(&connection, &record.id)?;
                record.owned_provider_sessions =
                    load_owned_provider_sessions(&connection, &record.id)?;
                Ok(Some(record))
            }
            _ => Err(SessionError::Ambiguous(id.to_owned())),
        }
    }

    fn create_with_id(
        &self,
        id: &str,
        provider: &str,
        provider_session_id: &str,
        workspace: &str,
        working_directory: &str,
        title: &str,
        model: Option<&str>,
        options: &ModelOptions,
        enabled_skill_ids: Option<&[String]>,
    ) -> Result<SessionRecord, SessionError> {
        let now = unix_timestamp();
        let title = title.lines().next().unwrap_or("New session").trim();
        let title = if title.is_empty() {
            "New session"
        } else {
            title
        };
        let enabled_skill_ids_json = enabled_skill_ids
            .map(serde_json::to_string)
            .transpose()
            .map_err(|source| SessionError::InvalidStoredJson {
                field: "sessions.enabled_skill_ids_json",
                source,
            })?;
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        connection.execute(
            "INSERT INTO sessions
             (id, provider, provider_session_id, workspace, working_directory, title, model, model_reasoning_effort, model_fast_mode, created_at, updated_at, last_owner_activity_at, enabled_skill_ids_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?10, ?10, ?11)
             ON CONFLICT(provider, provider_session_id) DO UPDATE SET
               model = excluded.model,
               model_reasoning_effort = excluded.model_reasoning_effort,
               model_fast_mode = excluded.model_fast_mode,
               enabled_skill_ids_json = COALESCE(
                 sessions.enabled_skill_ids_json,
                 excluded.enabled_skill_ids_json
               ),
               updated_at = excluded.updated_at,
               last_owner_activity_at = MAX(
                 COALESCE(sessions.last_owner_activity_at, 0),
                 excluded.last_owner_activity_at
               )",
            params![
                id,
                provider,
                provider_session_id,
                workspace,
                working_directory,
                title,
                model,
                options.reasoning_effort,
                i64::from(options.fast_mode),
                now,
                enabled_skill_ids_json,
            ],
        )?;
        connection.query_row(
            "SELECT id, provider, provider_session_id, workspace, title, model, model_reasoning_effort, model_fast_mode, last_turn_id, last_turn_model, last_turn_reasoning_effort, last_turn_fast_mode, last_turn_outcome, created_at, updated_at, COALESCE(working_directory, workspace), last_owner_activity_at, enabled_skill_ids_json, account_id, code_mode
             FROM sessions WHERE provider = ?1 AND provider_session_id = ?2",
            params![provider, provider_session_id],
            Self::row,
        ).map_err(Into::into)
    }

    #[allow(clippy::too_many_arguments)]
    fn create_with_account_id(
        &self,
        id: &str,
        provider: &str,
        account_id: Option<&str>,
        provider_session_id: &str,
        workspace: &str,
        working_directory: &str,
        title: &str,
        model: Option<&str>,
        options: &ModelOptions,
        enabled_skill_ids: Option<&[String]>,
    ) -> Result<SessionRecord, SessionError> {
        self.create_with_account_id_and_skill_profile(
            id,
            provider,
            account_id,
            provider_session_id,
            workspace,
            working_directory,
            title,
            model,
            options,
            enabled_skill_ids,
            None,
            None,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn create_with_account_id_and_skill_profile(
        &self,
        id: &str,
        provider: &str,
        account_id: Option<&str>,
        provider_session_id: &str,
        workspace: &str,
        working_directory: &str,
        title: &str,
        model: Option<&str>,
        options: &ModelOptions,
        enabled_skill_ids: Option<&[String]>,
        profile_id: Option<&str>,
        code_mode: Option<bool>,
        owner_prompt: Option<&PersistedOwnerPrompt>,
    ) -> Result<SessionRecord, SessionError> {
        let now = unix_timestamp();
        let title = title.lines().next().unwrap_or("New session").trim();
        let title = if title.is_empty() {
            "New session"
        } else {
            title
        };
        let enabled_skill_ids_json = enabled_skill_ids
            .map(serde_json::to_string)
            .transpose()
            .map_err(|source| SessionError::InvalidStoredJson {
                field: "sessions.enabled_skill_ids_json",
                source,
            })?;
        let mut connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(account_id) = account_id {
            let valid = transaction.query_row(
                "SELECT EXISTS(SELECT 1 FROM provider_accounts
                 WHERE account_id = ?1 AND provider = ?2)",
                params![account_id, provider],
                |row| row.get::<_, bool>(0),
            )?;
            if !valid {
                return Err(SessionError::ProviderAccountNotFound {
                    provider: provider.to_owned(),
                    account_id: account_id.to_owned(),
                });
            }
        }
        transaction.execute(
            "INSERT INTO sessions
             (id, provider, account_id, provider_session_id, workspace, working_directory,
              title, model, model_reasoning_effort, model_fast_mode, created_at, updated_at,
              last_owner_activity_at, enabled_skill_ids_json, code_mode)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?11, ?11, ?12,
                     COALESCE(?13, 0))
             ON CONFLICT(provider, provider_session_id) DO UPDATE SET
               account_id = COALESCE(sessions.account_id, excluded.account_id),
               code_mode = COALESCE(?13, sessions.code_mode),
               model = excluded.model,
               model_reasoning_effort = excluded.model_reasoning_effort,
               model_fast_mode = excluded.model_fast_mode,
               enabled_skill_ids_json = COALESCE(
                 sessions.enabled_skill_ids_json,
                 excluded.enabled_skill_ids_json
               ),
               updated_at = excluded.updated_at,
               last_owner_activity_at = MAX(
                 COALESCE(sessions.last_owner_activity_at, 0),
                 excluded.last_owner_activity_at
               )
             WHERE sessions.account_id IS NULL OR excluded.account_id IS NULL
                OR sessions.account_id = excluded.account_id",
            params![
                id,
                provider,
                account_id,
                provider_session_id,
                workspace,
                working_directory,
                title,
                model,
                options.reasoning_effort,
                i64::from(options.fast_mode),
                now,
                enabled_skill_ids_json,
                code_mode.map(i64::from),
            ],
        )?;
        let record = transaction.query_row(
            "SELECT id, provider, provider_session_id, workspace, title, model,
                    model_reasoning_effort, model_fast_mode, last_turn_id, last_turn_model,
                    last_turn_reasoning_effort, last_turn_fast_mode, last_turn_outcome,
                    created_at, updated_at, COALESCE(working_directory, workspace),
                    last_owner_activity_at, enabled_skill_ids_json, account_id, code_mode
             FROM sessions WHERE provider = ?1 AND provider_session_id = ?2",
            params![provider, provider_session_id],
            Self::row,
        )?;
        if let (Some(requested), Some(persisted)) = (account_id, record.account_id.as_deref())
            && requested != persisted
        {
            return Err(SessionError::ProviderAccountAffinityConflict {
                session_id: record.id.clone(),
                account_id: persisted.to_owned(),
            });
        }
        if let Some(profile_id) = profile_id {
            bind_session_skill_profile_in_transaction(&transaction, &record.id, profile_id)?;
        }
        if let Some(prompt) = owner_prompt {
            record_owner_prompt_on(&transaction, &record.id, prompt)?;
        }
        transaction.commit()?;
        Ok(record)
    }

    fn set_session_account(&self, id: &str, account_id: Option<&str>) -> Result<(), SessionError> {
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let updated = connection.execute(
            "UPDATE sessions SET account_id = ?2 WHERE id = ?1
             AND (account_id IS NULL OR account_id = ?2)",
            params![id, account_id],
        )?;
        if updated == 0 {
            let persisted = connection
                .query_row(
                    "SELECT account_id FROM sessions WHERE id = ?1",
                    [id],
                    |row| row.get::<_, Option<String>>(0),
                )
                .optional()?;
            return match persisted {
                Some(Some(account_id)) => Err(SessionError::ProviderAccountAffinityConflict {
                    session_id: id.to_owned(),
                    account_id,
                }),
                None | Some(None) => Err(SessionError::SessionNotFound(id.to_owned())),
            };
        }
        Ok(())
    }

    fn set_session_code_mode(&self, id: &str, enabled: bool) -> Result<(), SessionError> {
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let changed = connection.execute(
            "UPDATE sessions SET code_mode = ?2, updated_at = ?3 WHERE id = ?1",
            params![id, i64::from(enabled), unix_timestamp()],
        )?;
        if changed == 0 {
            return Err(SessionError::SessionNotFound(id.to_owned()));
        }
        Ok(())
    }

    fn set_session_skill_snapshot(
        &self,
        id: &str,
        enabled_skill_ids: &[String],
    ) -> Result<(), SessionError> {
        let encoded = serde_json::to_string(enabled_skill_ids).map_err(|source| {
            SessionError::InvalidStoredJson {
                field: "sessions.enabled_skill_ids_json",
                source,
            }
        })?;
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let changed = connection.execute(
            "UPDATE sessions SET enabled_skill_ids_json = ?2
             WHERE id = ?1 AND enabled_skill_ids_json IS NULL",
            params![id, encoded],
        )?;
        if changed == 0 {
            let exists = connection.query_row(
                "SELECT EXISTS(SELECT 1 FROM sessions WHERE id = ?1)",
                [id],
                |row| row.get::<_, bool>(0),
            )?;
            if !exists {
                return Err(SessionError::SessionNotFound(id.to_owned()));
            }
        }
        Ok(())
    }

    fn transition_primary(
        &self,
        id: &str,
        provider: &str,
        provider_session_id: &str,
        model: Option<&str>,
        options: &ModelOptions,
    ) -> Result<(), SessionError> {
        let mut connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let transaction = connection.transaction()?;
        let current = transaction
            .query_row(
                "SELECT provider, provider_session_id FROM sessions WHERE id = ?1",
                [id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?
            .ok_or_else(|| SessionError::SessionNotFound(id.to_owned()))?;
        transaction.execute(
            "DELETE FROM session_native_history
             WHERE parent_session_id = ?1 AND provider = ?2 AND provider_session_id = ?3",
            params![id, provider, provider_session_id],
        )?;
        if !is_pending_provider_session_id(&current.1) {
            transaction.execute(
                "INSERT OR IGNORE INTO session_native_history
                 (parent_session_id, provider, provider_session_id)
                 VALUES (?1, ?2, ?3)",
                params![id, current.0, current.1],
            )?;
        }
        transaction.execute(
            "UPDATE sessions
             SET provider = ?1, provider_session_id = ?2, model = ?3,
                 model_reasoning_effort = ?4, model_fast_mode = ?5, updated_at = ?6
             WHERE id = ?7",
            params![
                provider,
                provider_session_id,
                model,
                options.reasoning_effort,
                i64::from(options.fast_mode),
                unix_timestamp(),
                id
            ],
        )?;
        transaction.commit()?;
        Ok(())
    }

    fn transition_primary_with_account(
        &self,
        id: &str,
        provider: &str,
        account_id: Option<&str>,
        provider_session_id: &str,
        model: Option<&str>,
        options: &ModelOptions,
    ) -> Result<(), SessionError> {
        let mut connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let transaction = connection.transaction()?;
        if let Some(account_id) = account_id {
            let valid = transaction.query_row(
                "SELECT EXISTS(SELECT 1 FROM provider_accounts
                 WHERE account_id = ?1 AND provider = ?2)",
                params![account_id, provider],
                |row| row.get::<_, bool>(0),
            )?;
            if !valid {
                return Err(SessionError::ProviderAccountNotFound {
                    provider: provider.to_owned(),
                    account_id: account_id.to_owned(),
                });
            }
        }
        let current = transaction
            .query_row(
                "SELECT provider, provider_session_id, account_id FROM sessions WHERE id = ?1",
                [id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| SessionError::SessionNotFound(id.to_owned()))?;
        if current.0 == provider
            && let (Some(requested), Some(persisted)) = (account_id, current.2.as_deref())
            && requested != persisted
        {
            return Err(SessionError::ProviderAccountAffinityConflict {
                session_id: id.to_owned(),
                account_id: persisted.to_owned(),
            });
        }
        transaction.execute(
            "DELETE FROM session_native_history
             WHERE parent_session_id = ?1 AND provider = ?2 AND provider_session_id = ?3",
            params![id, provider, provider_session_id],
        )?;
        if !is_pending_provider_session_id(&current.1) {
            transaction.execute(
                "INSERT OR IGNORE INTO session_native_history
                 (parent_session_id, provider, provider_session_id)
                 VALUES (?1, ?2, ?3)",
                params![id, current.0, current.1],
            )?;
        }
        transaction.execute(
            "UPDATE sessions
             SET provider = ?1,
                 account_id = CASE
                   WHEN provider != ?1 THEN ?2
                   ELSE COALESCE(account_id, ?2)
                 END,
                 provider_session_id = ?3, model = ?4,
                 model_reasoning_effort = ?5, model_fast_mode = ?6, updated_at = ?7
             WHERE id = ?8",
            params![
                provider,
                account_id,
                provider_session_id,
                model,
                options.reasoning_effort,
                i64::from(options.fast_mode),
                unix_timestamp(),
                id
            ],
        )?;
        transaction.commit()?;
        Ok(())
    }

    fn touch(&self, id: &str) -> Result<(), SessionError> {
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let updated = connection.execute(
            "UPDATE sessions SET updated_at = ?1 WHERE id = ?2",
            params![unix_timestamp(), id],
        )?;
        if updated == 0 {
            return Err(SessionError::SessionNotFound(id.to_owned()));
        }
        Ok(())
    }

    fn delete(&self, id: &str) -> Result<(), SessionError> {
        // Resolve prefixes exactly as every other session operation does. A fully absent id is an
        // idempotent success, but an ambiguous prefix remains an error and must never pick a victim.
        let record = self.find(id)?;
        let resolved_id = record
            .as_ref()
            .map_or_else(|| id.to_owned(), |record| record.id.clone());
        let mut connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let transaction = connection.transaction()?;
        // Bridge identity is intentionally retained for thread history, but deletion is its terminal
        // lifecycle boundary. Archive it in this transaction before deleting the session hierarchy so
        // repository failure can roll back both pieces of authority together.
        transaction.execute(
            "UPDATE session_bridges
             SET lifecycle = 'archived', revision = revision + 1, updated_at_ms = ?1
             WHERE session_id = ?2 AND lifecycle != 'archived'",
            params![unix_timestamp().saturating_mul(1_000), resolved_id],
        )?;
        if let Some(record) = record {
            // The transcript first. It is keyed by the PROVIDER session id and has no foreign key to
            // `sessions`, so nothing removes it on our behalf; dropping the parent row first would leave it
            // unreachable and permanent — which is the orphan this table already accumulates.
            transaction.execute(
                "DELETE FROM native_runtime_sessions WHERE provider = ?1 AND session_id = ?2",
                params![record.provider, record.provider_session_id],
            )?;
            for (provider, provider_session_id) in &record.owned_provider_sessions {
                transaction.execute(
                    "DELETE FROM native_runtime_sessions WHERE provider = ?1 AND session_id = ?2",
                    params![provider, provider_session_id],
                )?;
            }
            // This normalized replay ledger predates a foreign key so remove its session-owned rows
            // explicitly; otherwise individual session deletion leaves permanent idempotency orphans.
            transaction.execute(
                "DELETE FROM session_bridge_inbound_events WHERE session_id = ?1",
                [&record.id],
            )?;
            transaction.execute(
                "DELETE FROM accepted_owner_prompts WHERE session_id = ?1",
                [&record.id],
            )?;
            // Then the session itself. `orchestration_runs` and `agent_turns` go with it by cascade, which
            // the connection's `PRAGMA foreign_keys = ON` is what makes true.
            transaction.execute("DELETE FROM sessions WHERE id = ?1", params![record.id])?;
        }
        transaction.commit()?;
        Ok(())
    }

    fn purge_all(&self) -> Result<SessionPurgeReport, SessionError> {
        let mut connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let transaction = connection.transaction()?;
        let report = SessionPurgeReport {
            sessions: table_row_count(&transaction, "sessions")?,
            orchestration_runs: table_row_count(&transaction, "orchestration_runs")?,
            agent_turns: table_row_count(&transaction, "agent_turns")?,
            native_runtime_sessions: table_row_count(&transaction, "native_runtime_sessions")?,
        };
        // Runtime histories intentionally have no parent foreign key and may exist without a complete
        // logical session. Clear the authoritative session-runtime table directly before cascading the
        // logical session hierarchy.
        transaction.execute("DELETE FROM native_runtime_sessions", [])?;
        transaction.execute("DELETE FROM accepted_owner_prompts", [])?;
        transaction.execute("DELETE FROM session_bridge_inbound_events", [])?;
        transaction.execute("DELETE FROM session_bridges", [])?;
        transaction.execute("DELETE FROM invocation_events", [])?;
        transaction.execute("DELETE FROM invocation_aggregates", [])?;
        transaction.execute("DELETE FROM sessions", [])?;
        transaction.commit()?;
        Ok(report)
    }

    fn update_model(
        &self,
        id: &str,
        model: Option<&str>,
        options: &ModelOptions,
    ) -> Result<(), SessionError> {
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let updated = connection.execute(
            "UPDATE sessions
             SET model = ?1, model_reasoning_effort = ?2, model_fast_mode = ?3, updated_at = ?4
             WHERE id = ?5",
            params![
                model,
                options.reasoning_effort,
                i64::from(options.fast_mode),
                unix_timestamp(),
                id
            ],
        )?;
        if updated == 0 {
            return Err(SessionError::SessionNotFound(id.to_owned()));
        }
        Ok(())
    }

    fn record_owner_activity(&self, id: &str) -> Result<(), SessionError> {
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let observed_at = unix_timestamp();
        let updated = connection.execute(
            "UPDATE sessions
             SET last_owner_activity_at = MAX(
               COALESCE(last_owner_activity_at, 0),
               ?1
             )
             WHERE id = ?2",
            params![observed_at, id],
        )?;
        if updated == 0 {
            return Err(SessionError::SessionNotFound(id.to_owned()));
        }
        Ok(())
    }

    fn record_owner_prompt(
        &self,
        session_id: &str,
        prompt: &PersistedOwnerPrompt,
    ) -> Result<(), SessionError> {
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        record_owner_prompt_on(&connection, session_id, prompt)
    }

    fn acknowledge_owner_prompt_dispatch(
        &self,
        session_id: &str,
        prompt_id: &str,
    ) -> Result<(), SessionError> {
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let updated = connection.execute(
            "UPDATE accepted_owner_prompts SET dispatch_pending = 0
             WHERE session_id = ?1 AND prompt_id = ?2",
            params![session_id, prompt_id],
        )?;
        if updated == 0 {
            return Err(SessionError::SessionNotFound(format!(
                "{session_id}/owner-prompt/{prompt_id}"
            )));
        }
        Ok(())
    }

    fn save_prompt_dispatch_checkpoint(
        &self,
        owner_prompts: &[(String, PersistedOwnerPrompt)],
        bridges: &[SessionBridgeRecord],
        inbound_event: Option<(&str, &str, nakode_protocol::BridgeContinuationDisposition)>,
    ) -> Result<(), SessionError> {
        let mut connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        for (session_id, prompt) in owner_prompts {
            record_owner_prompt_on(&transaction, session_id, prompt)?;
        }
        for bridge in bridges {
            save_session_bridge_on(&transaction, bridge)?;
        }
        if let Some((session_id, external_event_id, disposition)) = inbound_event {
            save_bridge_inbound_event_on(
                &transaction,
                bridges,
                session_id,
                external_event_id,
                disposition,
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    fn update_last_turn(
        &self,
        id: &str,
        turn: &PersistedTurnConfiguration,
    ) -> Result<(), SessionError> {
        let mut connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let transaction = connection.transaction()?;
        let observed_at = unix_timestamp();
        let (outcome, outcome_code) = match turn.outcome {
            TurnOutcome::Completed => ("completed", 1_i64),
            TurnOutcome::Interrupted => ("interrupted", 2_i64),
            TurnOutcome::Failed => ("failed", 3_i64),
        };
        let updated = transaction.execute(
            "UPDATE sessions
             SET last_turn_id = ?1, last_turn_model = ?2, last_turn_reasoning_effort = ?3,
                 last_turn_fast_mode = ?4, last_turn_outcome = ?5, updated_at = ?6,
                 last_owner_activity_at = MAX(COALESCE(last_owner_activity_at, 0), ?6)
             WHERE id = ?7",
            params![
                turn.id,
                turn.model,
                turn.options.reasoning_effort,
                i64::from(turn.options.fast_mode),
                outcome,
                observed_at,
                id
            ],
        )?;
        if updated == 0 {
            return Err(SessionError::SessionNotFound(id.to_owned()));
        }
        transaction.execute(
            "INSERT INTO owner_turns
             (session_id, turn_id, model, reasoning_effort, fast_mode, outcome)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(session_id, turn_id) DO NOTHING",
            params![
                id,
                turn.id,
                turn.model,
                turn.options.reasoning_effort,
                i64::from(turn.options.fast_mode),
                outcome_code
            ],
        )?;
        transaction.commit()?;
        Ok(())
    }

    fn list_models(&self, provider: &str) -> Result<Vec<ModelInfo>, SessionError> {
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let mut statement = connection.prepare(
            "SELECT model_id, is_default, capabilities
             FROM provider_models WHERE provider = ?1
             ORDER BY is_default DESC, model_id COLLATE NOCASE",
        )?;
        let model_provider = provider.to_owned();
        let rows = statement.query_map([provider], |row| {
            Ok(ModelInfo {
                provider: model_provider.clone(),
                id: row.get(0)?,
                is_default: row.get::<_, i64>(1)? != 0,
                capabilities: row
                    .get::<_, String>(2)
                    .ok()
                    .and_then(|encoded| serde_json::from_str(&encoded).ok())
                    .unwrap_or_default(),
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    fn replace_models(&self, provider: &str, models: &[ModelInfo]) -> Result<(), SessionError> {
        let mut connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let transaction = connection.transaction()?;
        let preferred = transaction
            .query_row(
                "SELECT model_id FROM provider_model_preferences WHERE provider = ?1",
                [provider],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .filter(|preferred| models.iter().any(|model| model.id == *preferred));
        transaction.execute(
            "DELETE FROM provider_models WHERE provider = ?1",
            [provider],
        )?;
        let now = unix_timestamp();
        {
            let mut statement = transaction.prepare(
                "INSERT INTO provider_models
                 (provider, model_id, is_default, cached_at, capabilities)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
            )?;
            for model in models {
                statement.execute(params![
                    provider,
                    model.id,
                    i64::from(
                        preferred
                            .as_ref()
                            .map_or(model.is_default, |preferred| preferred == &model.id)
                    ),
                    now,
                    serde_json::to_string(&model.capabilities)
                        .expect("model capabilities serialize"),
                ])?;
            }
        }
        transaction.commit()?;
        Ok(())
    }

    fn set_default_model(&self, provider: &str, model: &str) -> Result<(), SessionError> {
        let mut connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let transaction = connection.transaction()?;
        transaction.execute(
            "INSERT INTO provider_model_preferences (provider, model_id)
             VALUES (?1, ?2)
             ON CONFLICT(provider) DO UPDATE SET model_id = excluded.model_id",
            params![provider, model],
        )?;
        transaction.execute(
            "UPDATE provider_models
             SET is_default = CASE WHEN model_id = ?1 THEN 1 ELSE 0 END
             WHERE provider = ?2",
            params![model, provider],
        )?;
        transaction.commit()?;
        Ok(())
    }

    fn list_model_options(
        &self,
        provider: &str,
    ) -> Result<Vec<(String, ModelOptions)>, SessionError> {
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let mut statement = connection.prepare(
            "SELECT model_id, reasoning_effort, fast_mode
             FROM provider_model_options WHERE provider = ?1
             ORDER BY model_id COLLATE NOCASE",
        )?;
        let rows = statement.query_map([provider], |row| {
            Ok((
                row.get(0)?,
                ModelOptions {
                    reasoning_effort: row.get(1)?,
                    fast_mode: row.get::<_, i64>(2)? != 0,
                },
            ))
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    fn save_model_options(
        &self,
        provider: &str,
        model: &str,
        options: &ModelOptions,
    ) -> Result<(), SessionError> {
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        connection.execute(
            "INSERT INTO provider_model_options (provider, model_id, reasoning_effort, fast_mode)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(provider, model_id) DO UPDATE SET
               reasoning_effort = excluded.reasoning_effort,
               fast_mode = excluded.fast_mode",
            params![
                provider,
                model,
                options.reasoning_effort,
                i64::from(options.fast_mode)
            ],
        )?;
        Ok(())
    }

    fn save_mcp_server(&self, server: &McpServerRecord) -> Result<(), SessionError> {
        let tools = serde_json::to_string(&server.tools).map_err(|error| {
            SessionError::InvalidStoredValue {
                field: "mcp_servers.tools_json",
                value: error.to_string(),
            }
        })?;
        let grants = serde_json::to_string(&server.grants).map_err(|error| {
            SessionError::InvalidStoredValue {
                field: "mcp_servers.grants_json",
                value: error.to_string(),
            }
        })?;
        self.connection
            .lock()
            .expect("session database mutex poisoned")
            .execute(
                "INSERT INTO mcp_servers (
                   workspace, id, display_name, endpoint, transport, enabled, auth_kind,
                   credential_required, protocol_version, provenance_url, provenance_version,
                   provenance_commit, provenance_sha256, license_evidence, timeout_ms,
                   max_response_bytes, artifact_semantics, template_id, health, server_name,
                   server_version, last_error, last_connected_at_ms, updated_at_ms,
                   credential_kind, tools_json, grants_json
                 ) VALUES (
                   ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14,
                   ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27
                 ) ON CONFLICT(workspace, id) DO UPDATE SET
                   display_name=excluded.display_name, endpoint=excluded.endpoint,
                   transport=excluded.transport, enabled=excluded.enabled,
                   auth_kind=excluded.auth_kind, credential_required=excluded.credential_required,
                   protocol_version=excluded.protocol_version, provenance_url=excluded.provenance_url,
                   provenance_version=excluded.provenance_version,
                   provenance_commit=excluded.provenance_commit,
                   provenance_sha256=excluded.provenance_sha256,
                   license_evidence=excluded.license_evidence, timeout_ms=excluded.timeout_ms,
                   max_response_bytes=excluded.max_response_bytes,
                   artifact_semantics=excluded.artifact_semantics, template_id=excluded.template_id,
                   health=excluded.health, server_name=excluded.server_name,
                   server_version=excluded.server_version, last_error=excluded.last_error,
                   last_connected_at_ms=excluded.last_connected_at_ms,
                   updated_at_ms=excluded.updated_at_ms, credential_kind=excluded.credential_kind,
                   tools_json=excluded.tools_json, grants_json=excluded.grants_json",
                params![
                    server.workspace,
                    server.id,
                    server.display_name,
                    server.endpoint,
                    server.transport,
                    i64::from(server.enabled),
                    server.auth_kind,
                    i64::from(server.credential_required),
                    server.protocol_version,
                    server.provenance_url,
                    server.provenance_version,
                    server.provenance_commit,
                    server.provenance_sha256,
                    server.license_evidence,
                    i64::from(server.timeout_ms),
                    i64::from(server.max_response_bytes),
                    server.artifact_semantics,
                    server.template_id,
                    server.health,
                    server.server_name,
                    server.server_version,
                    server.last_error,
                    server.last_connected_at_ms.and_then(|value| i64::try_from(value).ok()),
                    i64::try_from(server.updated_at_ms).unwrap_or(i64::MAX),
                    server.credential_kind,
                    tools,
                    grants,
                ],
            )?;
        Ok(())
    }

    fn list_mcp_servers(&self, workspace: &str) -> Result<Vec<McpServerRecord>, SessionError> {
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let mut statement = connection.prepare(
            "SELECT id, display_name, endpoint, transport, enabled, auth_kind,
                    credential_required, protocol_version, provenance_url, provenance_version,
                    provenance_commit, provenance_sha256, license_evidence, timeout_ms,
                    max_response_bytes, artifact_semantics, template_id, health, server_name,
                    server_version, last_error, last_connected_at_ms, updated_at_ms,
                    credential_kind, tools_json, grants_json
             FROM mcp_servers WHERE workspace = ?1 ORDER BY display_name COLLATE NOCASE, id",
        )?;
        statement
            .query_map([workspace], |row| {
                let tools_source = row.get::<_, String>(24)?;
                let grants_source = row.get::<_, String>(25)?;
                let tools = serde_json::from_str(&tools_source).map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(
                        tools_source.len(),
                        rusqlite::types::Type::Text,
                        Box::new(error),
                    )
                })?;
                let grants = serde_json::from_str(&grants_source).map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(
                        grants_source.len(),
                        rusqlite::types::Type::Text,
                        Box::new(error),
                    )
                })?;
                Ok(McpServerRecord {
                    id: row.get(0)?,
                    workspace: workspace.to_owned(),
                    display_name: row.get(1)?,
                    endpoint: row.get(2)?,
                    transport: row.get(3)?,
                    enabled: row.get::<_, i64>(4)? != 0,
                    auth_kind: row.get(5)?,
                    credential_required: row.get::<_, i64>(6)? != 0,
                    protocol_version: row.get(7)?,
                    provenance_url: row.get(8)?,
                    provenance_version: row.get(9)?,
                    provenance_commit: row.get(10)?,
                    provenance_sha256: row.get(11)?,
                    license_evidence: row.get(12)?,
                    timeout_ms: u32::try_from(row.get::<_, i64>(13)?).unwrap_or(u32::MAX),
                    max_response_bytes: u32::try_from(row.get::<_, i64>(14)?).unwrap_or(u32::MAX),
                    artifact_semantics: row.get(15)?,
                    template_id: row.get(16)?,
                    health: row.get(17)?,
                    server_name: row.get(18)?,
                    server_version: row.get(19)?,
                    last_error: row.get(20)?,
                    last_connected_at_ms: row
                        .get::<_, Option<i64>>(21)?
                        .and_then(|value| u64::try_from(value).ok()),
                    updated_at_ms: u64::try_from(row.get::<_, i64>(22)?).unwrap_or_default(),
                    credential_kind: row.get(23)?,
                    tools,
                    grants,
                })
            })?
            .collect::<Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    fn delete_mcp_server(&self, workspace: &str, server_id: &str) -> Result<(), SessionError> {
        let mut connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let transaction = connection.transaction()?;
        transaction.execute(
            "DELETE FROM mcp_credentials WHERE workspace = ?1 AND server_id = ?2",
            params![workspace, server_id],
        )?;
        transaction.execute(
            "DELETE FROM mcp_servers WHERE workspace = ?1 AND id = ?2",
            params![workspace, server_id],
        )?;
        transaction.commit()?;
        Ok(())
    }

    fn audit_mcp_invocation(&self, audit: &McpInvocationAudit) -> Result<(), SessionError> {
        self.connection
            .lock()
            .expect("session database mutex poisoned")
            .execute(
                "INSERT INTO mcp_invocation_audit (
                   id, workspace, session_id, run_id, server_id, tool_name, arguments_json,
                   result_json, status, started_at_ms, duration_ms
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                params![
                    audit.id,
                    audit.workspace,
                    audit.session_id,
                    audit.run_id,
                    audit.server_id,
                    audit.tool_name,
                    audit.arguments_json,
                    audit.result_json,
                    audit.status,
                    i64::try_from(audit.started_at_ms).unwrap_or(i64::MAX),
                    i64::try_from(audit.duration_ms).unwrap_or(i64::MAX),
                ],
            )?;
        Ok(())
    }

    fn list_providers(&self) -> Result<Vec<ProviderRecord>, SessionError> {
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let mut statement = connection.prepare(
            "SELECT p.provider, p.display_name, p.enabled,
                    COALESCE(f.enabled, 0), COALESCE(f.selected_model_ids, '[]')
             FROM providers p
             LEFT JOIN provider_model_filters f ON f.provider = p.provider
             ORDER BY p.display_name COLLATE NOCASE",
        )?;
        let rows = statement.query_map([], |row| {
            let selected_model_ids =
                serde_json::from_str(&row.get::<_, String>(4)?).unwrap_or_default();
            Ok(ProviderRecord {
                provider: row.get(0)?,
                display_name: row.get(1)?,
                enabled: row.get::<_, i64>(2)? != 0,
                credential: None,
                accounts: Vec::new(),
                model_filter_enabled: row.get::<_, i64>(3)? != 0,
                selected_model_ids,
            })
        })?;
        let mut providers = rows.collect::<Result<Vec<_>, _>>()?;
        drop(statement);
        let mut account_statement = connection.prepare(
            "SELECT a.account_id, a.provider, a.label, a.enabled, a.is_default,
                    a.safe_identity, a.created_at, a.updated_at, a.routing_mode,
                    c.credential_kind, c.updated_at
             FROM provider_accounts a
             LEFT JOIN provider_account_credentials c ON c.account_id = a.account_id
             ORDER BY a.provider, a.label COLLATE NOCASE, a.account_id",
        )?;
        let accounts = account_statement.query_map([], provider_account_from_row)?;
        for account in accounts {
            let account = account?;
            if let Some(provider) = providers
                .iter_mut()
                .find(|provider| provider.provider == account.provider)
            {
                if account.is_default {
                    provider.credential.clone_from(&account.credential);
                }
                provider.accounts.push(account);
            }
        }
        Ok(providers)
    }

    fn add_provider_account(
        &self,
        provider: &str,
        label: &str,
    ) -> Result<ProviderAccountRecord, SessionError> {
        let account_id = Uuid::now_v7().to_string();
        let now = unix_timestamp();
        let routing_mode = if provider == crate::backend::CODEX_PROVIDER {
            "automatic"
        } else {
            "explicit_only"
        };
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let default = connection.query_row(
            "SELECT NOT EXISTS(SELECT 1 FROM provider_accounts WHERE provider = ?1)",
            [provider],
            |row| row.get::<_, bool>(0),
        )?;
        let inserted = connection.execute(
            "INSERT INTO provider_accounts
               (account_id, provider, label, enabled, is_default, routing_mode, created_at, updated_at)
             SELECT ?1, provider, ?3, 1, ?4, ?5, ?6, ?6 FROM providers WHERE provider = ?2",
            params![account_id, provider, label, i64::from(default), routing_mode, now],
        )?;
        if inserted == 0 {
            return Err(SessionError::ProviderNotFound(provider.to_owned()));
        }
        Ok(ProviderAccountRecord {
            account_id,
            provider: provider.to_owned(),
            label: label.to_owned(),
            enabled: true,
            is_default: default,
            identity: None,
            credential: None,
            created_at: now,
            updated_at: now,
            routing_mode: parse_account_routing_mode(routing_mode),
        })
    }

    fn set_provider_account_label(
        &self,
        provider: &str,
        account_id: &str,
        label: &str,
    ) -> Result<(), SessionError> {
        let updated = self
            .connection
            .lock()
            .expect("session database mutex poisoned")
            .execute(
                "UPDATE provider_accounts SET label = ?3, updated_at = ?4
                 WHERE provider = ?1 AND account_id = ?2",
                params![provider, account_id, label, unix_timestamp()],
            )?;
        ensure_provider_account_updated(updated, provider, account_id)
    }

    fn set_provider_account_enabled(
        &self,
        provider: &str,
        account_id: &str,
        enabled: bool,
    ) -> Result<(), SessionError> {
        let updated = self
            .connection
            .lock()
            .expect("session database mutex poisoned")
            .execute(
                "UPDATE provider_accounts SET enabled = ?3, updated_at = ?4
                 WHERE provider = ?1 AND account_id = ?2",
                params![provider, account_id, i64::from(enabled), unix_timestamp()],
            )?;
        ensure_provider_account_updated(updated, provider, account_id)
    }

    fn set_provider_account_default(
        &self,
        provider: &str,
        account_id: &str,
    ) -> Result<(), SessionError> {
        let mut connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let transaction = connection.transaction()?;
        let exists = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM provider_accounts
             WHERE provider = ?1 AND account_id = ?2)",
            params![provider, account_id],
            |row| row.get::<_, bool>(0),
        )?;
        if !exists {
            return Err(provider_account_not_found(provider, account_id));
        }
        transaction.execute(
            "UPDATE provider_accounts SET is_default = 0, updated_at = ?2 WHERE provider = ?1",
            params![provider, unix_timestamp()],
        )?;
        transaction.execute(
            "UPDATE provider_accounts SET is_default = 1, enabled = 1, updated_at = ?3
             WHERE provider = ?1 AND account_id = ?2",
            params![provider, account_id, unix_timestamp()],
        )?;
        transaction.commit()?;
        Ok(())
    }

    fn set_provider_account_identity(
        &self,
        provider: &str,
        account_id: &str,
        identity: Option<&str>,
    ) -> Result<(), SessionError> {
        let updated = self
            .connection
            .lock()
            .expect("session database mutex poisoned")
            .execute(
                "UPDATE provider_accounts SET safe_identity = ?3, updated_at = ?4
                 WHERE provider = ?1 AND account_id = ?2",
                params![provider, account_id, identity, unix_timestamp()],
            )?;
        ensure_provider_account_updated(updated, provider, account_id)
    }

    fn remove_provider_account(
        &self,
        provider: &str,
        account_id: &str,
    ) -> Result<(), SessionError> {
        let mut connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let transaction = connection.transaction()?;
        let in_use = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM sessions WHERE account_id = ?1)",
            [account_id],
            |row| row.get::<_, bool>(0),
        )?;
        if in_use {
            return Err(SessionError::ProviderAccountInUse {
                account_id: account_id.to_owned(),
            });
        }
        let removed = transaction.execute(
            "DELETE FROM provider_accounts WHERE provider = ?1 AND account_id = ?2",
            params![provider, account_id],
        )?;
        if removed == 0 {
            return Err(provider_account_not_found(provider, account_id));
        }
        transaction.execute(
            "UPDATE provider_accounts SET is_default = 1, updated_at = ?2
             WHERE account_id = (
               SELECT account_id FROM provider_accounts WHERE provider = ?1
               ORDER BY enabled DESC, account_id LIMIT 1
             ) AND NOT EXISTS(
               SELECT 1 FROM provider_accounts WHERE provider = ?1 AND is_default = 1
             )",
            params![provider, unix_timestamp()],
        )?;
        transaction.commit()?;
        Ok(())
    }

    fn set_provider_enabled(&self, provider: &str, enabled: bool) -> Result<(), SessionError> {
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let updated = connection.execute(
            "UPDATE providers SET enabled = ?1, updated_at = ?2 WHERE provider = ?3",
            params![i64::from(enabled), unix_timestamp(), provider],
        )?;
        if updated == 0 {
            return Err(SessionError::ProviderNotFound(provider.to_owned()));
        }
        Ok(())
    }

    fn set_provider_model_filter(
        &self,
        provider: &str,
        enabled: bool,
        selected_model_ids: &[String],
    ) -> Result<(), SessionError> {
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let updated = connection.execute(
            "INSERT INTO provider_model_filters (provider, enabled, selected_model_ids)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(provider) DO UPDATE SET enabled = excluded.enabled, selected_model_ids = excluded.selected_model_ids",
            params![provider, i64::from(enabled), serde_json::to_string(selected_model_ids).expect("model ids serialize")],
        )?;
        if updated == 0 {
            return Err(SessionError::ProviderNotFound(provider.to_owned()));
        }
        Ok(())
    }

    fn save_subagent(&self, record: &SubagentRecord) -> Result<(), SessionError> {
        let mut connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let transaction = connection.transaction()?;
        save_subagent_transaction(&transaction, record, unix_timestamp())?;
        transaction.commit()?;
        Ok(())
    }

    fn save_subagent_continuation(
        &self,
        source: &SubagentRecord,
        successor: &SubagentRecord,
    ) -> Result<(), SessionError> {
        let mut connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let transaction = connection.transaction()?;
        let now = unix_timestamp();
        save_subagent_transaction(&transaction, source, now)?;
        save_subagent_transaction(&transaction, successor, now)?;
        transaction.commit()?;
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    fn list_subagents(&self, parent_session_id: &str) -> Result<Vec<SubagentRecord>, SessionError> {
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let mut statement = connection.prepare(
            "SELECT id, agent_slug, provider, model, provider_session_id,
                    input_tokens, output_tokens, cached_input_tokens, cache_write_tokens,
                    objective, status, latest_activity, parent_run_id, invocation_turn_id,
                    invocation_call_id, originating_owner_entry_id, archetype_purpose,
                    policy_json, remaining_delegation_depth, started_at_ms, ended_at_ms,
                    termination_kind, termination_detail, objective_mismatch_handoff,
                    salvage_json, continued_from_run_id, continued_by_run_id, continuation_depth,
                    additional_turns, inherited_evidence_json, transcript_has_earlier
             FROM orchestration_runs
             WHERE parent_session_id = ?1
             ORDER BY started_at_ms, id",
        )?;
        let rows = statement.query_map([parent_session_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<String>>(4)?,
                u64::try_from(row.get::<_, i64>(5)?).unwrap_or_default(),
                u64::try_from(row.get::<_, i64>(6)?).unwrap_or_default(),
                u64::try_from(row.get::<_, i64>(7)?).unwrap_or_default(),
                u64::try_from(row.get::<_, i64>(8)?).unwrap_or_default(),
                row.get::<_, String>(9)?,
                row.get::<_, String>(10)?,
                row.get::<_, String>(11)?,
                row.get::<_, Option<String>>(12)?,
                row.get::<_, Option<String>>(13)?,
                row.get::<_, Option<String>>(14)?,
                row.get::<_, Option<String>>(15)?,
                row.get::<_, String>(16)?,
                row.get::<_, String>(17)?,
                u32::try_from(row.get::<_, i64>(18)?).unwrap_or_default(),
                u64::try_from(row.get::<_, i64>(19)?).unwrap_or_default(),
                row.get::<_, Option<i64>>(20)?
                    .map(|value| u64::try_from(value).unwrap_or_default()),
                row.get::<_, Option<String>>(21)?,
                row.get::<_, Option<String>>(22)?,
                row.get::<_, Option<String>>(23)?,
                row.get::<_, Option<String>>(24)?,
                row.get::<_, Option<String>>(25)?,
                row.get::<_, Option<String>>(26)?,
                u32::try_from(row.get::<_, i64>(27)?).unwrap_or_default(),
                row.get::<_, Option<i64>>(28)?
                    .map(|value| u32::try_from(value).unwrap_or_default()),
                row.get::<_, String>(29)?,
                row.get::<_, i64>(30)? != 0,
            ))
        })?;
        let stored_runs = rows.collect::<Result<Vec<_>, _>>()?;
        let mut records = Vec::with_capacity(stored_runs.len());
        for (
            id,
            agent,
            provider,
            model,
            provider_session_id,
            input_tokens,
            output_tokens,
            cached_input_tokens,
            cache_write_tokens,
            objective,
            status,
            latest_activity,
            parent_run_id,
            invocation_turn_id,
            invocation_call_id,
            originating_owner_entry_id,
            archetype_purpose,
            policy_json,
            remaining_delegation_depth,
            started_at_ms,
            ended_at_ms,
            termination_kind,
            termination_detail,
            objective_mismatch_handoff,
            salvage_json,
            continued_from_run_id,
            continued_by_run_id,
            continuation_depth,
            additional_turns,
            inherited_evidence_json,
            transcript_has_earlier,
        ) in stored_runs
        {
            let salvage = salvage_json
                .map(|json| {
                    serde_json::from_str(&json).map_err(|source| SessionError::InvalidStoredJson {
                        field: "orchestration_runs.salvage_json",
                        source,
                    })
                })
                .transpose()?;
            let inherited_evidence =
                serde_json::from_str(&inherited_evidence_json).map_err(|source| {
                    SessionError::InvalidStoredJson {
                        field: "orchestration_runs.inherited_evidence_json",
                        source,
                    }
                })?;
            let transcript = load_subagent_transcript(&connection, parent_session_id, &id)?;
            records.push(SubagentRecord {
                parent_session_id: parent_session_id.to_owned(),
                id,
                agent,
                provider,
                model,
                provider_session_id,
                input_tokens,
                output_tokens,
                cached_input_tokens,
                cache_write_tokens,
                objective,
                status: SubagentStatus::from_database(&status)?,
                latest_activity,
                observability: SubagentObservability {
                    parent_run_id,
                    invocation_turn_id,
                    invocation_call_id,
                    originating_owner_entry_id,
                    archetype_purpose,
                    policy_json,
                    remaining_delegation_depth,
                    started_at_ms,
                    ended_at_ms,
                    termination_kind,
                    termination_detail,
                    objective_mismatch_handoff,
                    salvage,
                    continued_from_run_id,
                    continued_by_run_id,
                    continuation_depth,
                    additional_turns,
                    inherited_evidence,
                },
                transcript,
                transcript_has_earlier,
            });
        }
        Ok(records)
    }

    fn load_web_config(&self) -> Result<WebConfig, SessionError> {
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let stored = connection
            .query_row(
                "SELECT backend, firecrawl_api_key FROM addon_web_settings WHERE singleton = 1",
                [],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?;
        let Some((backend, firecrawl_api_key)) = stored else {
            return Ok(WebConfig::default());
        };
        let backend = match backend.as_str() {
            "disabled" => WebBackend::Disabled,
            "agent-browser" => WebBackend::AgentBrowser,
            "firecrawl" => WebBackend::Firecrawl,
            _ => {
                return Err(SessionError::InvalidStoredValue {
                    field: "addon_web_settings.backend",
                    value: backend,
                });
            }
        };
        Ok(WebConfig {
            backend,
            firecrawl_api_key,
        })
    }

    fn save_web_config(&self, config: &WebConfig) -> Result<(), SessionError> {
        let backend = match config.backend {
            WebBackend::Disabled => "disabled",
            WebBackend::AgentBrowser => "agent-browser",
            WebBackend::Firecrawl => "firecrawl",
        };
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        connection.execute(
            "INSERT INTO addon_web_settings (singleton, backend, firecrawl_api_key) VALUES (1, ?1, ?2)
             ON CONFLICT(singleton) DO UPDATE SET backend = excluded.backend, firecrawl_api_key = excluded.firecrawl_api_key",
            params![backend, config.firecrawl_api_key],
        )?;
        Ok(())
    }

    fn load_memory_config(&self) -> Result<MemoryConfig, SessionError> {
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let stored = connection
            .query_row(
                "SELECT backend, executable, global_bank, data_directory FROM addon_memory_settings WHERE singleton = 1",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                },
            )
            .optional()?;
        let Some((backend, executable, global_bank, data_directory)) = stored else {
            return Ok(MemoryConfig::default());
        };
        let backend = match backend.as_str() {
            "disabled" => MemoryBackend::Disabled,
            "mnemosyne" => MemoryBackend::Mnemosyne,
            _ => {
                return Err(SessionError::InvalidStoredValue {
                    field: "addon_memory_settings.backend",
                    value: backend,
                });
            }
        };
        Ok(MemoryConfig {
            backend,
            executable,
            global_bank,
            data_directory,
        })
    }

    fn save_memory_config(&self, config: &MemoryConfig) -> Result<(), SessionError> {
        let backend = match config.backend {
            MemoryBackend::Disabled => "disabled",
            MemoryBackend::Mnemosyne => "mnemosyne",
        };
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        connection.execute(
            "INSERT INTO addon_memory_settings (singleton, backend, executable, global_bank, data_directory) VALUES (1, ?1, ?2, ?3, ?4)
             ON CONFLICT(singleton) DO UPDATE SET backend = excluded.backend, executable = excluded.executable, global_bank = excluded.global_bank, data_directory = excluded.data_directory",
            params![backend, config.executable, config.global_bank, config.data_directory],
        )?;
        Ok(())
    }

    fn load_vision_config(&self) -> Result<VisionConfig, SessionError> {
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let model = connection
            .query_row(
                "SELECT model FROM addon_vision_settings WHERE singleton = 1",
                [],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()?
            .flatten();
        Ok(VisionConfig { model })
    }

    fn save_vision_config(&self, config: &VisionConfig) -> Result<(), SessionError> {
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        connection.execute(
            "INSERT INTO addon_vision_settings (singleton, model) VALUES (1, ?1)
             ON CONFLICT(singleton) DO UPDATE SET model = excluded.model",
            params![config.model],
        )?;
        Ok(())
    }

    fn load_terminal_image_mode(&self) -> Result<TerminalImageMode, SessionError> {
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let mode = connection
            .query_row(
                "SELECT mode FROM terminal_image_settings WHERE singleton = 1",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        Ok(match mode.as_deref() {
            Some("on") => TerminalImageMode::On,
            Some("off") => TerminalImageMode::Off,
            _ => TerminalImageMode::Auto,
        })
    }

    fn save_terminal_image_mode(&self, mode: TerminalImageMode) -> Result<(), SessionError> {
        let mode = match mode {
            TerminalImageMode::Auto => "auto",
            TerminalImageMode::On => "on",
            TerminalImageMode::Off => "off",
        };
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        connection.execute(
            "INSERT INTO terminal_image_settings (singleton, mode) VALUES (1, ?1)
             ON CONFLICT(singleton) DO UPDATE SET mode = excluded.mode",
            [mode],
        )?;
        Ok(())
    }

    fn load_invocation_telemetry_enabled(&self) -> Result<bool, SessionError> {
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        connection
            .query_row(
                "SELECT enabled FROM invocation_telemetry_settings WHERE singleton = 1",
                [],
                |row| row.get::<_, bool>(0),
            )
            .optional()
            .map(Option::unwrap_or_default)
            .map_err(Into::into)
    }

    fn save_invocation_telemetry_enabled(&self, enabled: bool) -> Result<(), SessionError> {
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        connection.execute(
            "INSERT INTO invocation_telemetry_settings (singleton, enabled) VALUES (1, ?1)
             ON CONFLICT(singleton) DO UPDATE SET enabled = excluded.enabled",
            [enabled],
        )?;
        Ok(())
    }

    fn record_invocation(&self, invocation: &InvocationRecord) -> Result<bool, SessionError> {
        let occurred_at_ms = i64::try_from(invocation.occurred_at_ms).unwrap_or(i64::MAX);
        let kind = invocation_kind_value(invocation.kind);
        let mut connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        // IMMEDIATE makes the consent check, idempotent append, and aggregate update one short
        // writer-serialized decision. No unrelated read acquires this transaction.
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let enabled = transaction
            .query_row(
                "SELECT enabled FROM invocation_telemetry_settings WHERE singleton = 1",
                [],
                |row| row.get::<_, bool>(0),
            )
            .optional()?
            .unwrap_or_default();
        if !enabled {
            transaction.commit()?;
            return Ok(false);
        }
        let inserted = transaction.execute(
            "INSERT OR IGNORE INTO invocation_events
             (invocation_key, kind, identity, display_label, occurred_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                invocation.invocation_key,
                kind,
                invocation.identity,
                invocation.display_label,
                occurred_at_ms
            ],
        )?;
        if inserted == 1 {
            transaction.execute(
                "INSERT INTO invocation_aggregates
                 (kind, identity, display_label, invocation_count, first_used_at_ms, last_used_at_ms)
                 VALUES (?1, ?2, ?3, 1, ?4, ?4)
                 ON CONFLICT(kind, identity) DO UPDATE SET
                   display_label = excluded.display_label,
                   invocation_count = invocation_aggregates.invocation_count + 1,
                   first_used_at_ms = MIN(invocation_aggregates.first_used_at_ms, excluded.first_used_at_ms),
                   last_used_at_ms = MAX(invocation_aggregates.last_used_at_ms, excluded.last_used_at_ms)",
                params![kind, invocation.identity, invocation.display_label, occurred_at_ms],
            )?;
        }
        transaction.commit()?;
        Ok(inserted == 1)
    }

    fn invocation_summary(&self) -> Result<nakode_protocol::InvocationSummary, SessionError> {
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let enabled = connection
            .query_row(
                "SELECT enabled FROM invocation_telemetry_settings WHERE singleton = 1",
                [],
                |row| row.get::<_, bool>(0),
            )
            .optional()?
            .unwrap_or_default();
        let mut statement = connection.prepare(
            "SELECT kind, identity, display_label, invocation_count,
                    first_used_at_ms, last_used_at_ms
             FROM invocation_aggregates ORDER BY kind, identity",
        )?;
        let items = statement
            .query_map([], |row| {
                let kind = row.get::<_, String>(0)?;
                Ok((
                    kind,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, i64>(5)?,
                ))
            })?
            .map(|row| {
                let (kind, identity, display_label, count, first, last) = row?;
                Ok(nakode_protocol::InvocationUsage {
                    kind: parse_invocation_kind(&kind)?,
                    identity,
                    display_label,
                    currently_installed: false,
                    invocation_count: u64::try_from(count).unwrap_or_default(),
                    first_used_at_ms: u64::try_from(first).ok(),
                    last_used_at_ms: u64::try_from(last).ok(),
                })
            })
            .collect::<Result<Vec<_>, SessionError>>()?;
        Ok(nakode_protocol::InvocationSummary { enabled, items })
    }

    fn invocation_timeline(
        &self,
        start_at_ms: u64,
        end_at_ms: u64,
        bucket_width_ms: u64,
    ) -> Result<nakode_protocol::InvocationTimeline, SessionError> {
        if bucket_width_ms == 0 || start_at_ms >= end_at_ms {
            return Err(SessionError::InvalidStoredValue {
                field: "invocation timeline bounds",
                value: format!("{start_at_ms}..{end_at_ms}/{bucket_width_ms}"),
            });
        }
        let start = i64::try_from(start_at_ms).unwrap_or(i64::MAX);
        let end = i64::try_from(end_at_ms).unwrap_or(i64::MAX);
        let width = i64::try_from(bucket_width_ms).unwrap_or(i64::MAX);
        let connection = self
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let mut statement = connection.prepare(
            "SELECT (occurred_at_ms - ?1) / ?3 AS bucket_index, kind, COUNT(*)
             FROM invocation_events
             WHERE occurred_at_ms >= ?1 AND occurred_at_ms < ?2
             GROUP BY bucket_index, kind
             ORDER BY bucket_index, kind",
        )?;
        let rows = statement.query_map(params![start, end, width], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })?;
        let mut counts = BTreeMap::<u64, (u64, u64)>::new();
        for row in rows {
            let (index, kind, count) = row?;
            let entry = counts
                .entry(u64::try_from(index).unwrap_or_default())
                .or_default();
            match parse_invocation_kind(&kind)? {
                nakode_protocol::InvocationKind::Archetype => {
                    entry.0 = u64::try_from(count).unwrap_or_default();
                }
                nakode_protocol::InvocationKind::Skill => {
                    entry.1 = u64::try_from(count).unwrap_or_default();
                }
            }
        }
        let bucket_count = end_at_ms
            .saturating_sub(start_at_ms)
            .div_ceil(bucket_width_ms);
        let buckets = (0..bucket_count)
            .map(|index| {
                let (archetype_count, skill_count) = counts.remove(&index).unwrap_or_default();
                nakode_protocol::InvocationBucket {
                    start_at_ms: start_at_ms.saturating_add(index.saturating_mul(bucket_width_ms)),
                    archetype_count,
                    skill_count,
                }
            })
            .collect();
        Ok(nakode_protocol::InvocationTimeline {
            start_at_ms,
            end_at_ms,
            bucket_width_ms,
            buckets,
        })
    }
}

fn invocation_kind_value(kind: nakode_protocol::InvocationKind) -> &'static str {
    match kind {
        nakode_protocol::InvocationKind::Archetype => "archetype",
        nakode_protocol::InvocationKind::Skill => "skill",
    }
}

fn parse_invocation_kind(value: &str) -> Result<nakode_protocol::InvocationKind, SessionError> {
    match value {
        "archetype" => Ok(nakode_protocol::InvocationKind::Archetype),
        "skill" => Ok(nakode_protocol::InvocationKind::Skill),
        _ => Err(SessionError::InvalidStoredValue {
            field: "invocation kind",
            value: value.to_owned(),
        }),
    }
}

fn load_subagent_transcript(
    connection: &Connection,
    parent_session_id: &str,
    run_id: &str,
) -> Result<Vec<TranscriptEntry>, SessionError> {
    let mut statement = connection.prepare(
        "SELECT entry_id, item_key, kind, title, body, status, provider_id, model_id,
                tool_audit_json, created_at_ms, owner_turn_id, reasoning_effort, fast_mode,
                source_transport
         FROM agent_turns
         WHERE parent_session_id = ?1 AND run_id = ?2
         ORDER BY sequence",
    )?;
    let rows = statement.query_map(params![parent_session_id, run_id], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, String>(4)?,
            row.get::<_, String>(5)?,
            row.get::<_, Option<String>>(6)?,
            row.get::<_, Option<String>>(7)?,
            row.get::<_, Option<String>>(8)?,
            row.get::<_, Option<i64>>(9)?,
            row.get::<_, Option<String>>(10)?,
            row.get::<_, Option<String>>(11)?,
            row.get::<_, Option<i64>>(12)?,
            row.get::<_, Option<String>>(13)?,
        ))
    })?;
    rows.map(|row| {
        let (
            id,
            key,
            kind,
            title,
            body,
            status,
            provider_id,
            model_id,
            tool_audit_json,
            created_at_ms,
            owner_turn_id,
            reasoning_effort,
            fast_mode,
            source_transport,
        ) = row?;
        Ok(TranscriptEntry {
            id,
            key,
            kind: entry_kind_from_value(&kind)?,
            title,
            body,
            status: entry_status_from_value(&status)?,
            created_at_ms: created_at_ms.and_then(|value| u64::try_from(value).ok()),
            provider_id,
            model_id,
            owner_turn_id,
            reasoning_effort,
            fast_mode: fast_mode.map(|value| value != 0),
            source_transport,
            tool_audit_json,
        })
    })
    .collect()
}

const fn entry_kind_value(kind: EntryKind) -> &'static str {
    match kind {
        EntryKind::System => "system",
        EntryKind::User => "user",
        EntryKind::Assistant => "assistant",
        EntryKind::Steering => "steering",
        EntryKind::Reasoning => "reasoning",
        EntryKind::Tool => "tool",
        EntryKind::Diff => "diff",
        EntryKind::Warning => "warning",
        EntryKind::Error => "error",
    }
}

fn entry_kind_from_value(value: &str) -> Result<EntryKind, SessionError> {
    match value {
        "system" => Ok(EntryKind::System),
        "user" => Ok(EntryKind::User),
        "assistant" => Ok(EntryKind::Assistant),
        "steering" => Ok(EntryKind::Steering),
        "reasoning" => Ok(EntryKind::Reasoning),
        "tool" => Ok(EntryKind::Tool),
        "diff" => Ok(EntryKind::Diff),
        "warning" => Ok(EntryKind::Warning),
        "error" => Ok(EntryKind::Error),
        _ => Err(SessionError::InvalidStoredValue {
            field: "agent_turns.kind",
            value: value.to_owned(),
        }),
    }
}

const fn entry_status_value(status: EntryStatus) -> &'static str {
    match status {
        EntryStatus::Running => "running",
        EntryStatus::Complete => "complete",
        EntryStatus::Failed => "failed",
        EntryStatus::Interrupted => "interrupted",
    }
}

fn entry_status_from_value(value: &str) -> Result<EntryStatus, SessionError> {
    match value {
        "running" => Ok(EntryStatus::Running),
        "complete" => Ok(EntryStatus::Complete),
        "failed" => Ok(EntryStatus::Failed),
        "interrupted" => Ok(EntryStatus::Interrupted),
        _ => Err(SessionError::InvalidStoredValue {
            field: "agent_turns.status",
            value: value.to_owned(),
        }),
    }
}

fn provider_account_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ProviderAccountRecord> {
    let account_id = row.get::<_, String>(0)?;
    let provider = row.get::<_, String>(1)?;
    let credential_kind = row.get::<_, Option<String>>(9)?;
    let credential_updated_at = row.get::<_, Option<i64>>(10)?;
    Ok(ProviderAccountRecord {
        account_id: account_id.clone(),
        provider: provider.clone(),
        label: row.get(2)?,
        enabled: row.get::<_, i64>(3)? != 0,
        is_default: row.get::<_, i64>(4)? != 0,
        identity: row.get(5)?,
        created_at: row.get(6)?,
        updated_at: row.get(7)?,
        routing_mode: parse_account_routing_mode(&row.get::<_, String>(8)?),
        credential: credential_kind
            .zip(credential_updated_at)
            .map(|(kind, updated_at)| CredentialMetadata {
                provider,
                account_id,
                kind,
                updated_at,
            }),
    })
}

fn parse_account_routing_mode(value: &str) -> nakode_protocol::ProviderAccountRoutingMode {
    if value == "automatic" {
        nakode_protocol::ProviderAccountRoutingMode::Automatic
    } else {
        nakode_protocol::ProviderAccountRoutingMode::ExplicitOnly
    }
}

fn provider_account_not_found(provider: &str, account_id: &str) -> SessionError {
    SessionError::ProviderAccountNotFound {
        provider: provider.to_owned(),
        account_id: account_id.to_owned(),
    }
}

fn ensure_provider_account_updated(
    updated: usize,
    provider: &str,
    account_id: &str,
) -> Result<(), SessionError> {
    if updated == 0 {
        Err(provider_account_not_found(provider, account_id))
    } else {
        Ok(())
    }
}

fn unix_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .try_into()
        .unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::credential::{Credential, CredentialStore, SecretValue};

    #[test]
    fn terminal_image_mode_defaults_to_auto_and_persists() -> Result<(), SessionError> {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteSessionRepository::open(directory.path().join("sessions.db"))?;
        assert_eq!(store.load_terminal_image_mode()?, TerminalImageMode::Auto);

        store.save_terminal_image_mode(TerminalImageMode::Off)?;
        assert_eq!(store.load_terminal_image_mode()?, TerminalImageMode::Off);
        Ok(())
    }

    #[test]
    fn invocation_telemetry_defaults_off_is_idempotent_and_survives_restart()
    -> Result<(), SessionError> {
        let directory = tempfile::tempdir().expect("tempdir");
        let database = directory.path().join("sessions.db");
        let store = SqliteSessionRepository::open(&database)?;
        let archetype = InvocationRecord {
            invocation_key: "archetype:agent-1".to_owned(),
            kind: nakode_protocol::InvocationKind::Archetype,
            identity: "explorer".to_owned(),
            display_label: "Explorer".to_owned(),
            occurred_at_ms: 1_000,
        };
        assert!(!store.load_invocation_telemetry_enabled()?);
        assert!(!store.record_invocation(&archetype)?);
        assert!(store.invocation_summary()?.items.is_empty());

        store.save_invocation_telemetry_enabled(true)?;
        assert!(store.record_invocation(&archetype)?);
        assert!(!store.record_invocation(&archetype)?);
        let skill = InvocationRecord {
            invocation_key: "skill:session:turn:call".to_owned(),
            kind: nakode_protocol::InvocationKind::Skill,
            identity: "review".to_owned(),
            display_label: "Review".to_owned(),
            occurred_at_ms: 2_000,
        };
        assert!(store.record_invocation(&skill)?);
        store.save_invocation_telemetry_enabled(false)?;
        assert!(!store.record_invocation(&InvocationRecord {
            invocation_key: "archetype:agent-2".to_owned(),
            occurred_at_ms: 3_000,
            ..archetype
        })?);
        drop(store);

        let restarted = SqliteSessionRepository::open(database)?;
        let summary = restarted.invocation_summary()?;
        assert!(!summary.enabled);
        assert_eq!(summary.items.len(), 2);
        assert!(summary.items.iter().all(|item| item.invocation_count == 1));
        let timeline = restarted.invocation_timeline(0, 4_000, 1_000)?;
        assert_eq!(timeline.buckets.len(), 4);
        assert_eq!(timeline.buckets[1].archetype_count, 1);
        assert_eq!(timeline.buckets[2].skill_count, 1);
        Ok(())
    }

    #[test]
    fn unavailable_skill_prune_removes_only_profile_association_and_preserves_telemetry()
    -> Result<(), SessionError> {
        let directory = tempfile::tempdir().expect("tempdir");
        let database = directory.path().join("sessions.db");
        let store = SqliteSessionRepository::open(&database)?;
        store.set_skill_preferences(&[
            SkillPreference {
                profile_id: "profile-a".to_owned(),
                skill_id: "removed.review".to_owned(),
                last_name: "review".to_owned(),
                last_description: "Review".to_owned(),
                enabled: false,
            },
            SkillPreference {
                profile_id: "profile-b".to_owned(),
                skill_id: "removed.review".to_owned(),
                last_name: "review".to_owned(),
                last_description: "Review".to_owned(),
                enabled: false,
            },
            SkillPreference {
                profile_id: "profile-a".to_owned(),
                skill_id: "retained.testing".to_owned(),
                last_name: "testing".to_owned(),
                last_description: "Testing".to_owned(),
                enabled: false,
            },
        ])?;
        store.save_invocation_telemetry_enabled(true)?;
        for (key, kind, identity) in [
            (
                "target-skill",
                nakode_protocol::InvocationKind::Skill,
                "removed.review",
            ),
            (
                "other-skill",
                nakode_protocol::InvocationKind::Skill,
                "retained.testing",
            ),
            (
                "same-identity-archetype",
                nakode_protocol::InvocationKind::Archetype,
                "removed.review",
            ),
        ] {
            assert!(store.record_invocation(&InvocationRecord {
                invocation_key: key.to_owned(),
                kind,
                identity: identity.to_owned(),
                display_label: identity.to_owned(),
                occurred_at_ms: 1_000,
            })?);
        }

        let report = store.prune_unavailable_skill("profile-a", "removed.review")?;
        assert_eq!(report.preference_count, 1);
        assert_eq!(
            store
                .list_skill_preferences("profile-a")?
                .iter()
                .map(|preference| preference.skill_id.as_str())
                .collect::<Vec<_>>(),
            ["retained.testing"]
        );
        assert_eq!(
            store
                .list_skill_preferences("profile-b")?
                .iter()
                .map(|preference| preference.skill_id.as_str())
                .collect::<Vec<_>>(),
            ["removed.review"]
        );
        let summary = store.invocation_summary()?;
        assert_eq!(summary.items.len(), 3);
        assert!(summary.items.iter().any(|item| {
            item.kind == nakode_protocol::InvocationKind::Skill && item.identity == "removed.review"
        }));
        assert!(summary.items.iter().any(|item| {
            item.kind == nakode_protocol::InvocationKind::Skill
                && item.identity == "retained.testing"
        }));
        assert!(summary.items.iter().any(|item| {
            item.kind == nakode_protocol::InvocationKind::Archetype
                && item.identity == "removed.review"
        }));
        assert_eq!(
            store.prune_unavailable_skill("profile-a", "removed.review")?,
            crate::skill::SkillPruneReport::default()
        );
        drop(store);
        let restarted = SqliteSessionRepository::open(database)?;
        assert_eq!(
            restarted
                .list_skill_preferences("profile-b")?
                .iter()
                .map(|preference| preference.skill_id.as_str())
                .collect::<Vec<_>>(),
            ["removed.review"],
            "another profile's association must survive repository restart"
        );
        Ok(())
    }

    #[test]
    fn concurrent_duplicate_invocation_delivery_counts_once() -> Result<(), SessionError> {
        use std::sync::{Arc, Barrier};

        let directory = tempfile::tempdir().expect("tempdir");
        let database = directory.path().join("sessions.db");
        let setup = SqliteSessionRepository::open(&database)?;
        setup.save_invocation_telemetry_enabled(true)?;
        drop(setup);
        let barrier = Arc::new(Barrier::new(8));
        let handles = (0..8)
            .map(|_| {
                let database = database.clone();
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    let store = SqliteSessionRepository::open(database)?;
                    barrier.wait();
                    store.record_invocation(&InvocationRecord {
                        invocation_key: "skill:concurrent".to_owned(),
                        kind: nakode_protocol::InvocationKind::Skill,
                        identity: "review".to_owned(),
                        display_label: "Review".to_owned(),
                        occurred_at_ms: 1_000,
                    })
                })
            })
            .collect::<Vec<_>>();
        let inserted = handles
            .into_iter()
            .map(|handle| handle.join().expect("recording thread"))
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(inserted.into_iter().filter(|inserted| *inserted).count(), 1);
        let store = SqliteSessionRepository::open(database)?;
        assert_eq!(store.invocation_summary()?.items[0].invocation_count, 1);
        Ok(())
    }

    #[test]
    fn invocation_event_and_aggregate_update_roll_back_together() -> Result<(), SessionError> {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteSessionRepository::open(directory.path().join("sessions.db"))?;
        store.save_invocation_telemetry_enabled(true)?;
        {
            let connection = store.connection.lock().expect("database mutex");
            connection.execute_batch(
                "CREATE TRIGGER reject_invocation_aggregate
                 BEFORE INSERT ON invocation_aggregates
                 BEGIN SELECT RAISE(ABORT, 'aggregate refused'); END;",
            )?;
        }
        let result = store.record_invocation(&InvocationRecord {
            invocation_key: "skill:atomic".to_owned(),
            kind: nakode_protocol::InvocationKind::Skill,
            identity: "review".to_owned(),
            display_label: "Review".to_owned(),
            occurred_at_ms: 1_000,
        });
        assert!(result.is_err());
        let connection = store.connection.lock().expect("database mutex");
        let event_count =
            connection.query_row("SELECT COUNT(*) FROM invocation_events", [], |row| {
                row.get::<_, i64>(0)
            })?;
        let aggregate_count =
            connection.query_row("SELECT COUNT(*) FROM invocation_aggregates", [], |row| {
                row.get::<_, i64>(0)
            })?;
        assert_eq!((event_count, aggregate_count), (0, 0));
        Ok(())
    }

    #[test]
    fn invocation_timeline_uses_range_index_at_representative_scale() -> Result<(), SessionError> {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteSessionRepository::open(directory.path().join("sessions.db"))?;
        {
            let mut connection = store.connection.lock().expect("database mutex");
            let transaction = connection.transaction()?;
            {
                let mut insert = transaction.prepare(
                    "INSERT INTO invocation_events
                     (invocation_key, kind, identity, display_label, occurred_at_ms)
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                )?;
                for index in 0_i64..100_000 {
                    let kind = if index % 2 == 0 { "archetype" } else { "skill" };
                    insert.execute(params![
                        format!("load-{index}"),
                        kind,
                        "identity",
                        "Identity",
                        index * 1_000
                    ])?;
                }
            }
            transaction.commit()?;
        }
        let timeline = store.invocation_timeline(90_000_000, 100_000_000, 1_000_000)?;
        assert_eq!(timeline.buckets.len(), 10);
        assert_eq!(
            timeline
                .buckets
                .iter()
                .map(|bucket| bucket.archetype_count + bucket.skill_count)
                .sum::<u64>(),
            10_000
        );
        let connection = store.connection.lock().expect("database mutex");
        let plan = connection
            .prepare(
                "EXPLAIN QUERY PLAN SELECT (occurred_at_ms - ?1) / ?3, kind, COUNT(*)
                 FROM invocation_events
                 WHERE occurred_at_ms >= ?1 AND occurred_at_ms < ?2
                 GROUP BY 1, kind",
            )?
            .query_map(
                params![90_000_000_i64, 100_000_000_i64, 1_000_000_i64],
                |row| row.get::<_, String>(3),
            )?
            .collect::<Result<Vec<_>, _>>()?
            .join(" ");
        assert!(plan.contains("invocation_events_kind_time"), "{plan}");
        Ok(())
    }

    #[test]
    fn model_options_are_saved_per_model() -> Result<(), SessionError> {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteSessionRepository::open(directory.path().join("sessions.db"))?;
        assert!(store.list_model_options(CODEX_PROVIDER)?.is_empty());

        let high_fast = ModelOptions {
            reasoning_effort: Some("high".to_owned()),
            fast_mode: true,
        };
        let low_standard = ModelOptions {
            reasoning_effort: Some("low".to_owned()),
            fast_mode: false,
        };
        store.save_model_options(CODEX_PROVIDER, "model-a", &high_fast)?;
        store.save_model_options(CODEX_PROVIDER, "model-b", &low_standard)?;
        assert_eq!(
            store.list_model_options(CODEX_PROVIDER)?,
            vec![
                ("model-a".to_owned(), high_fast),
                ("model-b".to_owned(), low_standard),
            ]
        );
        Ok(())
    }

    #[test]
    fn migrates_provider_wide_model_options_to_a_wildcard_profile() -> Result<(), SessionError> {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("legacy-options.db");
        let connection = Connection::open(&path)?;
        connection.execute_batch(
            "CREATE TABLE provider_model_options (
               provider TEXT PRIMARY KEY,
               reasoning_effort TEXT,
               fast_mode INTEGER NOT NULL DEFAULT 0
             );
             INSERT INTO provider_model_options VALUES ('openai-codex', 'high', 1);",
        )?;
        drop(connection);

        let store = SqliteSessionRepository::open(&path)?;
        assert_eq!(
            store.list_model_options(CODEX_PROVIDER)?,
            vec![(
                "*".to_owned(),
                ModelOptions {
                    reasoning_effort: Some("high".to_owned()),
                    fast_mode: true,
                },
            )]
        );
        Ok(())
    }

    #[test]
    fn vision_addon_settings_are_optional_and_persisted() -> Result<(), SessionError> {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteSessionRepository::open(directory.path().join("sessions.db"))?;
        assert_eq!(store.load_vision_config()?, VisionConfig::default());

        let configured = VisionConfig {
            model: Some("openai-codex/gpt-5.4".to_owned()),
        };
        store.save_vision_config(&configured)?;
        assert_eq!(store.load_vision_config()?, configured);
        Ok(())
    }

    #[test]
    fn browser_addon_settings_are_optional_and_persisted() -> Result<(), SessionError> {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteSessionRepository::open(directory.path().join("sessions.db"))?;
        assert_eq!(store.load_web_config()?, WebConfig::default());

        let configured = WebConfig {
            backend: WebBackend::Firecrawl,
            firecrawl_api_key: "secret".to_owned(),
        };
        store.save_web_config(&configured)?;
        assert_eq!(store.load_web_config()?, configured);
        Ok(())
    }

    #[test]
    fn memory_addon_settings_are_optional_and_persisted() -> Result<(), SessionError> {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteSessionRepository::open(directory.path().join("sessions.db"))?;
        assert_eq!(store.load_memory_config()?, MemoryConfig::default());

        let configured = MemoryConfig {
            backend: MemoryBackend::Mnemosyne,
            executable: "/opt/bin/mnemosyne".to_owned(),
            global_bank: "my-global-memory".to_owned(),
            data_directory: "/tmp/memory".to_owned(),
        };
        store.save_memory_config(&configured)?;
        assert_eq!(store.load_memory_config()?, configured);
        Ok(())
    }

    #[test]
    fn session_cwd_persists_and_legacy_null_rows_inherit_workspace() -> Result<(), SessionError> {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteSessionRepository::open(directory.path().join("sessions.db"))?;
        let created = store.create_with_id(
            "logical-session",
            CODEX_PROVIDER,
            "provider-session",
            "/logical/workspace",
            "/explicit/repository",
            "Session",
            None,
            &ModelOptions::default(),
            None,
        )?;
        assert_eq!(created.working_directory, "/explicit/repository");
        assert_eq!(
            store
                .find("logical-session")?
                .expect("persisted session")
                .working_directory,
            "/explicit/repository"
        );

        store
            .connection
            .lock()
            .expect("session database mutex poisoned")
            .execute(
                "UPDATE sessions SET working_directory = NULL WHERE id = ?1",
                ["logical-session"],
            )?;
        assert_eq!(
            store
                .find("logical-session")?
                .expect("legacy session")
                .working_directory,
            "/logical/workspace"
        );
        Ok(())
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn bridge_schema_migrates_pending_inbox_and_installs_normalized_replay_ledger()
    -> Result<(), SessionError> {
        let directory = tempfile::tempdir().expect("tempdir");
        let database = directory.path().join("sessions.db");
        let legacy = Connection::open(&database)?;
        legacy.execute_batch(
            r#"CREATE TABLE session_bridges (
               session_id TEXT PRIMARY KEY,
               workspace TEXT NOT NULL,
               kind TEXT NOT NULL,
               lifecycle TEXT NOT NULL,
               display_title TEXT NOT NULL,
               revision INTEGER NOT NULL,
               transport TEXT,
               external_parent_id TEXT,
               external_thread_id TEXT,
               last_delivered_turn_id TEXT,
               delivery_json TEXT,
               live_turn_id TEXT,
               live_external_message_id TEXT,
               active_source_message_id TEXT,
               recent_inbound_event_ids_json TEXT NOT NULL DEFAULT '[]',
               updated_at_ms INTEGER NOT NULL
             );
             CREATE TABLE session_bridge_inbound_events (
               session_id TEXT NOT NULL,
               external_event_id TEXT NOT NULL,
               recorded_at_ms INTEGER NOT NULL,
               PRIMARY KEY(session_id, external_event_id)
             );
             INSERT INTO session_bridge_inbound_events
               (session_id, external_event_id, recorded_at_ms)
             VALUES ('legacy-session', 'event-c', 122);
             INSERT INTO session_bridges
               (session_id, workspace, kind, lifecycle, display_title, revision,
                last_delivered_turn_id, recent_inbound_event_ids_json, updated_at_ms)
             VALUES
               ('legacy-session', '/tmp/project', 'chat', 'open', 'Legacy bridge', 1,
                'legacy-turn', '["event-a","event-b","event-a"]', 123);"#,
        )?;
        drop(legacy);

        let store = SqliteSessionRepository::open(&database)?;
        let migrated = Connection::open(&database)?;
        let bridge_columns = {
            let mut statement = migrated.prepare("PRAGMA table_info(session_bridges)")?;
            statement
                .query_map([], |row| row.get::<_, String>(1))?
                .collect::<Result<Vec<_>, _>>()?
        };
        assert!(
            bridge_columns
                .iter()
                .any(|column| column == "pending_inbound_json")
        );
        let ledger_exists = migrated.query_row(
            "SELECT EXISTS(
                   SELECT 1 FROM sqlite_master
                   WHERE type = 'table' AND name = 'session_bridge_inbound_events'
                 )",
            [],
            |row| row.get::<_, i64>(0),
        )?;
        assert_eq!(ledger_exists, 1);
        let migrated_events = {
            let mut statement = migrated.prepare(
                "SELECT external_event_id, disposition FROM session_bridge_inbound_events
                 WHERE session_id = 'legacy-session' ORDER BY external_event_id",
            )?;
            statement
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })?
                .collect::<Result<Vec<_>, _>>()?
        };
        assert_eq!(
            migrated_events,
            vec![
                ("event-a".to_owned(), "duplicate".to_owned()),
                ("event-b".to_owned(), "duplicate".to_owned()),
                ("event-c".to_owned(), "duplicate".to_owned()),
            ]
        );
        let legacy_json = migrated.query_row(
            "SELECT recent_inbound_event_ids_json FROM session_bridges
             WHERE session_id = 'legacy-session'",
            [],
            |row| row.get::<_, String>(0),
        )?;
        assert_eq!(legacy_json, "[]");
        let bridge = store
            .list_session_bridges("/tmp/project")?
            .pop()
            .expect("migrated bridge");
        assert_eq!(
            bridge.last_projected,
            Some(BridgeProjectionRecord {
                kind: nakode_protocol::BridgeProjectionKind::Assistant,
                turn_id: "legacy-turn".to_owned(),
            }),
            "legacy turn-only cursors migrate as assistant projections"
        );
        assert!(bridge.inbound_turn_origins.is_empty());
        Ok(())
    }

    #[test]
    fn session_prefix_lookup_treats_sql_wildcards_as_literal_characters() -> Result<(), SessionError>
    {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteSessionRepository::open(directory.path().join("sessions.db"))?;
        let target = store.create_with_id(
            "literal-prefix-target",
            CODEX_PROVIDER,
            "literal-prefix-provider",
            "/tmp/project",
            "/tmp/project",
            "Literal prefix",
            None,
            &ModelOptions::default(),
            None,
        )?;

        assert_eq!(store.find("literal%")?, None);
        assert_eq!(store.find("literal_")?, None);
        store.delete("literal%")?;
        assert_eq!(store.find(&target.id)?, Some(target));
        Ok(())
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn bridge_state_is_archived_by_session_delete_while_replay_rows_are_removed()
    -> Result<(), SessionError> {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteSessionRepository::open(directory.path().join("sessions.db"))?;
        let session = store.create(
            CODEX_PROVIDER,
            "provider-bridge",
            "/tmp/project",
            "Bridge prompt",
            Some("model"),
        )?;
        let bridge = SessionBridgeRecord {
            session_id: session.id.clone(),
            workspace: "/tmp/project".to_owned(),
            kind: nakode_protocol::OrchestratorKind::Agent,
            lifecycle: nakode_protocol::BridgeLifecycle::Open,
            display_title: "Agent bridge".to_owned(),
            revision: 7,
            transport: Some("thread-transport".to_owned()),
            external_parent_id: Some("100".to_owned()),
            external_thread_id: Some("101".to_owned()),
            last_projected: Some(BridgeProjectionRecord {
                kind: nakode_protocol::BridgeProjectionKind::Assistant,
                turn_id: "turn-0".to_owned(),
            }),
            delivery: Some(BridgeDeliveryRecord {
                projection_kind: nakode_protocol::BridgeProjectionKind::User,
                turn_id: "turn-1".to_owned(),
                previous_projection: Some(BridgeProjectionRecord {
                    kind: nakode_protocol::BridgeProjectionKind::Assistant,
                    turn_id: "turn-0".to_owned(),
                }),
                body_sha256: "a".repeat(64),
                part_count: 2,
                completed_parts: 1,
                last_external_message_id: Some("200".to_owned()),
            }),
            live_turn_id: Some("turn-2".to_owned()),
            live_external_message_id: Some("201".to_owned()),
            active_source_message_id: Some("202".to_owned()),
            recent_inbound_event_ids: vec!["event-1".to_owned(), "event-2".to_owned()],
            pending_inbound: Some(BridgePendingInboundRecord {
                external_event_id: "event-2".to_owned(),
                source_message_id: "202".to_owned(),
                client_prompt_id: "bridge-deadbeef".to_owned(),
                text: "continue after restart".to_owned(),
                attachments: Vec::new(),
            }),
            inbound_turn_origins: vec![BridgeInboundTurnOriginRecord {
                turn_id: "turn-2".to_owned(),
                transport: "thread-transport".to_owned(),
            }],
            updated_at_ms: 123,
        };
        store.save_session_bridges_with_inbound_event(
            &[SessionBridgeRecord {
                recent_inbound_event_ids: vec!["event-1".to_owned()],
                ..bridge.clone()
            }],
            &session.id,
            "event-1",
            nakode_protocol::BridgeContinuationDisposition::Accepted,
        )?;
        store.save_session_bridges_with_inbound_event(
            std::slice::from_ref(&bridge),
            &session.id,
            "event-2",
            nakode_protocol::BridgeContinuationDisposition::Busy,
        )?;
        let mut persisted_bridge = bridge.clone();
        persisted_bridge.recent_inbound_event_ids.clear();
        assert_eq!(
            store.list_session_bridges("/tmp/project")?,
            vec![persisted_bridge.clone()]
        );
        assert_eq!(
            store.find_session_bridge_inbound_event(&session.id, "event-1")?,
            Some(nakode_protocol::BridgeContinuationDisposition::Accepted)
        );
        assert_eq!(
            store.find_session_bridge_inbound_event(&session.id, "event-2")?,
            Some(nakode_protocol::BridgeContinuationDisposition::Busy)
        );
        let conflicting = SessionBridgeRecord {
            pending_inbound: Some(BridgePendingInboundRecord {
                external_event_id: "event-3".to_owned(),
                source_message_id: "203".to_owned(),
                client_prompt_id: "bridge-concurrent-other".to_owned(),
                text: "must not overwrite the first inbox item".to_owned(),
                attachments: Vec::new(),
            }),
            ..bridge.clone()
        };
        assert!(matches!(
            store.save_session_bridges_with_inbound_event(
                &[conflicting],
                &session.id,
                "event-3",
                nakode_protocol::BridgeContinuationDisposition::Accepted,
            ),
            Err(SessionError::BridgePendingInboundConflict { .. })
        ));
        assert_eq!(
            store.find_session_bridge_inbound_event(&session.id, "event-3")?,
            None,
            "a losing distinct event claim rolls back with its stale bridge replacement"
        );
        assert_eq!(
            store.list_session_bridges("/tmp/project")?[0]
                .pending_inbound
                .as_ref()
                .map(|pending| pending.client_prompt_id.as_str()),
            Some("bridge-deadbeef")
        );
        assert!(
            store
                .save_session_bridges_with_inbound_event(
                    std::slice::from_ref(&bridge),
                    &session.id,
                    "event-2",
                    nakode_protocol::BridgeContinuationDisposition::Busy,
                )
                .is_err(),
            "a concurrent/replayed durable claim must fail closed"
        );

        store.delete(&session.id)?;
        let archived_bridges = store.list_session_bridges("/tmp/project")?;
        assert_eq!(archived_bridges.len(), 1);
        assert_eq!(
            archived_bridges[0].lifecycle,
            nakode_protocol::BridgeLifecycle::Archived
        );
        assert_eq!(archived_bridges[0].revision, persisted_bridge.revision + 1);
        assert_eq!(archived_bridges[0].session_id, persisted_bridge.session_id);
        assert_eq!(
            archived_bridges[0].external_thread_id,
            persisted_bridge.external_thread_id
        );

        assert_eq!(
            store.find_session_bridge_inbound_event(&session.id, "event-1")?,
            None,
            "session deletion removes its normalized replay ledger rows"
        );

        store.purge_all()?;
        assert!(store.list_session_bridges("/tmp/project")?.is_empty());
        assert_eq!(
            store.find_session_bridge_inbound_event(&session.id, "event-1")?,
            None
        );
        Ok(())
    }

    #[test]
    fn bridge_replay_ledger_is_bounded_but_preserves_the_pending_prompt() -> Result<(), SessionError>
    {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteSessionRepository::open(directory.path().join("sessions.db"))?;
        let session = store.create(
            CODEX_PROVIDER,
            "provider-ledger",
            "/tmp/project",
            "Ledger",
            None,
        )?;
        let bridge = SessionBridgeRecord {
            session_id: session.id.clone(),
            workspace: "/tmp/project".to_owned(),
            kind: nakode_protocol::OrchestratorKind::Chat,
            lifecycle: nakode_protocol::BridgeLifecycle::Open,
            display_title: "Ledger bridge".to_owned(),
            revision: 1,
            transport: Some("thread-transport".to_owned()),
            external_parent_id: Some("100".to_owned()),
            external_thread_id: Some("101".to_owned()),
            last_projected: None,
            delivery: None,
            live_turn_id: None,
            live_external_message_id: None,
            active_source_message_id: Some("protected".to_owned()),
            recent_inbound_event_ids: Vec::new(),
            pending_inbound: Some(BridgePendingInboundRecord {
                external_event_id: "protected".to_owned(),
                source_message_id: "protected".to_owned(),
                client_prompt_id: "bridge-protected".to_owned(),
                text: "still awaiting backend acknowledgement".to_owned(),
                attachments: Vec::new(),
            }),
            inbound_turn_origins: Vec::new(),
            updated_at_ms: 1,
        };
        store.save_session_bridges_with_inbound_event(
            std::slice::from_ref(&bridge),
            &session.id,
            "protected",
            nakode_protocol::BridgeContinuationDisposition::Accepted,
        )?;
        {
            let mut connection = store
                .connection
                .lock()
                .expect("session database mutex poisoned");
            let transaction = connection.transaction()?;
            for index in 0..MAX_BRIDGE_REPLAY_EVENTS_PER_SESSION + 3 {
                transaction.execute(
                    "INSERT INTO session_bridge_inbound_events
                     (session_id, external_event_id, disposition, recorded_at_ms)
                     VALUES (?1, ?2, 'busy', ?3)",
                    params![
                        session.id,
                        format!("event-{index}"),
                        i64::try_from(index).expect("bounded ledger index")
                    ],
                )?;
            }
            transaction.commit()?;
        }
        store.save_session_bridges_with_inbound_event(
            std::slice::from_ref(&bridge),
            &session.id,
            "newest",
            nakode_protocol::BridgeContinuationDisposition::Busy,
        )?;

        let connection = store
            .connection
            .lock()
            .expect("session database mutex poisoned");
        let count = connection.query_row(
            "SELECT COUNT(*) FROM session_bridge_inbound_events WHERE session_id = ?1",
            [&session.id],
            |row| row.get::<_, i64>(0),
        )?;
        assert_eq!(
            usize::try_from(count).expect("non-negative replay count"),
            MAX_BRIDGE_REPLAY_EVENTS_PER_SESSION + 1
        );
        drop(connection);
        assert_eq!(
            store.find_session_bridge_inbound_event(&session.id, "protected")?,
            Some(nakode_protocol::BridgeContinuationDisposition::Accepted)
        );
        Ok(())
    }

    #[test]
    fn bridge_batch_is_atomic_when_external_thread_identity_conflicts() -> Result<(), SessionError>
    {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteSessionRepository::open(directory.path().join("sessions.db"))?;
        let first = SessionBridgeRecord {
            session_id: "session-a".to_owned(),
            workspace: "/tmp/project".to_owned(),
            kind: nakode_protocol::OrchestratorKind::Chat,
            lifecycle: nakode_protocol::BridgeLifecycle::Open,
            display_title: "First".to_owned(),
            revision: 1,
            transport: Some("thread-transport".to_owned()),
            external_parent_id: Some("100".to_owned()),
            external_thread_id: Some("101".to_owned()),
            last_projected: None,
            delivery: None,
            live_turn_id: None,
            live_external_message_id: None,
            active_source_message_id: None,
            recent_inbound_event_ids: Vec::new(),
            pending_inbound: None,
            inbound_turn_origins: Vec::new(),
            updated_at_ms: 1,
        };
        let second = SessionBridgeRecord {
            session_id: "session-b".to_owned(),
            display_title: "Second".to_owned(),
            ..first.clone()
        };

        assert!(store.save_session_bridges(&[first, second]).is_err());
        assert!(store.list_session_bridges("/tmp/project")?.is_empty());
        Ok(())
    }

    #[test]
    fn accepted_owner_prompts_are_immutable_ordered_and_deleted_with_the_session()
    -> Result<(), SessionError> {
        let directory = tempfile::tempdir().expect("tempdir");
        let database = directory.path().join("owner-prompts.db");
        let store = SqliteSessionRepository::open(&database)?;
        let session = store.create(
            CODEX_PROVIDER,
            "provider-owner-prompts",
            "/tmp/project",
            "Owner transcript",
            Some("model-a"),
        )?;
        let first = PersistedOwnerPrompt {
            prompt_id: "prompt-z".to_owned(),
            raw_text: "first raw prompt".to_owned(),
            source_transport: Some("slack".to_owned()),
            dispatch_pending: true,
        };
        let second = PersistedOwnerPrompt {
            prompt_id: "prompt-a".to_owned(),
            raw_text: "second raw prompt".to_owned(),
            source_transport: None,
            dispatch_pending: true,
        };
        let rolled_back = PersistedOwnerPrompt {
            prompt_id: "prompt-rolled-back".to_owned(),
            raw_text: "must not survive a failed bridge checkpoint".to_owned(),
            source_transport: None,
            dispatch_pending: true,
        };
        assert!(
            store
                .save_prompt_dispatch_checkpoint(
                    &[(session.id.clone(), rolled_back)],
                    &[],
                    Some((
                        &session.id,
                        "event-without-bridge",
                        nakode_protocol::BridgeContinuationDisposition::Accepted,
                    )),
                )
                .is_err()
        );
        assert!(
            load_owner_prompts(
                &store.connection.lock().expect("database mutex"),
                &session.id,
            )?
            .is_empty()
        );

        store.record_owner_prompt(&session.id, &first)?;
        store.record_owner_prompt(&session.id, &second)?;
        store.record_owner_prompt(&session.id, &first)?;
        store.acknowledge_owner_prompt_dispatch(&session.id, &first.prompt_id)?;
        store.acknowledge_owner_prompt_dispatch(&session.id, &first.prompt_id)?;
        assert!(matches!(
            store.record_owner_prompt(
                &session.id,
                &PersistedOwnerPrompt {
                    prompt_id: first.prompt_id.clone(),
                    raw_text: "conflicting text".to_owned(),
                    source_transport: None,
                    dispatch_pending: true,
                },
            ),
            Err(SessionError::OwnerPromptConflict { .. })
        ));
        assert!(matches!(
            store.record_owner_prompt(
                &session.id,
                &PersistedOwnerPrompt {
                    prompt_id: first.prompt_id.clone(),
                    raw_text: first.raw_text.clone(),
                    source_transport: Some("discord".to_owned()),
                    dispatch_pending: true,
                },
            ),
            Err(SessionError::OwnerPromptConflict { .. })
        ));
        drop(store);

        let restarted = SqliteSessionRepository::open(&database)?;
        let mut acknowledged_first = first.clone();
        acknowledged_first.dispatch_pending = false;
        assert_eq!(
            restarted.find(&session.id)?.expect("session").owner_prompts,
            vec![acknowledged_first, second]
        );
        restarted.delete(&session.id)?;
        let remaining = restarted
            .connection
            .lock()
            .expect("database mutex")
            .query_row(
                "SELECT COUNT(*) FROM accepted_owner_prompts WHERE session_id = ?1",
                [&session.id],
                |row| row.get::<_, i64>(0),
            )?;
        assert_eq!(remaining, 0);
        Ok(())
    }

    #[test]
    fn owner_activity_is_monotonic_and_generic_touch_does_not_change_it() -> Result<(), SessionError>
    {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteSessionRepository::open(directory.path().join("activity.db"))?;
        let session = store.create(
            CODEX_PROVIDER,
            "provider-activity",
            "/tmp/project",
            "Initial owner prompt",
            Some("model-a"),
        )?;
        {
            let connection = store.connection.lock().expect("database mutex");
            connection.execute(
                "UPDATE sessions SET updated_at = 1, last_owner_activity_at = 4000000000 WHERE id = ?1",
                [&session.id],
            )?;
        }

        store.touch(&session.id)?;
        store.record_owner_activity(&session.id)?;
        let turn = PersistedTurnConfiguration {
            id: "turn-terminal".to_owned(),
            model: Some("model-a".to_owned()),
            options: ModelOptions::default(),
            outcome: TurnOutcome::Completed,
        };
        store.update_last_turn(&session.id, &turn)?;
        let future = store.find(&session.id)?.expect("session");
        assert!(future.updated_at > 1);
        assert_eq!(future.last_owner_activity_at, Some(4_000_000_000));

        store.connection.lock().expect("database mutex").execute(
            "UPDATE sessions SET last_owner_activity_at = NULL WHERE id = ?1",
            [&session.id],
        )?;
        store.record_owner_activity(&session.id)?;
        assert!(
            store
                .find(&session.id)?
                .expect("recorded activity")
                .last_owner_activity_at
                .is_some()
        );
        Ok(())
    }

    #[test]
    fn legacy_session_schema_adds_nullable_owner_activity_without_data_loss()
    -> Result<(), SessionError> {
        let directory = tempfile::tempdir().expect("tempdir");
        let database = directory.path().join("legacy-activity.db");
        let legacy = Connection::open(&database)?;
        legacy.execute_batch(
            "CREATE TABLE sessions (
               id TEXT PRIMARY KEY,
               provider TEXT NOT NULL,
               provider_session_id TEXT NOT NULL,
               workspace TEXT NOT NULL,
               title TEXT NOT NULL,
               model TEXT,
               created_at INTEGER NOT NULL,
               updated_at INTEGER NOT NULL,
               UNIQUE(provider, provider_session_id)
             );
             INSERT INTO sessions
               (id, provider, provider_session_id, workspace, title, model, created_at, updated_at)
             VALUES
               ('legacy', 'openai-codex', 'native', '/tmp/project', 'Legacy', NULL, 10, 12);",
        )?;
        drop(legacy);

        let store = SqliteSessionRepository::open(&database)?;
        let restored = store.find("legacy")?.expect("legacy row");
        assert_eq!(restored.created_at, 10);
        assert_eq!(restored.updated_at, 12);
        assert_eq!(restored.last_owner_activity_at, None);
        assert_eq!(restored.enabled_skill_ids, None);
        assert!(!restored.code_mode);

        store.set_session_skill_snapshot("legacy", &[])?;
        drop(store);
        let upgraded = SqliteSessionRepository::open(&database)?
            .find("legacy")?
            .expect("upgraded legacy row");
        assert_eq!(upgraded.enabled_skill_ids, Some(Vec::new()));
        Ok(())
    }

    #[test]
    fn persists_and_orders_sessions() -> Result<(), SessionError> {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteSessionRepository::open(directory.path().join("sessions.db"))?;
        let first = store.create(
            CODEX_PROVIDER,
            "provider-1",
            "/tmp/project",
            "First prompt",
            Some("model"),
        )?;
        let second = store.create(
            CODEX_PROVIDER,
            "provider-2",
            "/tmp/project",
            "Second prompt",
            None,
        )?;
        assert_eq!(store.find(&first.id)?, Some(first));
        let recent = store.list_recent("/tmp/project", 10)?;
        assert_eq!(recent.len(), 2);
        assert!(recent.iter().any(|record| record.id == second.id));

        let models = vec![
            ModelInfo {
                provider: CODEX_PROVIDER.to_owned(),
                id: "model-a".to_owned(),
                is_default: true,
                capabilities: crate::backend::ModelCapabilities {
                    reasoning_efforts: vec!["low".to_owned(), "high".to_owned()],
                },
            },
            ModelInfo {
                provider: CODEX_PROVIDER.to_owned(),
                id: "model-b".to_owned(),
                is_default: false,
                capabilities: crate::codex::model_capabilities(),
            },
        ];
        store.update_model(&second.id, Some("model-a"), &ModelOptions::default())?;
        assert_eq!(
            store.find(&second.id)?.and_then(|record| record.model),
            Some("model-a".to_owned())
        );
        store.replace_models(CODEX_PROVIDER, &models)?;
        assert_eq!(store.list_models(CODEX_PROVIDER)?, models);
        store.set_default_model(CODEX_PROVIDER, "model-b")?;
        let preferred = store.list_models(CODEX_PROVIDER)?;
        assert_eq!(preferred[0].id, "model-b");
        assert!(preferred[0].is_default);
        assert_eq!(preferred[1].id, "model-a");
        assert!(!preferred[1].is_default);
        store.replace_models(CODEX_PROVIDER, &models)?;
        assert_eq!(store.list_models(CODEX_PROVIDER)?, preferred);
        store.replace_models(CODEX_PROVIDER, &[])?;
        assert!(store.list_models(CODEX_PROVIDER)?.is_empty());
        Ok(())
    }

    #[test]
    fn restores_next_and_last_owner_turn_configuration() -> Result<(), SessionError> {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("sessions.db");
        let store = SqliteSessionRepository::open(&path)?;
        let session = store.create(
            CODEX_PROVIDER,
            "provider-turn-config",
            "/tmp/project",
            "Configured prompt",
            Some("openai-codex/model-a"),
        )?;
        let next = ModelOptions {
            reasoning_effort: Some("high".to_owned()),
            fast_mode: true,
        };
        store.update_model(&session.id, Some("openai-codex/model-b"), &next)?;
        let last = PersistedTurnConfiguration {
            id: "turn-immutable".to_owned(),
            model: Some("openai-codex/model-a".to_owned()),
            options: ModelOptions {
                reasoning_effort: Some("medium".to_owned()),
                fast_mode: false,
            },
            outcome: TurnOutcome::Interrupted,
        };
        store.update_last_turn(&session.id, &last)?;
        drop(store);

        let restored = SqliteSessionRepository::open(&path)?
            .find(&session.id)?
            .expect("restored session");
        assert_eq!(restored.model.as_deref(), Some("openai-codex/model-b"));
        assert_eq!(restored.model_options, next);
        assert_eq!(restored.last_turn, Some(last.clone()));
        assert_eq!(restored.owner_turns, vec![last]);
        Ok(())
    }

    #[test]
    fn transitions_primary_provider_without_changing_logical_identity() -> Result<(), SessionError>
    {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("transition.db");
        let store = SqliteSessionRepository::open(&path)?;
        let original = store.create(
            CODEX_PROVIDER,
            "codex-primary",
            "/tmp/project",
            "Transition me",
            Some("openai-codex/model-a"),
        )?;
        let options = ModelOptions {
            reasoning_effort: Some("high".to_owned()),
            fast_mode: false,
        };

        store.transition_primary(
            &original.id,
            DEVIN_PROVIDER,
            "devin-primary",
            Some("devin-acp/model-b"),
            &options,
        )?;
        drop(store);

        let restored = SqliteSessionRepository::open(&path)?
            .find(&original.id)?
            .expect("same logical session");
        assert_eq!(restored.id, original.id);
        assert_eq!(restored.provider, DEVIN_PROVIDER);
        assert_eq!(restored.provider_session_id, "devin-primary");
        assert_eq!(restored.model.as_deref(), Some("devin-acp/model-b"));
        assert_eq!(restored.model_options, options);
        assert!(
            restored
                .owned_provider_sessions
                .contains(&(CODEX_PROVIDER.to_owned(), "codex-primary".to_owned()))
        );
        Ok(())
    }

    #[test]
    fn pending_creation_identity_is_not_archived_as_provider_history() -> Result<(), SessionError> {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("pending-transition.db");
        let store = SqliteSessionRepository::open(&path)?;
        let logical_id = "logical-pending-transition";
        let pending_id = pending_provider_session_id(logical_id);
        store.create_with_id(
            logical_id,
            CODEX_PROVIDER,
            &pending_id,
            "/tmp/project",
            "/tmp/project",
            "Pending transition",
            Some("openai-codex/model-a"),
            &ModelOptions::default(),
            None,
        )?;

        store.transition_primary_with_account(
            logical_id,
            CODEX_PROVIDER,
            None,
            "provider-created",
            Some("openai-codex/model-a"),
            &ModelOptions::default(),
        )?;
        drop(store);

        let restored = SqliteSessionRepository::open(&path)?
            .find(logical_id)?
            .expect("transitioned session");
        assert_eq!(restored.provider_session_id, "provider-created");
        assert!(
            !restored
                .owned_provider_sessions
                .contains(&(CODEX_PROVIDER.to_owned(), pending_id,))
        );
        Ok(())
    }

    #[test]
    fn session_code_mode_defaults_off_and_round_trips_across_restart() -> Result<(), SessionError> {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("session-code-mode.db");
        let store = SqliteSessionRepository::open(&path)?;
        let created = store.create(
            CODEX_PROVIDER,
            "provider-session",
            "/tmp/project",
            "Code Mode",
            None,
        )?;
        assert!(!created.code_mode);
        store.set_session_code_mode(&created.id, true)?;
        drop(store);

        let restored = SqliteSessionRepository::open(&path)?
            .find(&created.id)?
            .expect("persisted session");
        assert!(restored.code_mode);
        Ok(())
    }

    #[test]
    fn session_skill_snapshot_preserves_all_enabled_identities_across_restart()
    -> Result<(), SessionError> {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("session-skills.db");
        let store = SqliteSessionRepository::open(&path)?;
        let enabled = vec!["stable.review".to_owned(), "stable.testing".to_owned()];
        let created = store.create_with_id(
            "logical-session",
            CODEX_PROVIDER,
            "provider-session",
            "/tmp/project",
            "/tmp/project",
            "Skills",
            None,
            &ModelOptions::default(),
            Some(&enabled),
        )?;
        assert_eq!(
            created.enabled_skill_ids.as_deref(),
            Some(enabled.as_slice())
        );
        store.set_session_skill_snapshot("logical-session", &[])?;
        assert_eq!(
            store
                .find("logical-session")?
                .expect("already snapshotted session")
                .enabled_skill_ids
                .as_deref(),
            Some(enabled.as_slice()),
            "legacy migration must not overwrite an explicit snapshot"
        );
        drop(store);

        let restored = SqliteSessionRepository::open(path)?
            .find("logical-session")?
            .expect("restored session");
        assert_eq!(restored.enabled_skill_ids, Some(enabled));
        Ok(())
    }

    #[test]
    fn persists_independent_profile_skill_preferences_across_restart() -> Result<(), SessionError> {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("skills.db");
        let store = SqliteSessionRepository::open(&path)?;
        store.set_skill_preference(&SkillPreference {
            profile_id: "profile-a".to_owned(),
            skill_id: "stable.review".to_owned(),
            last_name: "Review".to_owned(),
            last_description: "Reviews changes".to_owned(),
            enabled: false,
        })?;
        store.set_skill_preference(&SkillPreference {
            profile_id: "profile-b".to_owned(),
            skill_id: "stable.review".to_owned(),
            last_name: "Review".to_owned(),
            last_description: "Reviews changes".to_owned(),
            enabled: true,
        })?;
        store.set_skill_preference(&SkillPreference {
            profile_id: "profile-a".to_owned(),
            skill_id: "stable.testing".to_owned(),
            last_name: "Testing".to_owned(),
            last_description: "Runs tests".to_owned(),
            enabled: false,
        })?;
        store.set_skill_preference(&SkillPreference {
            profile_id: "profile-a".to_owned(),
            skill_id: "stable.review".to_owned(),
            last_name: "Review".to_owned(),
            last_description: "Reviews changes".to_owned(),
            enabled: true,
        })?;
        let session = store.create("codex", "provider-session", "/workspace", "Session", None)?;
        store.bind_session_skill_profile(&session.id, "profile-a")?;
        drop(store);

        let restored = SqliteSessionRepository::open(path)?;
        let profile_a = restored.list_skill_preferences("profile-a")?;
        let profile_b = restored.list_skill_preferences("profile-b")?;
        assert_eq!(profile_a.len(), 2);
        assert!(
            profile_a
                .iter()
                .find(|preference| preference.skill_id == "stable.review")
                .is_some_and(|preference| preference.enabled)
        );
        assert!(
            profile_a
                .iter()
                .find(|preference| preference.skill_id == "stable.testing")
                .is_some_and(|preference| !preference.enabled)
        );
        assert_eq!(profile_b.len(), 1);
        assert!(profile_b[0].enabled);
        assert_eq!(
            restored.session_skill_profile(&session.id)?.as_deref(),
            Some("profile-a")
        );
        assert!(
            restored
                .bind_session_skill_profile(&session.id, "profile-b")
                .is_err()
        );
        let all = restored.list_all_skill_preferences()?;
        assert_eq!(all.len(), 3);
        assert_eq!(
            all.iter()
                .filter(|preference| preference.profile_id == "profile-a")
                .count(),
            2
        );
        assert!(
            restored
                .list_skill_preferences("legacy-profile")?
                .is_empty()
        );
        Ok(())
    }

    #[test]
    fn persists_provider_enablement() -> Result<(), SessionError> {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("providers.db");
        let store = SqliteSessionRepository::open(&path)?;
        let credentials =
            crate::credential::SqliteCredentialStore::open(&path).expect("credential store");
        let providers = store.list_providers()?;
        assert_eq!(providers.len(), 6);
        assert!(
            providers
                .iter()
                .any(|provider| provider.provider == crate::backend::CLAUDE_PROVIDER)
        );
        assert!(
            providers
                .iter()
                .any(|provider| provider.provider == crate::backend::KIMI_PROVIDER)
        );
        assert!(
            providers
                .iter()
                .any(|provider| provider.provider == crate::backend::GLM_PROVIDER)
        );
        assert!(providers.iter().all(|provider| !provider.enabled));

        let metadata = serde_json::json!({
            "credential_store": "codex_managed",
            "account": "fixture",
        });
        crate::credential::CredentialStore::put(
            &credentials,
            CODEX_PROVIDER,
            &crate::credential::Credential {
                kind: "chatgpt_device_code".to_owned(),
                secret: crate::credential::SecretValue::new(metadata),
            },
        )
        .expect("save credential");
        store.set_provider_enabled(CODEX_PROVIDER, true)?;
        let codex = store
            .list_providers()?
            .into_iter()
            .find(|provider| provider.provider == CODEX_PROVIDER)
            .expect("Codex provider");
        assert!(codex.enabled);
        assert_eq!(
            codex.credential.map(|credential| credential.kind),
            Some("chatgpt_device_code".to_owned())
        );
        store.set_provider_enabled(CODEX_PROVIDER, false)?;
        crate::credential::CredentialStore::delete(&credentials, CODEX_PROVIDER)
            .expect("delete credential");
        let codex = store
            .list_providers()?
            .into_iter()
            .find(|provider| provider.provider == CODEX_PROVIDER)
            .expect("Codex provider");
        assert!(!codex.enabled);
        assert!(codex.credential.is_none());

        store.set_provider_enabled(DEVIN_PROVIDER, false)?;
        let devin = store
            .list_providers()?
            .into_iter()
            .find(|provider| provider.provider == DEVIN_PROVIDER)
            .expect("Devin provider");
        assert!(!devin.enabled);
        Ok(())
    }

    #[test]
    fn persists_independent_provider_model_filters() -> Result<(), SessionError> {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteSessionRepository::open(directory.path().join("providers.db"))?;
        let codex = store
            .list_providers()?
            .into_iter()
            .find(|provider| provider.provider == CODEX_PROVIDER)
            .expect("Codex provider");
        assert!(!codex.model_filter_enabled);
        assert!(codex.selected_model_ids.is_empty());

        store.set_provider_model_filter(
            CODEX_PROVIDER,
            true,
            &[
                "openai-codex/stale-model".to_owned(),
                "openai-codex/model-a".to_owned(),
            ],
        )?;
        store.set_provider_model_filter(DEVIN_PROVIDER, true, &[])?;
        let providers = store.list_providers()?;
        let codex = providers
            .iter()
            .find(|provider| provider.provider == CODEX_PROVIDER)
            .expect("Codex provider");
        assert!(codex.model_filter_enabled);
        assert_eq!(
            codex.selected_model_ids,
            ["openai-codex/stale-model", "openai-codex/model-a"]
        );
        let devin = providers
            .iter()
            .find(|provider| provider.provider == DEVIN_PROVIDER)
            .expect("Devin provider");
        assert!(devin.model_filter_enabled);
        assert!(devin.selected_model_ids.is_empty());
        Ok(())
    }

    #[test]
    fn rejects_mutations_for_unknown_records() -> Result<(), SessionError> {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteSessionRepository::open(directory.path().join("unknown.db"))?;

        assert!(matches!(
            store.touch("missing-session"),
            Err(SessionError::SessionNotFound(id)) if id == "missing-session"
        ));
        assert!(matches!(
            store.update_model("missing-session", Some("model"), &ModelOptions::default()),
            Err(SessionError::SessionNotFound(id)) if id == "missing-session"
        ));
        assert!(matches!(
            store.set_provider_enabled("missing-provider", true),
            Err(SessionError::ProviderNotFound(provider)) if provider == "missing-provider"
        ));
        Ok(())
    }

    /// Deleting a session must take everything under it, including the one table no cascade reaches.
    ///
    /// `native_runtime_sessions` holds the transcripts and has no foreign key to `sessions`, so it is
    /// the table a naive `DELETE FROM sessions` leaves behind — permanently, since nothing else keys off
    /// it. That orphan is the whole reason this test writes a row into it by hand.
    #[test]
    fn deletes_a_session_with_its_runs_and_native_history() -> Result<(), SessionError> {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteSessionRepository::open(directory.path().join("sessions.db"))?;
        let doomed = store.create(
            CODEX_PROVIDER,
            "doomed-provider-session",
            "/tmp/project",
            "Finished work",
            Some("model-a"),
        )?;
        let kept = store.create(
            CODEX_PROVIDER,
            "kept-provider-session",
            "/tmp/project",
            "Live work",
            Some("model-a"),
        )?;
        store.save_subagent(&SubagentRecord {
            parent_session_id: doomed.id.clone(),
            id: "agent-1".to_owned(),
            agent: "explorer".to_owned(),
            provider: CODEX_PROVIDER.to_owned(),
            model: None,
            provider_session_id: Some("child-provider-session".to_owned()),
            input_tokens: 0,
            output_tokens: 0,
            cached_input_tokens: 0,
            cache_write_tokens: 0,
            objective: "Map persistence".to_owned(),
            status: SubagentStatus::Completed,
            latest_activity: "Completed".to_owned(),
            transcript: vec![TranscriptEntry {
                id: "entry-1".to_owned(),
                key: None,
                kind: EntryKind::User,
                title: "PARENT".to_owned(),
                body: "Delegated task".to_owned(),
                status: EntryStatus::Complete,
                created_at_ms: None,
                provider_id: None,
                model_id: None,
                owner_turn_id: None,
                reasoning_effort: None,
                fast_mode: None,
                source_transport: None,
                tool_audit_json: None,
            }],
            observability: SubagentObservability::default(),
            transcript_has_earlier: false,
        })?;
        let native_rows = |provider_session_id: &str| -> Result<i64, SessionError> {
            let connection = store
                .connection
                .lock()
                .expect("session database mutex poisoned");
            Ok(connection.query_row(
                "SELECT COUNT(*) FROM native_runtime_sessions WHERE provider = ?1 AND session_id = ?2",
                params![CODEX_PROVIDER, provider_session_id],
                |row| row.get::<_, i64>(0),
            )?)
        };
        {
            let connection = store
                .connection
                .lock()
                .expect("session database mutex poisoned");
            for provider_session_id in ["doomed-provider-session", "kept-provider-session"] {
                connection.execute(
                    "INSERT INTO native_runtime_sessions (provider, session_id, session_json, updated_at)
                     VALUES (?1, ?2, ?3, unixepoch())",
                    params![CODEX_PROVIDER, provider_session_id, "{}"],
                )?;
            }
        }
        assert_eq!(native_rows("doomed-provider-session")?, 1);
        assert_eq!(store.list_subagents(&doomed.id)?.len(), 1);

        store.delete(&doomed.id)?;

        assert_eq!(store.find(&doomed.id)?, None);
        // The cascade, which only fires because the connection sets `PRAGMA foreign_keys = ON`.
        assert!(store.list_subagents(&doomed.id)?.is_empty());
        // The table no cascade reaches.
        assert_eq!(native_rows("doomed-provider-session")?, 0);
        // And nothing belonging to the session beside it moved.
        assert_eq!(store.find(&kept.id)?, Some(kept));
        assert_eq!(native_rows("kept-provider-session")?, 1);

        // Deleting what is already gone is an idempotent success. This lets a retry converge when a
        // prior response was lost after the durable transaction committed.
        store.delete(&doomed.id)?;
        assert_eq!(store.find(&doomed.id)?, None);
        Ok(())
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn persists_subagent_run_and_transcript_projection() -> Result<(), SessionError> {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteSessionRepository::open(directory.path().join("subagents.db"))?;
        let parent = store.create(
            CODEX_PROVIDER,
            "parent-provider-session",
            "/tmp/project",
            "Parent work",
            Some("model-a"),
        )?;
        let record = SubagentRecord {
            parent_session_id: parent.id.clone(),
            id: "agent-1".to_owned(),
            agent: "explorer".to_owned(),
            provider: CODEX_PROVIDER.to_owned(),
            model: None,
            provider_session_id: Some("child-provider-session".to_owned()),
            input_tokens: 0,
            output_tokens: 0,
            cached_input_tokens: 0,
            cache_write_tokens: 0,
            objective: "Map persistence".to_owned(),
            status: SubagentStatus::Partial,
            latest_activity: "Partial result preserved".to_owned(),
            transcript: vec![
                TranscriptEntry {
                    id: "entry-1".to_owned(),
                    key: None,
                    kind: EntryKind::User,
                    title: "PARENT".to_owned(),
                    body: "Delegated task: Map persistence".to_owned(),
                    status: EntryStatus::Complete,
                    created_at_ms: Some(900),
                    provider_id: None,
                    model_id: None,
                    owner_turn_id: None,
                    reasoning_effort: None,
                    fast_mode: None,
                    source_transport: None,
                    tool_audit_json: None,
                },
                TranscriptEntry {
                    id: "entry-2".to_owned(),
                    key: Some("assistant-1".to_owned()),
                    kind: EntryKind::Assistant,
                    title: "ASSISTANT".to_owned(),
                    body: "Persistence report".to_owned(),
                    status: EntryStatus::Complete,
                    created_at_ms: Some(1_100),
                    provider_id: Some(CODEX_PROVIDER.to_owned()),
                    model_id: Some("openai-codex/gpt-5.4".to_owned()),
                    owner_turn_id: Some("turn-child-1".to_owned()),
                    reasoning_effort: Some("high".to_owned()),
                    fast_mode: Some(true),
                    source_transport: Some("dashboard".to_owned()),
                    tool_audit_json: Some(
                        r#"{"version":1,"callId":"call-1","kind":"native"}"#.to_owned(),
                    ),
                },
            ],
            observability: SubagentObservability {
                parent_run_id: Some("agent-root".to_owned()),
                invocation_turn_id: Some("turn-owner".to_owned()),
                invocation_call_id: Some("call-delegate".to_owned()),
                originating_owner_entry_id: Some("entry-owner".to_owned()),
                archetype_purpose: "Read-only persistence scout".to_owned(),
                policy_json: r#"{"slug":"explorer","description":"Read-only persistence scout","tool_profile":"read_only","allowed_capabilities":["filesystem_read"],"denied_capabilities":["filesystem_write"],"allowed_tools":["read","grep"],"denied_tools":["write"],"timeout_seconds":300,"max_turns":5,"require_parent_attribution":true}"#.to_owned(),
                remaining_delegation_depth: 0,
                started_at_ms: 1_000,
                ended_at_ms: Some(1_250),
                termination_kind: Some("hard_turn_limit".to_owned()),
                termination_detail: Some("maximum reached".to_owned()),
                objective_mismatch_handoff: None,
                salvage: Some(SubagentSalvage {
                    terminal_reason: "hard_turn_limit: maximum reached".to_owned(),
                    original_objective: "Map persistence".to_owned(),
                    completed_work: vec!["[entry-2] read persistence".to_owned()],
                    verified_evidence: vec![SalvagedEvidence {
                        entry_id: "entry-2".to_owned(),
                        title: "read persistence".to_owned(),
                        body: "Persistence report".to_owned(),
                        truncated: false,
                    }],
                    last_successful_evidence: Some(SalvagedEvidence {
                        entry_id: "entry-2".to_owned(),
                        title: "read persistence".to_owned(),
                        body: "Persistence report".to_owned(),
                        truncated: false,
                    }),
                    unresolved_questions: vec!["Complete synthesis".to_owned()],
                    continuation: ContinuationProposition {
                        verified_findings: vec!["Evidence retained at entry-2".to_owned()],
                        unresolved_boundary: "Complete synthesis".to_owned(),
                        why_it_matters: "Objective remains incomplete".to_owned(),
                        recommended_archetype: "explorer".to_owned(),
                        follow_up_objective: "Finish the persistence map".to_owned(),
                        inherited_evidence: vec!["[entry-2] read persistence".to_owned()],
                        can_proceed_independently: false,
                    },
                    can_resume: false,
                    redacted: false,
                    truncated: false,
                }),
                continued_from_run_id: Some("agent-0".to_owned()),
                continued_by_run_id: Some("agent-2".to_owned()),
                continuation_depth: 1,
                additional_turns: Some(12),
                inherited_evidence: vec![SalvagedEvidence {
                    entry_id: "entry-0".to_owned(),
                    title: "source evidence".to_owned(),
                    body: "retained source output".to_owned(),
                    truncated: false,
                }],
            },
        transcript_has_earlier: true,
        };

        store.save_subagent(&record)?;
        assert_eq!(store.list_subagents(&parent.id)?, vec![record.clone()]);

        let mut updated = record;
        updated.latest_activity = "Reviewed".to_owned();
        updated.transcript[1].body = "Updated persistence report".to_owned();
        store.save_subagent(&updated)?;
        assert_eq!(store.list_subagents(&parent.id)?, vec![updated]);
        Ok(())
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn legacy_orchestration_schema_migrates_delegation_attribution_round_trip()
    -> Result<(), SessionError> {
        let directory = tempfile::tempdir().expect("tempdir");
        let database = directory.path().join("legacy-subagents.db");
        let legacy = Connection::open(&database)?;
        legacy.execute_batch(
            "CREATE TABLE orchestration_runs (
               parent_session_id TEXT NOT NULL,
               id TEXT NOT NULL,
               agent_slug TEXT NOT NULL,
               provider TEXT NOT NULL,
               model TEXT,
               provider_session_id TEXT,
               input_tokens INTEGER NOT NULL DEFAULT 0,
               output_tokens INTEGER NOT NULL DEFAULT 0,
               cached_input_tokens INTEGER NOT NULL DEFAULT 0,
               cache_write_tokens INTEGER NOT NULL DEFAULT 0,
               objective TEXT NOT NULL,
               status TEXT NOT NULL,
               latest_activity TEXT NOT NULL,
               parent_run_id TEXT,
               archetype_purpose TEXT NOT NULL DEFAULT '',
               policy_json TEXT NOT NULL DEFAULT '{}',
               remaining_delegation_depth INTEGER NOT NULL DEFAULT 0,
               started_at_ms INTEGER NOT NULL DEFAULT 0,
               ended_at_ms INTEGER,
               termination_kind TEXT,
               termination_detail TEXT,
               objective_mismatch_handoff TEXT,
               salvage_json TEXT,
               continued_from_run_id TEXT,
               continued_by_run_id TEXT,
               continuation_depth INTEGER NOT NULL DEFAULT 0,
               additional_turns INTEGER,
               inherited_evidence_json TEXT NOT NULL DEFAULT '[]',
               transcript_has_earlier INTEGER NOT NULL DEFAULT 0,
               created_at INTEGER NOT NULL,
               updated_at INTEGER NOT NULL,
               PRIMARY KEY(parent_session_id, id)
             );",
        )?;
        legacy.execute(
            "INSERT INTO orchestration_runs
             (parent_session_id, id, agent_slug, provider, objective, status, latest_activity,
              created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                "legacy-parent",
                "legacy-bounded-run",
                "explorer",
                CODEX_PROVIDER,
                "Legacy bounded history",
                "working",
                "Retained suffix",
                1_i64,
                1_i64,
            ],
        )?;
        drop(legacy);

        let store = SqliteSessionRepository::open(&database)?;
        let restored_legacy = store
            .list_subagents("legacy-parent")?
            .pop()
            .expect("legacy run");
        assert!(restored_legacy.transcript_has_earlier);
        let parent = store.create(
            CODEX_PROVIDER,
            "parent-provider-session",
            "/tmp/project",
            "Parent work",
            Some("model-a"),
        )?;
        let record = SubagentRecord {
            parent_session_id: parent.id.clone(),
            id: "agent-migrated".to_owned(),
            agent: "explorer".to_owned(),
            provider: CODEX_PROVIDER.to_owned(),
            model: None,
            provider_session_id: None,
            input_tokens: 0,
            output_tokens: 0,
            cached_input_tokens: 0,
            cache_write_tokens: 0,
            objective: "Verify migration".to_owned(),
            status: SubagentStatus::Working,
            latest_activity: "Migrated".to_owned(),
            transcript: Vec::new(),
            observability: SubagentObservability {
                invocation_turn_id: Some("turn-migrated".to_owned()),
                invocation_call_id: Some("call-migrated".to_owned()),
                originating_owner_entry_id: Some("owner-migrated".to_owned()),
                ..SubagentObservability::default()
            },
            transcript_has_earlier: false,
        };

        store.save_subagent(&record)?;
        let restored = store
            .list_subagents(&parent.id)?
            .pop()
            .expect("migrated run");
        assert_eq!(restored, record);
        let columns = {
            let connection = store.connection.lock().expect("database mutex");
            let mut statement = connection.prepare("PRAGMA table_info(orchestration_runs)")?;
            statement
                .query_map([], |row| row.get::<_, String>(1))?
                .collect::<Result<Vec<_>, _>>()?
        };
        for expected in [
            "invocation_turn_id",
            "invocation_call_id",
            "originating_owner_entry_id",
        ] {
            assert!(columns.iter().any(|column| column == expected));
        }
        Ok(())
    }

    #[test]
    fn continuation_source_and_successor_persist_atomically() -> Result<(), SessionError> {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteSessionRepository::open(directory.path().join("continuation.db"))?;
        let parent = store.create(
            CODEX_PROVIDER,
            "parent-provider-session",
            "/tmp/project",
            "Parent work",
            Some("model-a"),
        )?;
        let source = SubagentRecord {
            parent_session_id: parent.id.clone(),
            id: "source".to_owned(),
            agent: "explorer".to_owned(),
            provider: CODEX_PROVIDER.to_owned(),
            model: None,
            provider_session_id: None,
            input_tokens: 0,
            output_tokens: 0,
            cached_input_tokens: 0,
            cache_write_tokens: 0,
            objective: "Inspect".to_owned(),
            status: SubagentStatus::Partial,
            latest_activity: "Partial".to_owned(),
            transcript: Vec::new(),
            observability: SubagentObservability::default(),
            transcript_has_earlier: false,
        };
        store.save_subagent(&source)?;

        let mut linked_source = source.clone();
        linked_source.observability.continued_by_run_id = Some("successor".to_owned());
        let mut invalid_successor = source.clone();
        invalid_successor.parent_session_id = "missing-parent".to_owned();
        invalid_successor.id = "successor".to_owned();
        invalid_successor.observability.continued_from_run_id = Some("source".to_owned());
        assert!(
            store
                .save_subagent_continuation(&linked_source, &invalid_successor)
                .is_err()
        );
        assert_eq!(store.list_subagents(&parent.id)?, vec![source.clone()]);

        let mut successor = invalid_successor;
        successor.parent_session_id = parent.id.clone();
        store.save_subagent_continuation(&linked_source, &successor)?;
        assert_eq!(
            store.list_subagents(&parent.id)?,
            vec![linked_source, successor]
        );
        Ok(())
    }

    #[test]
    fn migrates_legacy_model_metadata_to_slug_only_catalog() -> Result<(), SessionError> {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("legacy-models.db");
        let connection = Connection::open(&path)?;
        connection.execute_batch(
            "CREATE TABLE backend_models (
               provider TEXT NOT NULL,
               model_id TEXT NOT NULL,
               display_name TEXT NOT NULL,
               description TEXT NOT NULL,
               is_default INTEGER NOT NULL,
               cached_at INTEGER NOT NULL,
               PRIMARY KEY(provider, model_id)
             );
             INSERT INTO backend_models VALUES
               ('openai-codex', 'legacy-model', 'Old name', 'Old description', 1, 1);",
        )?;
        drop(connection);

        let store = SqliteSessionRepository::open(&path)?;
        assert_eq!(
            store.list_models(CODEX_PROVIDER)?,
            vec![ModelInfo {
                provider: CODEX_PROVIDER.to_owned(),
                id: "legacy-model".to_owned(),
                is_default: true,
                capabilities: crate::backend::ModelCapabilities::default(),
            }]
        );
        let connection = store.connection.lock().expect("database mutex");
        let legacy_exists = connection
            .query_row(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'backend_models'",
                [],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        assert!(!legacy_exists);
        Ok(())
    }

    /// Seeds two logical sessions, a delegated run with one transcript turn, and two native runtime
    /// histories: one owned by a live session and one orphaned by a partially initialized session.
    fn seed_mixed_session_state(store: &SqliteSessionRepository) -> Result<(), SessionError> {
        let first = store.create(
            CODEX_PROVIDER,
            "provider-first",
            "/tmp/first",
            "First",
            None,
        )?;
        store.create(
            DEVIN_PROVIDER,
            "provider-second",
            "/tmp/second",
            "Second",
            None,
        )?;
        store.save_subagent(&SubagentRecord {
            parent_session_id: first.id,
            id: "run-1".to_owned(),
            agent: "explorer".to_owned(),
            provider: CODEX_PROVIDER.to_owned(),
            model: None,
            provider_session_id: Some("provider-child".to_owned()),
            input_tokens: 0,
            output_tokens: 0,
            cached_input_tokens: 0,
            cache_write_tokens: 0,
            objective: "Inspect".to_owned(),
            status: SubagentStatus::Interrupted,
            latest_activity: "stale".to_owned(),
            transcript: vec![TranscriptEntry {
                id: "turn-1".to_owned(),
                key: None,
                kind: EntryKind::Assistant,
                title: "ASSISTANT".to_owned(),
                body: "partial".to_owned(),
                status: EntryStatus::Running,
                created_at_ms: None,
                provider_id: None,
                model_id: None,
                owner_turn_id: None,
                reasoning_effort: None,
                fast_mode: None,
                source_transport: None,
                tool_audit_json: None,
            }],
            observability: SubagentObservability::default(),
            transcript_has_earlier: false,
        })?;
        let connection = store.connection.lock().expect("database mutex");
        for (provider, session_id) in [
            (CODEX_PROVIDER, "provider-first"),
            (DEVIN_PROVIDER, "orphaned-partial-session"),
        ] {
            connection.execute(
                "INSERT INTO native_runtime_sessions
                 (provider, session_id, session_json, updated_at)
                 VALUES (?1, ?2, '{}', unixepoch())",
                params![provider, session_id],
            )?;
        }
        Ok(())
    }

    #[test]
    fn bulk_purge_removes_all_session_state_and_preserves_global_configuration()
    -> Result<(), SessionError> {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("purge.db");
        let store = SqliteSessionRepository::open(&path)?;
        let credentials =
            crate::credential::SqliteCredentialStore::open(&path).expect("credential store");
        credentials
            .put(
                CODEX_PROVIDER,
                &Credential {
                    kind: "oauth".to_owned(),
                    secret: SecretValue::new(serde_json::json!({"access_token": "keep-me"})),
                },
            )
            .expect("save credential");
        store.set_provider_enabled(CODEX_PROVIDER, true)?;
        store.set_default_model(CODEX_PROVIDER, "model-default")?;
        store.save_web_config(&WebConfig {
            backend: WebBackend::Firecrawl,
            firecrawl_api_key: "keep-web-key".to_owned(),
        })?;

        store.save_invocation_telemetry_enabled(true)?;
        assert!(store.record_invocation(&InvocationRecord {
            invocation_key: "skill:purge".to_owned(),
            kind: nakode_protocol::InvocationKind::Skill,
            identity: "review".to_owned(),
            display_label: "Review".to_owned(),
            occurred_at_ms: 1_000,
        })?);
        seed_mixed_session_state(&store)?;

        assert_eq!(
            store.purge_all()?,
            SessionPurgeReport {
                sessions: 2,
                orchestration_runs: 1,
                agent_turns: 1,
                native_runtime_sessions: 2,
            }
        );
        assert!(store.list_recent("/tmp/first", 500)?.is_empty());
        assert!(store.list_recent("/tmp/second", 500)?.is_empty());
        assert!(store.invocation_summary()?.items.is_empty());
        assert!(store.load_invocation_telemetry_enabled()?);
        assert_eq!(store.purge_all()?, SessionPurgeReport::default());

        let kept_credential = credentials
            .get(CODEX_PROVIDER)
            .expect("load credential")
            .expect("credential survives");
        assert_eq!(kept_credential.secret.expose()["access_token"], "keep-me");
        assert!(
            store
                .list_providers()?
                .into_iter()
                .find(|provider| provider.provider == CODEX_PROVIDER)
                .expect("provider")
                .enabled
        );
        assert_eq!(
            store.load_web_config()?,
            WebConfig {
                backend: WebBackend::Firecrawl,
                firecrawl_api_key: "keep-web-key".to_owned(),
            }
        );
        Ok(())
    }

    #[test]
    fn failed_bulk_purge_rolls_back_every_session_table() -> Result<(), SessionError> {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = SqliteSessionRepository::open(directory.path().join("purge-failure.db"))?;
        let session = store.create(
            CODEX_PROVIDER,
            "provider-session",
            "/tmp/project",
            "Keep on failure",
            None,
        )?;
        {
            let connection = store.connection.lock().expect("database mutex");
            connection.execute_batch(
                "INSERT INTO native_runtime_sessions
                   (provider, session_id, session_json, updated_at)
                 VALUES ('openai-codex', 'provider-session', '{}', unixepoch());
                 CREATE TRIGGER refuse_session_purge BEFORE DELETE ON sessions
                 BEGIN SELECT RAISE(FAIL, 'injected purge failure'); END;",
            )?;
        }

        assert!(store.purge_all().is_err());
        assert!(store.find(&session.id)?.is_some());
        let connection = store.connection.lock().expect("database mutex");
        let histories =
            connection.query_row("SELECT COUNT(*) FROM native_runtime_sessions", [], |row| {
                row.get::<_, i64>(0)
            })?;
        assert_eq!(
            histories, 1,
            "the failed transaction restores runtime history"
        );
        Ok(())
    }

    #[test]
    fn legacy_single_account_migration_is_idempotent_and_preserves_session_affinity()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let database = directory.path().join("legacy.db");
        let provider = crate::backend::CODEX_PROVIDER;
        let repository = SqliteSessionRepository::open(&database)?;
        repository.create_with_id(
            "legacy-session",
            provider,
            "native-session",
            "/workspace",
            "/workspace",
            "Legacy",
            None,
            &ModelOptions::default(),
            None,
        )?;
        {
            let connection = repository.connection.lock().expect("database mutex");
            connection.execute(
                "INSERT INTO provider_credentials
                 (provider, credential_kind, credential_json, updated_at)
                 VALUES (?1, 'oauth', ?2, 42)",
                params![
                    provider,
                    serde_json::json!({"access_token": "legacy-secret"}).to_string()
                ],
            )?;
        }
        drop(repository);

        for _ in 0..2 {
            let reopened = SqliteSessionRepository::open(&database)?;
            let record = reopened.find("legacy-session")?.expect("legacy session");
            assert_eq!(record.account_id.as_deref(), Some("legacy-openai-codex"));
            let provider_record = reopened
                .list_providers()?
                .into_iter()
                .find(|candidate| candidate.provider == provider)
                .expect("provider");
            let accounts = provider_record
                .accounts
                .iter()
                .filter(|account| account.account_id == "legacy-openai-codex")
                .collect::<Vec<_>>();
            assert_eq!(accounts.len(), 1);
            assert!(accounts[0].is_default);
            assert_eq!(
                accounts[0]
                    .credential
                    .as_ref()
                    .map(|value| value.kind.as_str()),
                Some("oauth")
            );
        }
        Ok(())
    }

    #[test]
    fn provider_accounts_are_redacted_and_survive_restart() -> Result<(), Box<dyn std::error::Error>>
    {
        let directory = tempfile::tempdir()?;
        let database = directory.path().join("accounts.db");
        let provider = crate::backend::CODEX_PROVIDER;
        let repository = SqliteSessionRepository::open(&database)?;
        let account = repository.add_provider_account(provider, "Work")?;
        repository.set_provider_account_identity(
            provider,
            &account.account_id,
            Some("w***@example"),
        )?;
        let credentials = crate::credential::SqliteCredentialStore::open(&database)?;
        credentials.put_account(
            provider,
            &account.account_id,
            &Credential {
                kind: "oauth".to_owned(),
                secret: SecretValue::new(serde_json::json!({"access_token": "never-print"})),
            },
        )?;
        drop(repository);
        drop(credentials);

        let restarted = SqliteSessionRepository::open(&database)?;
        let restored = restarted
            .list_providers()?
            .into_iter()
            .find(|record| record.provider == provider)
            .and_then(|record| {
                record
                    .accounts
                    .into_iter()
                    .find(|candidate| candidate.account_id == account.account_id)
            })
            .expect("account survives restart");
        assert_eq!(restored.label, "Work");
        assert_eq!(restored.identity.as_deref(), Some("w***@example"));
        assert_eq!(
            restored
                .credential
                .as_ref()
                .map(|value| value.kind.as_str()),
            Some("oauth")
        );
        let diagnostic = format!("{restored:?}");
        assert!(!diagnostic.contains("never-print"));
        assert!(!diagnostic.contains("access_token"));
        Ok(())
    }

    #[test]
    fn session_account_affinity_is_atomic_and_cannot_be_switched() -> Result<(), SessionError> {
        let directory = tempfile::tempdir().expect("tempdir");
        let repository = SqliteSessionRepository::open(directory.path().join("sessions.db"))?;
        let provider = crate::backend::CODEX_PROVIDER;
        let first = repository.add_provider_account(provider, "First")?;
        let second = repository.add_provider_account(provider, "Second")?;
        let options = ModelOptions::default();
        let created = repository.create_with_account_id(
            "logical-session",
            provider,
            Some(&first.account_id),
            "native-session",
            "/workspace",
            "/workspace",
            "Pinned",
            Some("model"),
            &options,
            None,
        )?;
        assert_eq!(
            created.account_id.as_deref(),
            Some(first.account_id.as_str())
        );
        let error = repository
            .create_with_account_id(
                "duplicate-request",
                provider,
                Some(&second.account_id),
                "native-session",
                "/workspace",
                "/workspace",
                "Pinned",
                Some("model"),
                &options,
                None,
            )
            .expect_err("native identity cannot change account affinity");
        assert!(matches!(
            error,
            SessionError::ProviderAccountAffinityConflict { .. }
        ));
        let restored = repository
            .find("logical-session")?
            .expect("persisted session");
        assert_eq!(
            restored.account_id.as_deref(),
            Some(first.account_id.as_str())
        );
        assert!(matches!(
            repository.remove_provider_account(provider, &first.account_id),
            Err(SessionError::ProviderAccountInUse { .. })
        ));
        Ok(())
    }

    #[test]
    fn cross_provider_transition_rebinds_account_affinity() -> Result<(), SessionError> {
        let directory = tempfile::tempdir().expect("tempdir");
        let repository = SqliteSessionRepository::open(directory.path().join("sessions.db"))?;
        let codex = repository.add_provider_account(CODEX_PROVIDER, "Codex")?;
        let devin = repository.add_provider_account(DEVIN_PROVIDER, "Devin")?;
        repository.create_with_account_id(
            "logical-cross-provider",
            CODEX_PROVIDER,
            Some(&codex.account_id),
            "codex-native",
            "/workspace",
            "/workspace",
            "Cross provider",
            Some("codex-model"),
            &ModelOptions::default(),
            None,
        )?;

        repository.transition_primary_with_account(
            "logical-cross-provider",
            DEVIN_PROVIDER,
            Some(&devin.account_id),
            "devin-native",
            Some("devin-model"),
            &ModelOptions::default(),
        )?;

        let restored = repository
            .find("logical-cross-provider")?
            .expect("persisted session");
        assert_eq!(restored.provider, DEVIN_PROVIDER);
        assert_eq!(
            restored.account_id.as_deref(),
            Some(devin.account_id.as_str())
        );
        assert_eq!(restored.provider_session_id, "devin-native");
        assert!(
            restored
                .owned_provider_sessions
                .contains(&(CODEX_PROVIDER.to_owned(), "codex-native".to_owned()))
        );
        Ok(())
    }

    #[test]
    fn account_profile_and_code_mode_creation_is_atomic() -> Result<(), SessionError> {
        let directory = tempfile::tempdir().expect("tempdir");
        let repository = SqliteSessionRepository::open(directory.path().join("sessions.db"))?;
        let provider = crate::backend::CODEX_PROVIDER;
        let account = repository.add_provider_account(provider, "Primary")?;
        let options = ModelOptions::default();
        let created = repository.create_with_account_id_and_skill_profile(
            "logical-session",
            provider,
            Some(&account.account_id),
            "native-session",
            "/workspace",
            "/workspace",
            "Pinned",
            Some("model"),
            &options,
            None,
            Some("profile-a"),
            Some(true),
            None,
        )?;
        assert_eq!(
            created.account_id.as_deref(),
            Some(account.account_id.as_str())
        );
        assert!(created.code_mode);
        assert_eq!(
            repository.session_skill_profile(&created.id)?.as_deref(),
            Some("profile-a")
        );

        let error = repository
            .create_with_account_id_and_skill_profile(
                "duplicate-request",
                provider,
                Some(&account.account_id),
                "native-session",
                "/workspace",
                "/workspace",
                "Changed",
                Some("other-model"),
                &options,
                None,
                Some("profile-b"),
                Some(false),
                None,
            )
            .expect_err("a conflicting profile must roll back the whole creation transaction");
        assert!(matches!(error, SessionError::InvalidStoredValue { .. }));
        let restored = repository
            .find("logical-session")?
            .expect("persisted session");
        assert_eq!(restored.title, "Pinned");
        assert_eq!(restored.model.as_deref(), Some("model"));
        assert!(restored.code_mode);
        assert_eq!(
            repository.session_skill_profile(&restored.id)?.as_deref(),
            Some("profile-a")
        );
        Ok(())
    }

    #[test]
    fn first_owner_prompt_insert_failure_rolls_back_session_profile_and_owner()
    -> Result<(), SessionError> {
        let directory = tempfile::tempdir().expect("tempdir");
        let repository = SqliteSessionRepository::open(directory.path().join("sessions.db"))?;
        {
            let connection = repository
                .connection
                .lock()
                .expect("session database mutex poisoned");
            connection.execute_batch(
                "CREATE TRIGGER fail_first_owner_prompt
                 BEFORE INSERT ON accepted_owner_prompts
                 WHEN NEW.session_id = 'rollback-session'
                 BEGIN SELECT RAISE(ABORT, 'forced first owner failure'); END;",
            )?;
        }
        let prompt = PersistedOwnerPrompt {
            prompt_id: "first-prompt".to_owned(),
            raw_text: "exact first owner text".to_owned(),
            source_transport: None,
            dispatch_pending: true,
        };
        let error = repository
            .create_with_account_id_and_skill_profile(
                "rollback-session",
                crate::backend::CODEX_PROVIDER,
                None,
                "rollback-native-session",
                "/workspace",
                "/workspace",
                "Rollback",
                Some("model"),
                &ModelOptions::default(),
                None,
                Some("profile-a"),
                Some(false),
                Some(&prompt),
            )
            .expect_err("owner insert failure must abort the session creation transaction");
        assert!(matches!(error, SessionError::Database(_)));

        let connection = repository
            .connection
            .lock()
            .expect("session database mutex poisoned");
        for (table, column) in [
            ("sessions", "id"),
            ("session_skill_profiles", "session_id"),
            ("accepted_owner_prompts", "session_id"),
        ] {
            let count = connection.query_row(
                &format!("SELECT COUNT(*) FROM {table} WHERE {column} = ?1"),
                params!["rollback-session"],
                |row| row.get::<_, i64>(0),
            )?;
            assert_eq!(count, 0, "{table} must roll back with the owner prompt");
        }
        Ok(())
    }

    #[test]
    fn concurrent_first_owner_creation_is_idempotent_across_repositories()
    -> Result<(), SessionError> {
        let directory = tempfile::tempdir().expect("tempdir");
        let database = directory.path().join("concurrent-first-owner.db");
        let bootstrap = SqliteSessionRepository::open(&database)?;
        let provider = crate::backend::CODEX_PROVIDER;
        let account = bootstrap.add_provider_account(provider, "Primary")?;
        drop(bootstrap);
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let mut threads = Vec::new();
        for _ in 0..2 {
            let database = database.clone();
            let account_id = account.account_id.clone();
            let barrier = std::sync::Arc::clone(&barrier);
            threads.push(std::thread::spawn(move || {
                let repository =
                    SqliteSessionRepository::open(database).expect("concurrent repository");
                let prompt = PersistedOwnerPrompt {
                    prompt_id: "stable-first-prompt".to_owned(),
                    raw_text: "exact first owner text".to_owned(),
                    source_transport: Some("slack".to_owned()),
                    dispatch_pending: false,
                };
                barrier.wait();
                repository.create_with_account_id_and_skill_profile(
                    "logical-session",
                    provider,
                    Some(&account_id),
                    &pending_provider_session_id("logical-session"),
                    "/workspace",
                    "/workspace",
                    "First owner",
                    Some("model"),
                    &ModelOptions::default(),
                    None,
                    Some("profile-a"),
                    Some(false),
                    Some(&prompt),
                )
            }));
        }
        for thread in threads {
            let created = thread.join().expect("creation thread")?;
            assert_eq!(created.id, "logical-session");
            assert_eq!(
                created.account_id.as_deref(),
                Some(account.account_id.as_str())
            );
        }

        let repository = SqliteSessionRepository::open(&database)?;
        let restored = repository
            .find("logical-session")?
            .expect("single logical session");
        assert_eq!(restored.owner_prompts.len(), 1);
        assert_eq!(restored.owner_prompts[0].prompt_id, "stable-first-prompt");
        assert_eq!(restored.owner_prompts[0].raw_text, "exact first owner text");
        assert_eq!(
            restored.owner_prompts[0].source_transport.as_deref(),
            Some("slack")
        );
        Ok(())
    }

    #[test]
    fn concurrent_account_binding_accepts_only_one_affinity() -> Result<(), SessionError> {
        let directory = tempfile::tempdir().expect("tempdir");
        let database = directory.path().join("concurrent-affinity.db");
        let repository = SqliteSessionRepository::open(&database)?;
        let provider = crate::backend::CODEX_PROVIDER;
        let first = repository.add_provider_account(provider, "First")?;
        let second = repository.add_provider_account(provider, "Second")?;
        repository.create_with_account_id(
            "logical-session",
            provider,
            None,
            "native-session",
            "/workspace",
            "/workspace",
            "Concurrent",
            None,
            &ModelOptions::default(),
            None,
        )?;
        drop(repository);
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(3));
        let handles = [first.account_id, second.account_id]
            .into_iter()
            .map(|account_id| {
                let database = database.clone();
                let barrier = std::sync::Arc::clone(&barrier);
                std::thread::spawn(move || {
                    let repository = SqliteSessionRepository::open(database).expect("repository");
                    barrier.wait();
                    repository.set_session_account("logical-session", Some(&account_id))
                })
            })
            .collect::<Vec<_>>();
        barrier.wait();
        let results = handles
            .into_iter()
            .map(|handle| handle.join().expect("binding thread"))
            .collect::<Vec<_>>();
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(
                    result,
                    Err(SessionError::ProviderAccountAffinityConflict { .. })
                ))
                .count(),
            1
        );
        Ok(())
    }

    #[test]
    fn concurrent_repository_initialization_waits_for_schema_lock() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("concurrent.db");
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(3));
        let handles = (0..2)
            .map(|_| {
                let path = path.clone();
                let barrier = std::sync::Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    SqliteSessionRepository::open(path).expect("concurrent repository open");
                })
            })
            .collect::<Vec<_>>();

        barrier.wait();
        for handle in handles {
            handle.join().expect("repository thread");
        }
    }
}
