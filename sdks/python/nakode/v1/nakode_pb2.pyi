from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class InteractionResolutionKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    INTERACTION_RESOLUTION_KIND_UNSPECIFIED: _ClassVar[InteractionResolutionKind]
    INTERACTION_RESOLUTION_KIND_APPROVE_ONCE: _ClassVar[InteractionResolutionKind]
    INTERACTION_RESOLUTION_KIND_APPROVE_FOR_SESSION: _ClassVar[InteractionResolutionKind]
    INTERACTION_RESOLUTION_KIND_DECLINE: _ClassVar[InteractionResolutionKind]
    INTERACTION_RESOLUTION_KIND_ANSWER: _ClassVar[InteractionResolutionKind]

class TranscriptOwnerKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    TRANSCRIPT_OWNER_KIND_UNSPECIFIED: _ClassVar[TranscriptOwnerKind]
    TRANSCRIPT_OWNER_KIND_SESSION: _ClassVar[TranscriptOwnerKind]
    TRANSCRIPT_OWNER_KIND_RUN: _ClassVar[TranscriptOwnerKind]

class RunTextField(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    RUN_TEXT_FIELD_UNSPECIFIED: _ClassVar[RunTextField]
    RUN_TEXT_FIELD_OBJECTIVE: _ClassVar[RunTextField]
    RUN_TEXT_FIELD_LATEST_ACTIVITY: _ClassVar[RunTextField]
    RUN_TEXT_FIELD_OUTCOME: _ClassVar[RunTextField]
    RUN_TEXT_FIELD_RESULT: _ClassVar[RunTextField]

class ProviderCapability(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PROVIDER_CAPABILITY_UNSPECIFIED: _ClassVar[ProviderCapability]
    PROVIDER_CAPABILITY_RESUME: _ClassVar[ProviderCapability]
    PROVIDER_CAPABILITY_STEERING: _ClassVar[ProviderCapability]
    PROVIDER_CAPABILITY_INTERRUPTION: _ClassVar[ProviderCapability]
    PROVIDER_CAPABILITY_MODEL_CATALOG: _ClassVar[ProviderCapability]
    PROVIDER_CAPABILITY_MODELS_REQUIRE_SESSION: _ClassVar[ProviderCapability]
    PROVIDER_CAPABILITY_SESSION_MODEL_CONFIGURATION: _ClassVar[ProviderCapability]
    PROVIDER_CAPABILITY_CONTEXT_COMPACTION: _ClassVar[ProviderCapability]
    PROVIDER_CAPABILITY_APPROVALS: _ClassVar[ProviderCapability]
    PROVIDER_CAPABILITY_NATIVE_TOOLS: _ClassVar[ProviderCapability]
    PROVIDER_CAPABILITY_MCP: _ClassVar[ProviderCapability]
    PROVIDER_CAPABILITY_CLOSE_SESSION: _ClassVar[ProviderCapability]

class ConnectionState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CONNECTION_STATE_UNSPECIFIED: _ClassVar[ConnectionState]
    CONNECTION_STATE_DISABLED: _ClassVar[ConnectionState]
    CONNECTION_STATE_STARTING: _ClassVar[ConnectionState]
    CONNECTION_STATE_READY: _ClassVar[ConnectionState]
    CONNECTION_STATE_FAILED: _ClassVar[ConnectionState]
    CONNECTION_STATE_DISCONNECTED: _ClassVar[ConnectionState]

class TerminalImageMode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    TERMINAL_IMAGE_MODE_UNSPECIFIED: _ClassVar[TerminalImageMode]
    TERMINAL_IMAGE_MODE_AUTO: _ClassVar[TerminalImageMode]
    TERMINAL_IMAGE_MODE_ON: _ClassVar[TerminalImageMode]
    TERMINAL_IMAGE_MODE_OFF: _ClassVar[TerminalImageMode]

class SessionActivity(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    SESSION_ACTIVITY_UNSPECIFIED: _ClassVar[SessionActivity]
    SESSION_ACTIVITY_IDLE: _ClassVar[SessionActivity]
    SESSION_ACTIVITY_CREATING_AGENT_SESSION: _ClassVar[SessionActivity]
    SESSION_ACTIVITY_STARTING_TURN: _ClassVar[SessionActivity]
    SESSION_ACTIVITY_RUNNING_TURN: _ClassVar[SessionActivity]
    SESSION_ACTIVITY_COMPACTING_CONTEXT: _ClassVar[SessionActivity]
    SESSION_ACTIVITY_RUNNING_DELEGATES: _ClassVar[SessionActivity]
    SESSION_ACTIVITY_RUNNING_SHELL: _ClassVar[SessionActivity]

class TurnStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    TURN_STATUS_UNSPECIFIED: _ClassVar[TurnStatus]
    TURN_STATUS_STARTING: _ClassVar[TurnStatus]
    TURN_STATUS_RUNNING: _ClassVar[TurnStatus]
    TURN_STATUS_CANCELLING: _ClassVar[TurnStatus]
    TURN_STATUS_COMPLETED: _ClassVar[TurnStatus]
    TURN_STATUS_INTERRUPTED: _ClassVar[TurnStatus]
    TURN_STATUS_FAILED: _ClassVar[TurnStatus]

class TranscriptEntryKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    TRANSCRIPT_ENTRY_KIND_UNSPECIFIED: _ClassVar[TranscriptEntryKind]
    TRANSCRIPT_ENTRY_KIND_SYSTEM: _ClassVar[TranscriptEntryKind]
    TRANSCRIPT_ENTRY_KIND_USER: _ClassVar[TranscriptEntryKind]
    TRANSCRIPT_ENTRY_KIND_ASSISTANT: _ClassVar[TranscriptEntryKind]
    TRANSCRIPT_ENTRY_KIND_STEERING: _ClassVar[TranscriptEntryKind]
    TRANSCRIPT_ENTRY_KIND_REASONING: _ClassVar[TranscriptEntryKind]
    TRANSCRIPT_ENTRY_KIND_TOOL: _ClassVar[TranscriptEntryKind]
    TRANSCRIPT_ENTRY_KIND_DIFF: _ClassVar[TranscriptEntryKind]
    TRANSCRIPT_ENTRY_KIND_WARNING: _ClassVar[TranscriptEntryKind]
    TRANSCRIPT_ENTRY_KIND_ERROR: _ClassVar[TranscriptEntryKind]

class TranscriptEntryStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    TRANSCRIPT_ENTRY_STATUS_UNSPECIFIED: _ClassVar[TranscriptEntryStatus]
    TRANSCRIPT_ENTRY_STATUS_RUNNING: _ClassVar[TranscriptEntryStatus]
    TRANSCRIPT_ENTRY_STATUS_COMPLETE: _ClassVar[TranscriptEntryStatus]
    TRANSCRIPT_ENTRY_STATUS_FAILED: _ClassVar[TranscriptEntryStatus]
    TRANSCRIPT_ENTRY_STATUS_INTERRUPTED: _ClassVar[TranscriptEntryStatus]

class InteractionKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    INTERACTION_KIND_UNSPECIFIED: _ClassVar[InteractionKind]
    INTERACTION_KIND_APPROVAL: _ClassVar[InteractionKind]
    INTERACTION_KIND_QUESTION: _ClassVar[InteractionKind]

class InteractionStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    INTERACTION_STATUS_UNSPECIFIED: _ClassVar[InteractionStatus]
    INTERACTION_STATUS_PENDING: _ClassVar[InteractionStatus]
    INTERACTION_STATUS_RESOLVED: _ClassVar[InteractionStatus]
    INTERACTION_STATUS_DECLINED: _ClassVar[InteractionStatus]
    INTERACTION_STATUS_CANCELLED: _ClassVar[InteractionStatus]

class TodoStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    TODO_STATUS_UNSPECIFIED: _ClassVar[TodoStatus]
    TODO_STATUS_PENDING: _ClassVar[TodoStatus]
    TODO_STATUS_IN_PROGRESS: _ClassVar[TodoStatus]
    TODO_STATUS_COMPLETED: _ClassVar[TodoStatus]
    TODO_STATUS_ABANDONED: _ClassVar[TodoStatus]

class NoticeLevel(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    NOTICE_LEVEL_UNSPECIFIED: _ClassVar[NoticeLevel]
    NOTICE_LEVEL_INFO: _ClassVar[NoticeLevel]
    NOTICE_LEVEL_WARNING: _ClassVar[NoticeLevel]
    NOTICE_LEVEL_ERROR: _ClassVar[NoticeLevel]

class RunStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    RUN_STATUS_UNSPECIFIED: _ClassVar[RunStatus]
    RUN_STATUS_STARTING: _ClassVar[RunStatus]
    RUN_STATUS_WORKING: _ClassVar[RunStatus]
    RUN_STATUS_COMPLETED: _ClassVar[RunStatus]
    RUN_STATUS_INTERRUPTED: _ClassVar[RunStatus]
    RUN_STATUS_FAILED: _ClassVar[RunStatus]
INTERACTION_RESOLUTION_KIND_UNSPECIFIED: InteractionResolutionKind
INTERACTION_RESOLUTION_KIND_APPROVE_ONCE: InteractionResolutionKind
INTERACTION_RESOLUTION_KIND_APPROVE_FOR_SESSION: InteractionResolutionKind
INTERACTION_RESOLUTION_KIND_DECLINE: InteractionResolutionKind
INTERACTION_RESOLUTION_KIND_ANSWER: InteractionResolutionKind
TRANSCRIPT_OWNER_KIND_UNSPECIFIED: TranscriptOwnerKind
TRANSCRIPT_OWNER_KIND_SESSION: TranscriptOwnerKind
TRANSCRIPT_OWNER_KIND_RUN: TranscriptOwnerKind
RUN_TEXT_FIELD_UNSPECIFIED: RunTextField
RUN_TEXT_FIELD_OBJECTIVE: RunTextField
RUN_TEXT_FIELD_LATEST_ACTIVITY: RunTextField
RUN_TEXT_FIELD_OUTCOME: RunTextField
RUN_TEXT_FIELD_RESULT: RunTextField
PROVIDER_CAPABILITY_UNSPECIFIED: ProviderCapability
PROVIDER_CAPABILITY_RESUME: ProviderCapability
PROVIDER_CAPABILITY_STEERING: ProviderCapability
PROVIDER_CAPABILITY_INTERRUPTION: ProviderCapability
PROVIDER_CAPABILITY_MODEL_CATALOG: ProviderCapability
PROVIDER_CAPABILITY_MODELS_REQUIRE_SESSION: ProviderCapability
PROVIDER_CAPABILITY_SESSION_MODEL_CONFIGURATION: ProviderCapability
PROVIDER_CAPABILITY_CONTEXT_COMPACTION: ProviderCapability
PROVIDER_CAPABILITY_APPROVALS: ProviderCapability
PROVIDER_CAPABILITY_NATIVE_TOOLS: ProviderCapability
PROVIDER_CAPABILITY_MCP: ProviderCapability
PROVIDER_CAPABILITY_CLOSE_SESSION: ProviderCapability
CONNECTION_STATE_UNSPECIFIED: ConnectionState
CONNECTION_STATE_DISABLED: ConnectionState
CONNECTION_STATE_STARTING: ConnectionState
CONNECTION_STATE_READY: ConnectionState
CONNECTION_STATE_FAILED: ConnectionState
CONNECTION_STATE_DISCONNECTED: ConnectionState
TERMINAL_IMAGE_MODE_UNSPECIFIED: TerminalImageMode
TERMINAL_IMAGE_MODE_AUTO: TerminalImageMode
TERMINAL_IMAGE_MODE_ON: TerminalImageMode
TERMINAL_IMAGE_MODE_OFF: TerminalImageMode
SESSION_ACTIVITY_UNSPECIFIED: SessionActivity
SESSION_ACTIVITY_IDLE: SessionActivity
SESSION_ACTIVITY_CREATING_AGENT_SESSION: SessionActivity
SESSION_ACTIVITY_STARTING_TURN: SessionActivity
SESSION_ACTIVITY_RUNNING_TURN: SessionActivity
SESSION_ACTIVITY_COMPACTING_CONTEXT: SessionActivity
SESSION_ACTIVITY_RUNNING_DELEGATES: SessionActivity
SESSION_ACTIVITY_RUNNING_SHELL: SessionActivity
TURN_STATUS_UNSPECIFIED: TurnStatus
TURN_STATUS_STARTING: TurnStatus
TURN_STATUS_RUNNING: TurnStatus
TURN_STATUS_CANCELLING: TurnStatus
TURN_STATUS_COMPLETED: TurnStatus
TURN_STATUS_INTERRUPTED: TurnStatus
TURN_STATUS_FAILED: TurnStatus
TRANSCRIPT_ENTRY_KIND_UNSPECIFIED: TranscriptEntryKind
TRANSCRIPT_ENTRY_KIND_SYSTEM: TranscriptEntryKind
TRANSCRIPT_ENTRY_KIND_USER: TranscriptEntryKind
TRANSCRIPT_ENTRY_KIND_ASSISTANT: TranscriptEntryKind
TRANSCRIPT_ENTRY_KIND_STEERING: TranscriptEntryKind
TRANSCRIPT_ENTRY_KIND_REASONING: TranscriptEntryKind
TRANSCRIPT_ENTRY_KIND_TOOL: TranscriptEntryKind
TRANSCRIPT_ENTRY_KIND_DIFF: TranscriptEntryKind
TRANSCRIPT_ENTRY_KIND_WARNING: TranscriptEntryKind
TRANSCRIPT_ENTRY_KIND_ERROR: TranscriptEntryKind
TRANSCRIPT_ENTRY_STATUS_UNSPECIFIED: TranscriptEntryStatus
TRANSCRIPT_ENTRY_STATUS_RUNNING: TranscriptEntryStatus
TRANSCRIPT_ENTRY_STATUS_COMPLETE: TranscriptEntryStatus
TRANSCRIPT_ENTRY_STATUS_FAILED: TranscriptEntryStatus
TRANSCRIPT_ENTRY_STATUS_INTERRUPTED: TranscriptEntryStatus
INTERACTION_KIND_UNSPECIFIED: InteractionKind
INTERACTION_KIND_APPROVAL: InteractionKind
INTERACTION_KIND_QUESTION: InteractionKind
INTERACTION_STATUS_UNSPECIFIED: InteractionStatus
INTERACTION_STATUS_PENDING: InteractionStatus
INTERACTION_STATUS_RESOLVED: InteractionStatus
INTERACTION_STATUS_DECLINED: InteractionStatus
INTERACTION_STATUS_CANCELLED: InteractionStatus
TODO_STATUS_UNSPECIFIED: TodoStatus
TODO_STATUS_PENDING: TodoStatus
TODO_STATUS_IN_PROGRESS: TodoStatus
TODO_STATUS_COMPLETED: TodoStatus
TODO_STATUS_ABANDONED: TodoStatus
NOTICE_LEVEL_UNSPECIFIED: NoticeLevel
NOTICE_LEVEL_INFO: NoticeLevel
NOTICE_LEVEL_WARNING: NoticeLevel
NOTICE_LEVEL_ERROR: NoticeLevel
RUN_STATUS_UNSPECIFIED: RunStatus
RUN_STATUS_STARTING: RunStatus
RUN_STATUS_WORKING: RunStatus
RUN_STATUS_COMPLETED: RunStatus
RUN_STATUS_INTERRUPTED: RunStatus
RUN_STATUS_FAILED: RunStatus

class MutationOptions(_message.Message):
    __slots__ = ("idempotency_key", "expected_revision")
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_REVISION_FIELD_NUMBER: _ClassVar[int]
    idempotency_key: str
    expected_revision: int
    def __init__(self, idempotency_key: _Optional[str] = ..., expected_revision: _Optional[int] = ...) -> None: ...

class MutationResult(_message.Message):
    __slots__ = ("resource_id", "revision")
    RESOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    REVISION_FIELD_NUMBER: _ClassVar[int]
    resource_id: str
    revision: int
    def __init__(self, resource_id: _Optional[str] = ..., revision: _Optional[int] = ...) -> None: ...

class Cursor(_message.Message):
    __slots__ = ("server_epoch", "sequence")
    SERVER_EPOCH_FIELD_NUMBER: _ClassVar[int]
    SEQUENCE_FIELD_NUMBER: _ClassVar[int]
    server_epoch: str
    sequence: int
    def __init__(self, server_epoch: _Optional[str] = ..., sequence: _Optional[int] = ...) -> None: ...

class ServerInfo(_message.Message):
    __slots__ = ("server_version", "api_version", "capabilities")
    SERVER_VERSION_FIELD_NUMBER: _ClassVar[int]
    API_VERSION_FIELD_NUMBER: _ClassVar[int]
    CAPABILITIES_FIELD_NUMBER: _ClassVar[int]
    server_version: str
    api_version: str
    capabilities: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, server_version: _Optional[str] = ..., api_version: _Optional[str] = ..., capabilities: _Optional[_Iterable[str]] = ...) -> None: ...

class GetWorkspaceRequest(_message.Message):
    __slots__ = ("workspace", "session_id")
    WORKSPACE_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    workspace: str
    session_id: str
    def __init__(self, workspace: _Optional[str] = ..., session_id: _Optional[str] = ...) -> None: ...

class WatchWorkspaceRequest(_message.Message):
    __slots__ = ("workspace_id", "after")
    WORKSPACE_ID_FIELD_NUMBER: _ClassVar[int]
    AFTER_FIELD_NUMBER: _ClassVar[int]
    workspace_id: str
    after: Cursor
    def __init__(self, workspace_id: _Optional[str] = ..., after: _Optional[_Union[Cursor, _Mapping]] = ...) -> None: ...

class ReloadWorkspaceRequest(_message.Message):
    __slots__ = ("mutation", "workspace_id", "session_id")
    MUTATION_FIELD_NUMBER: _ClassVar[int]
    WORKSPACE_ID_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    mutation: MutationOptions
    workspace_id: str
    session_id: str
    def __init__(self, mutation: _Optional[_Union[MutationOptions, _Mapping]] = ..., workspace_id: _Optional[str] = ..., session_id: _Optional[str] = ...) -> None: ...

class WorkspaceSnapshot(_message.Message):
    __slots__ = ("cursor", "state")
    CURSOR_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    cursor: Cursor
    state: WorkspaceState
    def __init__(self, cursor: _Optional[_Union[Cursor, _Mapping]] = ..., state: _Optional[_Union[WorkspaceState, _Mapping]] = ...) -> None: ...

class CreateSessionRequest(_message.Message):
    __slots__ = ("mutation", "workspace_id", "title")
    MUTATION_FIELD_NUMBER: _ClassVar[int]
    WORKSPACE_ID_FIELD_NUMBER: _ClassVar[int]
    TITLE_FIELD_NUMBER: _ClassVar[int]
    mutation: MutationOptions
    workspace_id: str
    title: str
    def __init__(self, mutation: _Optional[_Union[MutationOptions, _Mapping]] = ..., workspace_id: _Optional[str] = ..., title: _Optional[str] = ...) -> None: ...

class OpenSessionRequest(_message.Message):
    __slots__ = ("mutation", "session_id")
    MUTATION_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    mutation: MutationOptions
    session_id: str
    def __init__(self, mutation: _Optional[_Union[MutationOptions, _Mapping]] = ..., session_id: _Optional[str] = ...) -> None: ...

class ListSessionsRequest(_message.Message):
    __slots__ = ("workspace_id", "limit")
    WORKSPACE_ID_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    workspace_id: str
    limit: int
    def __init__(self, workspace_id: _Optional[str] = ..., limit: _Optional[int] = ...) -> None: ...

class ListSessionsResponse(_message.Message):
    __slots__ = ("sessions",)
    SESSIONS_FIELD_NUMBER: _ClassVar[int]
    sessions: _containers.RepeatedCompositeFieldContainer[SessionSummary]
    def __init__(self, sessions: _Optional[_Iterable[_Union[SessionSummary, _Mapping]]] = ...) -> None: ...

class GetSessionRequest(_message.Message):
    __slots__ = ("session_id",)
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    session_id: str
    def __init__(self, session_id: _Optional[str] = ...) -> None: ...

class WatchSessionRequest(_message.Message):
    __slots__ = ("session_id", "after")
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    AFTER_FIELD_NUMBER: _ClassVar[int]
    session_id: str
    after: Cursor
    def __init__(self, session_id: _Optional[str] = ..., after: _Optional[_Union[Cursor, _Mapping]] = ...) -> None: ...

class SessionSnapshot(_message.Message):
    __slots__ = ("cursor", "state")
    CURSOR_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    cursor: Cursor
    state: SessionState
    def __init__(self, cursor: _Optional[_Union[Cursor, _Mapping]] = ..., state: _Optional[_Union[SessionState, _Mapping]] = ...) -> None: ...

class PromptInput(_message.Message):
    __slots__ = ("text", "attachments")
    TEXT_FIELD_NUMBER: _ClassVar[int]
    ATTACHMENTS_FIELD_NUMBER: _ClassVar[int]
    text: str
    attachments: _containers.RepeatedCompositeFieldContainer[PromptAttachment]
    def __init__(self, text: _Optional[str] = ..., attachments: _Optional[_Iterable[_Union[PromptAttachment, _Mapping]]] = ...) -> None: ...

class PromptAttachment(_message.Message):
    __slots__ = ("label", "artifact_id", "local_file", "inline_image")
    LABEL_FIELD_NUMBER: _ClassVar[int]
    ARTIFACT_ID_FIELD_NUMBER: _ClassVar[int]
    LOCAL_FILE_FIELD_NUMBER: _ClassVar[int]
    INLINE_IMAGE_FIELD_NUMBER: _ClassVar[int]
    label: str
    artifact_id: str
    local_file: str
    inline_image: InlineImage
    def __init__(self, label: _Optional[str] = ..., artifact_id: _Optional[str] = ..., local_file: _Optional[str] = ..., inline_image: _Optional[_Union[InlineImage, _Mapping]] = ...) -> None: ...

class InlineImage(_message.Message):
    __slots__ = ("media_type", "data")
    MEDIA_TYPE_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    media_type: str
    data: bytes
    def __init__(self, media_type: _Optional[str] = ..., data: _Optional[bytes] = ...) -> None: ...

class SendPromptRequest(_message.Message):
    __slots__ = ("mutation", "session_id", "prompt")
    MUTATION_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    PROMPT_FIELD_NUMBER: _ClassVar[int]
    mutation: MutationOptions
    session_id: str
    prompt: PromptInput
    def __init__(self, mutation: _Optional[_Union[MutationOptions, _Mapping]] = ..., session_id: _Optional[str] = ..., prompt: _Optional[_Union[PromptInput, _Mapping]] = ...) -> None: ...

class EnqueuePromptRequest(_message.Message):
    __slots__ = ("mutation", "session_id", "prompt")
    MUTATION_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    PROMPT_FIELD_NUMBER: _ClassVar[int]
    mutation: MutationOptions
    session_id: str
    prompt: PromptInput
    def __init__(self, mutation: _Optional[_Union[MutationOptions, _Mapping]] = ..., session_id: _Optional[str] = ..., prompt: _Optional[_Union[PromptInput, _Mapping]] = ...) -> None: ...

class RemoveQueuedPromptRequest(_message.Message):
    __slots__ = ("mutation", "session_id", "prompt_id")
    MUTATION_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    PROMPT_ID_FIELD_NUMBER: _ClassVar[int]
    mutation: MutationOptions
    session_id: str
    prompt_id: str
    def __init__(self, mutation: _Optional[_Union[MutationOptions, _Mapping]] = ..., session_id: _Optional[str] = ..., prompt_id: _Optional[str] = ...) -> None: ...

class SteerTurnRequest(_message.Message):
    __slots__ = ("mutation", "turn_id", "text")
    MUTATION_FIELD_NUMBER: _ClassVar[int]
    TURN_ID_FIELD_NUMBER: _ClassVar[int]
    TEXT_FIELD_NUMBER: _ClassVar[int]
    mutation: MutationOptions
    turn_id: str
    text: str
    def __init__(self, mutation: _Optional[_Union[MutationOptions, _Mapping]] = ..., turn_id: _Optional[str] = ..., text: _Optional[str] = ...) -> None: ...

class CancelTurnRequest(_message.Message):
    __slots__ = ("mutation", "turn_id")
    MUTATION_FIELD_NUMBER: _ClassVar[int]
    TURN_ID_FIELD_NUMBER: _ClassVar[int]
    mutation: MutationOptions
    turn_id: str
    def __init__(self, mutation: _Optional[_Union[MutationOptions, _Mapping]] = ..., turn_id: _Optional[str] = ...) -> None: ...

class CancelSessionWorkRequest(_message.Message):
    __slots__ = ("mutation", "session_id")
    MUTATION_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    mutation: MutationOptions
    session_id: str
    def __init__(self, mutation: _Optional[_Union[MutationOptions, _Mapping]] = ..., session_id: _Optional[str] = ...) -> None: ...

class CompactContextRequest(_message.Message):
    __slots__ = ("mutation", "agent_session_id")
    MUTATION_FIELD_NUMBER: _ClassVar[int]
    AGENT_SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    mutation: MutationOptions
    agent_session_id: str
    def __init__(self, mutation: _Optional[_Union[MutationOptions, _Mapping]] = ..., agent_session_id: _Optional[str] = ...) -> None: ...

class RunShellRequest(_message.Message):
    __slots__ = ("mutation", "session_id", "command")
    MUTATION_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    COMMAND_FIELD_NUMBER: _ClassVar[int]
    mutation: MutationOptions
    session_id: str
    command: str
    def __init__(self, mutation: _Optional[_Union[MutationOptions, _Mapping]] = ..., session_id: _Optional[str] = ..., command: _Optional[str] = ...) -> None: ...

class ResolveInteractionRequest(_message.Message):
    __slots__ = ("mutation", "interaction_id", "resolution", "option_ids")
    MUTATION_FIELD_NUMBER: _ClassVar[int]
    INTERACTION_ID_FIELD_NUMBER: _ClassVar[int]
    RESOLUTION_FIELD_NUMBER: _ClassVar[int]
    OPTION_IDS_FIELD_NUMBER: _ClassVar[int]
    mutation: MutationOptions
    interaction_id: str
    resolution: InteractionResolutionKind
    option_ids: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, mutation: _Optional[_Union[MutationOptions, _Mapping]] = ..., interaction_id: _Optional[str] = ..., resolution: _Optional[_Union[InteractionResolutionKind, str]] = ..., option_ids: _Optional[_Iterable[str]] = ...) -> None: ...

class ModelOptions(_message.Message):
    __slots__ = ("reasoning_effort", "fast_mode")
    REASONING_EFFORT_FIELD_NUMBER: _ClassVar[int]
    FAST_MODE_FIELD_NUMBER: _ClassVar[int]
    reasoning_effort: str
    fast_mode: bool
    def __init__(self, reasoning_effort: _Optional[str] = ..., fast_mode: _Optional[bool] = ...) -> None: ...

class ModelTarget(_message.Message):
    __slots__ = ("provider_default", "session_id", "agent_session_id", "vision")
    PROVIDER_DEFAULT_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    AGENT_SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    VISION_FIELD_NUMBER: _ClassVar[int]
    provider_default: str
    session_id: str
    agent_session_id: str
    vision: bool
    def __init__(self, provider_default: _Optional[str] = ..., session_id: _Optional[str] = ..., agent_session_id: _Optional[str] = ..., vision: _Optional[bool] = ...) -> None: ...

class SelectModelRequest(_message.Message):
    __slots__ = ("mutation", "target", "model_id", "options")
    MUTATION_FIELD_NUMBER: _ClassVar[int]
    TARGET_FIELD_NUMBER: _ClassVar[int]
    MODEL_ID_FIELD_NUMBER: _ClassVar[int]
    OPTIONS_FIELD_NUMBER: _ClassVar[int]
    mutation: MutationOptions
    target: ModelTarget
    model_id: str
    options: ModelOptions
    def __init__(self, mutation: _Optional[_Union[MutationOptions, _Mapping]] = ..., target: _Optional[_Union[ModelTarget, _Mapping]] = ..., model_id: _Optional[str] = ..., options: _Optional[_Union[ModelOptions, _Mapping]] = ...) -> None: ...

class SetProviderEnabledRequest(_message.Message):
    __slots__ = ("mutation", "provider_id", "enabled")
    MUTATION_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_ID_FIELD_NUMBER: _ClassVar[int]
    ENABLED_FIELD_NUMBER: _ClassVar[int]
    mutation: MutationOptions
    provider_id: str
    enabled: bool
    def __init__(self, mutation: _Optional[_Union[MutationOptions, _Mapping]] = ..., provider_id: _Optional[str] = ..., enabled: _Optional[bool] = ...) -> None: ...

class BeginProviderAuthenticationRequest(_message.Message):
    __slots__ = ("mutation", "provider_id")
    MUTATION_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_ID_FIELD_NUMBER: _ClassVar[int]
    mutation: MutationOptions
    provider_id: str
    def __init__(self, mutation: _Optional[_Union[MutationOptions, _Mapping]] = ..., provider_id: _Optional[str] = ...) -> None: ...

class SetProviderCredentialRequest(_message.Message):
    __slots__ = ("mutation", "provider_id", "kind", "credential")
    MUTATION_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_ID_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    CREDENTIAL_FIELD_NUMBER: _ClassVar[int]
    mutation: MutationOptions
    provider_id: str
    kind: str
    credential: str
    def __init__(self, mutation: _Optional[_Union[MutationOptions, _Mapping]] = ..., provider_id: _Optional[str] = ..., kind: _Optional[str] = ..., credential: _Optional[str] = ...) -> None: ...

class ClearProviderCredentialRequest(_message.Message):
    __slots__ = ("mutation", "provider_id")
    MUTATION_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_ID_FIELD_NUMBER: _ClassVar[int]
    mutation: MutationOptions
    provider_id: str
    def __init__(self, mutation: _Optional[_Union[MutationOptions, _Mapping]] = ..., provider_id: _Optional[str] = ...) -> None: ...

class AgentDefinitionInput(_message.Message):
    __slots__ = ("slug", "description", "system_prompt", "first_message", "model_id", "fallback_models", "fast_mode")
    SLUG_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    SYSTEM_PROMPT_FIELD_NUMBER: _ClassVar[int]
    FIRST_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    MODEL_ID_FIELD_NUMBER: _ClassVar[int]
    FALLBACK_MODELS_FIELD_NUMBER: _ClassVar[int]
    FAST_MODE_FIELD_NUMBER: _ClassVar[int]
    slug: str
    description: str
    system_prompt: str
    first_message: str
    model_id: str
    fallback_models: _containers.RepeatedScalarFieldContainer[str]
    fast_mode: bool
    def __init__(self, slug: _Optional[str] = ..., description: _Optional[str] = ..., system_prompt: _Optional[str] = ..., first_message: _Optional[str] = ..., model_id: _Optional[str] = ..., fallback_models: _Optional[_Iterable[str]] = ..., fast_mode: _Optional[bool] = ...) -> None: ...

class SaveAgentRequest(_message.Message):
    __slots__ = ("mutation", "workspace_id", "definition", "previous_slug")
    MUTATION_FIELD_NUMBER: _ClassVar[int]
    WORKSPACE_ID_FIELD_NUMBER: _ClassVar[int]
    DEFINITION_FIELD_NUMBER: _ClassVar[int]
    PREVIOUS_SLUG_FIELD_NUMBER: _ClassVar[int]
    mutation: MutationOptions
    workspace_id: str
    definition: AgentDefinitionInput
    previous_slug: str
    def __init__(self, mutation: _Optional[_Union[MutationOptions, _Mapping]] = ..., workspace_id: _Optional[str] = ..., definition: _Optional[_Union[AgentDefinitionInput, _Mapping]] = ..., previous_slug: _Optional[str] = ...) -> None: ...

class DeleteAgentRequest(_message.Message):
    __slots__ = ("mutation", "workspace_id", "slug")
    MUTATION_FIELD_NUMBER: _ClassVar[int]
    WORKSPACE_ID_FIELD_NUMBER: _ClassVar[int]
    SLUG_FIELD_NUMBER: _ClassVar[int]
    mutation: MutationOptions
    workspace_id: str
    slug: str
    def __init__(self, mutation: _Optional[_Union[MutationOptions, _Mapping]] = ..., workspace_id: _Optional[str] = ..., slug: _Optional[str] = ...) -> None: ...

class CheckAgentBrowserRequest(_message.Message):
    __slots__ = ("mutation", "workspace_id")
    MUTATION_FIELD_NUMBER: _ClassVar[int]
    WORKSPACE_ID_FIELD_NUMBER: _ClassVar[int]
    mutation: MutationOptions
    workspace_id: str
    def __init__(self, mutation: _Optional[_Union[MutationOptions, _Mapping]] = ..., workspace_id: _Optional[str] = ...) -> None: ...

class WebSettingsPatch(_message.Message):
    __slots__ = ("backend", "credential")
    BACKEND_FIELD_NUMBER: _ClassVar[int]
    CREDENTIAL_FIELD_NUMBER: _ClassVar[int]
    backend: str
    credential: str
    def __init__(self, backend: _Optional[str] = ..., credential: _Optional[str] = ...) -> None: ...

class MemorySettingsPatch(_message.Message):
    __slots__ = ("backend", "executable", "global_bank", "data_directory")
    BACKEND_FIELD_NUMBER: _ClassVar[int]
    EXECUTABLE_FIELD_NUMBER: _ClassVar[int]
    GLOBAL_BANK_FIELD_NUMBER: _ClassVar[int]
    DATA_DIRECTORY_FIELD_NUMBER: _ClassVar[int]
    backend: str
    executable: str
    global_bank: str
    data_directory: str
    def __init__(self, backend: _Optional[str] = ..., executable: _Optional[str] = ..., global_bank: _Optional[str] = ..., data_directory: _Optional[str] = ...) -> None: ...

class VisionSettingsPatch(_message.Message):
    __slots__ = ("model_id", "clear_model")
    MODEL_ID_FIELD_NUMBER: _ClassVar[int]
    CLEAR_MODEL_FIELD_NUMBER: _ClassVar[int]
    model_id: str
    clear_model: bool
    def __init__(self, model_id: _Optional[str] = ..., clear_model: _Optional[bool] = ...) -> None: ...

class TerminalImagesSettingsPatch(_message.Message):
    __slots__ = ("mode",)
    MODE_FIELD_NUMBER: _ClassVar[int]
    mode: str
    def __init__(self, mode: _Optional[str] = ...) -> None: ...

class SettingsPatch(_message.Message):
    __slots__ = ("web", "memory", "vision", "terminal_images")
    WEB_FIELD_NUMBER: _ClassVar[int]
    MEMORY_FIELD_NUMBER: _ClassVar[int]
    VISION_FIELD_NUMBER: _ClassVar[int]
    TERMINAL_IMAGES_FIELD_NUMBER: _ClassVar[int]
    web: WebSettingsPatch
    memory: MemorySettingsPatch
    vision: VisionSettingsPatch
    terminal_images: TerminalImagesSettingsPatch
    def __init__(self, web: _Optional[_Union[WebSettingsPatch, _Mapping]] = ..., memory: _Optional[_Union[MemorySettingsPatch, _Mapping]] = ..., vision: _Optional[_Union[VisionSettingsPatch, _Mapping]] = ..., terminal_images: _Optional[_Union[TerminalImagesSettingsPatch, _Mapping]] = ...) -> None: ...

class UpdateSettingsRequest(_message.Message):
    __slots__ = ("mutation", "patch")
    MUTATION_FIELD_NUMBER: _ClassVar[int]
    PATCH_FIELD_NUMBER: _ClassVar[int]
    mutation: MutationOptions
    patch: SettingsPatch
    def __init__(self, mutation: _Optional[_Union[MutationOptions, _Mapping]] = ..., patch: _Optional[_Union[SettingsPatch, _Mapping]] = ...) -> None: ...

class DelegateRequest(_message.Message):
    __slots__ = ("mutation", "session_id", "agent_slug", "task")
    MUTATION_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    AGENT_SLUG_FIELD_NUMBER: _ClassVar[int]
    TASK_FIELD_NUMBER: _ClassVar[int]
    mutation: MutationOptions
    session_id: str
    agent_slug: str
    task: str
    def __init__(self, mutation: _Optional[_Union[MutationOptions, _Mapping]] = ..., session_id: _Optional[str] = ..., agent_slug: _Optional[str] = ..., task: _Optional[str] = ...) -> None: ...

class ListRunsRequest(_message.Message):
    __slots__ = ("session_id", "before_run_id", "limit")
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    BEFORE_RUN_ID_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    session_id: str
    before_run_id: str
    limit: int
    def __init__(self, session_id: _Optional[str] = ..., before_run_id: _Optional[str] = ..., limit: _Optional[int] = ...) -> None: ...

class ListRunsResponse(_message.Message):
    __slots__ = ("runs", "has_earlier")
    RUNS_FIELD_NUMBER: _ClassVar[int]
    HAS_EARLIER_FIELD_NUMBER: _ClassVar[int]
    runs: _containers.RepeatedCompositeFieldContainer[RunState]
    has_earlier: bool
    def __init__(self, runs: _Optional[_Iterable[_Union[RunState, _Mapping]]] = ..., has_earlier: _Optional[bool] = ...) -> None: ...

class GetRunRequest(_message.Message):
    __slots__ = ("run_id",)
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    def __init__(self, run_id: _Optional[str] = ...) -> None: ...

class WatchRunRequest(_message.Message):
    __slots__ = ("run_id", "after")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    AFTER_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    after: Cursor
    def __init__(self, run_id: _Optional[str] = ..., after: _Optional[_Union[Cursor, _Mapping]] = ...) -> None: ...

class RunSnapshot(_message.Message):
    __slots__ = ("cursor", "state")
    CURSOR_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    cursor: Cursor
    state: RunState
    def __init__(self, cursor: _Optional[_Union[Cursor, _Mapping]] = ..., state: _Optional[_Union[RunState, _Mapping]] = ...) -> None: ...

class CancelRunRequest(_message.Message):
    __slots__ = ("mutation", "run_id")
    MUTATION_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    mutation: MutationOptions
    run_id: str
    def __init__(self, mutation: _Optional[_Union[MutationOptions, _Mapping]] = ..., run_id: _Optional[str] = ...) -> None: ...

class GetTranscriptPageRequest(_message.Message):
    __slots__ = ("owner_kind", "owner_id", "before_entry_id", "limit")
    OWNER_KIND_FIELD_NUMBER: _ClassVar[int]
    OWNER_ID_FIELD_NUMBER: _ClassVar[int]
    BEFORE_ENTRY_ID_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    owner_kind: TranscriptOwnerKind
    owner_id: str
    before_entry_id: str
    limit: int
    def __init__(self, owner_kind: _Optional[_Union[TranscriptOwnerKind, str]] = ..., owner_id: _Optional[str] = ..., before_entry_id: _Optional[str] = ..., limit: _Optional[int] = ...) -> None: ...

class GetTranscriptBodyWindowRequest(_message.Message):
    __slots__ = ("owner_kind", "owner_id", "entry_id", "before_byte", "limit_bytes")
    OWNER_KIND_FIELD_NUMBER: _ClassVar[int]
    OWNER_ID_FIELD_NUMBER: _ClassVar[int]
    ENTRY_ID_FIELD_NUMBER: _ClassVar[int]
    BEFORE_BYTE_FIELD_NUMBER: _ClassVar[int]
    LIMIT_BYTES_FIELD_NUMBER: _ClassVar[int]
    owner_kind: TranscriptOwnerKind
    owner_id: str
    entry_id: str
    before_byte: int
    limit_bytes: int
    def __init__(self, owner_kind: _Optional[_Union[TranscriptOwnerKind, str]] = ..., owner_id: _Optional[str] = ..., entry_id: _Optional[str] = ..., before_byte: _Optional[int] = ..., limit_bytes: _Optional[int] = ...) -> None: ...

class GetRunTextWindowRequest(_message.Message):
    __slots__ = ("run_id", "field", "before_byte", "limit_bytes")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    FIELD_FIELD_NUMBER: _ClassVar[int]
    BEFORE_BYTE_FIELD_NUMBER: _ClassVar[int]
    LIMIT_BYTES_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    field: RunTextField
    before_byte: int
    limit_bytes: int
    def __init__(self, run_id: _Optional[str] = ..., field: _Optional[_Union[RunTextField, str]] = ..., before_byte: _Optional[int] = ..., limit_bytes: _Optional[int] = ...) -> None: ...

class GetArtifactRequest(_message.Message):
    __slots__ = ("artifact_id",)
    ARTIFACT_ID_FIELD_NUMBER: _ClassVar[int]
    artifact_id: str
    def __init__(self, artifact_id: _Optional[str] = ...) -> None: ...

class GetDiagnosticsRequest(_message.Message):
    __slots__ = ("days", "session_limit", "provider_id")
    DAYS_FIELD_NUMBER: _ClassVar[int]
    SESSION_LIMIT_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_ID_FIELD_NUMBER: _ClassVar[int]
    days: int
    session_limit: int
    provider_id: str
    def __init__(self, days: _Optional[int] = ..., session_limit: _Optional[int] = ..., provider_id: _Optional[str] = ...) -> None: ...

class WorkspaceState(_message.Message):
    __slots__ = ("workspace_id", "workspace_path", "providers", "models", "agents", "skills", "settings", "sessions", "active_session")
    WORKSPACE_ID_FIELD_NUMBER: _ClassVar[int]
    WORKSPACE_PATH_FIELD_NUMBER: _ClassVar[int]
    PROVIDERS_FIELD_NUMBER: _ClassVar[int]
    MODELS_FIELD_NUMBER: _ClassVar[int]
    AGENTS_FIELD_NUMBER: _ClassVar[int]
    SKILLS_FIELD_NUMBER: _ClassVar[int]
    SETTINGS_FIELD_NUMBER: _ClassVar[int]
    SESSIONS_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_SESSION_FIELD_NUMBER: _ClassVar[int]
    workspace_id: str
    workspace_path: str
    providers: _containers.RepeatedCompositeFieldContainer[Provider]
    models: _containers.RepeatedCompositeFieldContainer[Model]
    agents: _containers.RepeatedCompositeFieldContainer[AgentDefinition]
    skills: _containers.RepeatedCompositeFieldContainer[Skill]
    settings: Settings
    sessions: _containers.RepeatedCompositeFieldContainer[SessionSummary]
    active_session: SessionState
    def __init__(self, workspace_id: _Optional[str] = ..., workspace_path: _Optional[str] = ..., providers: _Optional[_Iterable[_Union[Provider, _Mapping]]] = ..., models: _Optional[_Iterable[_Union[Model, _Mapping]]] = ..., agents: _Optional[_Iterable[_Union[AgentDefinition, _Mapping]]] = ..., skills: _Optional[_Iterable[_Union[Skill, _Mapping]]] = ..., settings: _Optional[_Union[Settings, _Mapping]] = ..., sessions: _Optional[_Iterable[_Union[SessionSummary, _Mapping]]] = ..., active_session: _Optional[_Union[SessionState, _Mapping]] = ...) -> None: ...

class ProviderCapabilities(_message.Message):
    __slots__ = ("supported",)
    SUPPORTED_FIELD_NUMBER: _ClassVar[int]
    supported: _containers.RepeatedScalarFieldContainer[ProviderCapability]
    def __init__(self, supported: _Optional[_Iterable[_Union[ProviderCapability, str]]] = ...) -> None: ...

class Connection(_message.Message):
    __slots__ = ("state", "message")
    STATE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    state: ConnectionState
    message: str
    def __init__(self, state: _Optional[_Union[ConnectionState, str]] = ..., message: _Optional[str] = ...) -> None: ...

class ProviderAuthentication(_message.Message):
    __slots__ = ("kind", "dashboard_url", "credential_kind", "verification_url", "user_code")
    class Kind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        KIND_UNSPECIFIED: _ClassVar[ProviderAuthentication.Kind]
        KIND_STARTING: _ClassVar[ProviderAuthentication.Kind]
        KIND_API_KEY_REQUIRED: _ClassVar[ProviderAuthentication.Kind]
        KIND_CHALLENGE: _ClassVar[ProviderAuthentication.Kind]
    KIND_UNSPECIFIED: ProviderAuthentication.Kind
    KIND_STARTING: ProviderAuthentication.Kind
    KIND_API_KEY_REQUIRED: ProviderAuthentication.Kind
    KIND_CHALLENGE: ProviderAuthentication.Kind
    KIND_FIELD_NUMBER: _ClassVar[int]
    DASHBOARD_URL_FIELD_NUMBER: _ClassVar[int]
    CREDENTIAL_KIND_FIELD_NUMBER: _ClassVar[int]
    VERIFICATION_URL_FIELD_NUMBER: _ClassVar[int]
    USER_CODE_FIELD_NUMBER: _ClassVar[int]
    kind: ProviderAuthentication.Kind
    dashboard_url: str
    credential_kind: str
    verification_url: str
    user_code: str
    def __init__(self, kind: _Optional[_Union[ProviderAuthentication.Kind, str]] = ..., dashboard_url: _Optional[str] = ..., credential_kind: _Optional[str] = ..., verification_url: _Optional[str] = ..., user_code: _Optional[str] = ...) -> None: ...

class Provider(_message.Message):
    __slots__ = ("id", "display_name", "enabled", "credential_configured", "credential_kind", "connection", "capabilities", "authentication")
    ID_FIELD_NUMBER: _ClassVar[int]
    DISPLAY_NAME_FIELD_NUMBER: _ClassVar[int]
    ENABLED_FIELD_NUMBER: _ClassVar[int]
    CREDENTIAL_CONFIGURED_FIELD_NUMBER: _ClassVar[int]
    CREDENTIAL_KIND_FIELD_NUMBER: _ClassVar[int]
    CONNECTION_FIELD_NUMBER: _ClassVar[int]
    CAPABILITIES_FIELD_NUMBER: _ClassVar[int]
    AUTHENTICATION_FIELD_NUMBER: _ClassVar[int]
    id: str
    display_name: str
    enabled: bool
    credential_configured: bool
    credential_kind: str
    connection: Connection
    capabilities: ProviderCapabilities
    authentication: ProviderAuthentication
    def __init__(self, id: _Optional[str] = ..., display_name: _Optional[str] = ..., enabled: _Optional[bool] = ..., credential_configured: _Optional[bool] = ..., credential_kind: _Optional[str] = ..., connection: _Optional[_Union[Connection, _Mapping]] = ..., capabilities: _Optional[_Union[ProviderCapabilities, _Mapping]] = ..., authentication: _Optional[_Union[ProviderAuthentication, _Mapping]] = ...) -> None: ...

class ModelConfiguration(_message.Message):
    __slots__ = ("reasoning_efforts", "fast_mode_configurable", "vision_eligible")
    REASONING_EFFORTS_FIELD_NUMBER: _ClassVar[int]
    FAST_MODE_CONFIGURABLE_FIELD_NUMBER: _ClassVar[int]
    VISION_ELIGIBLE_FIELD_NUMBER: _ClassVar[int]
    reasoning_efforts: _containers.RepeatedScalarFieldContainer[str]
    fast_mode_configurable: bool
    vision_eligible: bool
    def __init__(self, reasoning_efforts: _Optional[_Iterable[str]] = ..., fast_mode_configurable: _Optional[bool] = ..., vision_eligible: _Optional[bool] = ...) -> None: ...

class Model(_message.Message):
    __slots__ = ("id", "provider_id", "model_slug", "display_name", "is_default", "reasoning_effort", "fast_mode", "configuration")
    ID_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_ID_FIELD_NUMBER: _ClassVar[int]
    MODEL_SLUG_FIELD_NUMBER: _ClassVar[int]
    DISPLAY_NAME_FIELD_NUMBER: _ClassVar[int]
    IS_DEFAULT_FIELD_NUMBER: _ClassVar[int]
    REASONING_EFFORT_FIELD_NUMBER: _ClassVar[int]
    FAST_MODE_FIELD_NUMBER: _ClassVar[int]
    CONFIGURATION_FIELD_NUMBER: _ClassVar[int]
    id: str
    provider_id: str
    model_slug: str
    display_name: str
    is_default: bool
    reasoning_effort: str
    fast_mode: bool
    configuration: ModelConfiguration
    def __init__(self, id: _Optional[str] = ..., provider_id: _Optional[str] = ..., model_slug: _Optional[str] = ..., display_name: _Optional[str] = ..., is_default: _Optional[bool] = ..., reasoning_effort: _Optional[str] = ..., fast_mode: _Optional[bool] = ..., configuration: _Optional[_Union[ModelConfiguration, _Mapping]] = ...) -> None: ...

class AgentDefinition(_message.Message):
    __slots__ = ("slug", "description", "system_prompt", "first_message", "model_id", "fallback_models", "fast_mode")
    SLUG_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    SYSTEM_PROMPT_FIELD_NUMBER: _ClassVar[int]
    FIRST_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    MODEL_ID_FIELD_NUMBER: _ClassVar[int]
    FALLBACK_MODELS_FIELD_NUMBER: _ClassVar[int]
    FAST_MODE_FIELD_NUMBER: _ClassVar[int]
    slug: str
    description: str
    system_prompt: str
    first_message: str
    model_id: str
    fallback_models: _containers.RepeatedScalarFieldContainer[str]
    fast_mode: bool
    def __init__(self, slug: _Optional[str] = ..., description: _Optional[str] = ..., system_prompt: _Optional[str] = ..., first_message: _Optional[str] = ..., model_id: _Optional[str] = ..., fallback_models: _Optional[_Iterable[str]] = ..., fast_mode: _Optional[bool] = ...) -> None: ...

class Skill(_message.Message):
    __slots__ = ("name", "description")
    NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    name: str
    description: str
    def __init__(self, name: _Optional[str] = ..., description: _Optional[str] = ...) -> None: ...

class WebSettings(_message.Message):
    __slots__ = ("backend", "credential_configured", "agent_browser")
    BACKEND_FIELD_NUMBER: _ClassVar[int]
    CREDENTIAL_CONFIGURED_FIELD_NUMBER: _ClassVar[int]
    AGENT_BROWSER_FIELD_NUMBER: _ClassVar[int]
    backend: str
    credential_configured: bool
    agent_browser: AgentBrowser
    def __init__(self, backend: _Optional[str] = ..., credential_configured: _Optional[bool] = ..., agent_browser: _Optional[_Union[AgentBrowser, _Mapping]] = ...) -> None: ...

class AgentBrowser(_message.Message):
    __slots__ = ("state", "version")
    class State(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        STATE_UNSPECIFIED: _ClassVar[AgentBrowser.State]
        STATE_CHECKING: _ClassVar[AgentBrowser.State]
        STATE_AVAILABLE: _ClassVar[AgentBrowser.State]
        STATE_UNAVAILABLE: _ClassVar[AgentBrowser.State]
    STATE_UNSPECIFIED: AgentBrowser.State
    STATE_CHECKING: AgentBrowser.State
    STATE_AVAILABLE: AgentBrowser.State
    STATE_UNAVAILABLE: AgentBrowser.State
    STATE_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    state: AgentBrowser.State
    version: str
    def __init__(self, state: _Optional[_Union[AgentBrowser.State, str]] = ..., version: _Optional[str] = ...) -> None: ...

class MemorySettings(_message.Message):
    __slots__ = ("backend", "executable", "global_bank", "data_directory", "configured", "available")
    BACKEND_FIELD_NUMBER: _ClassVar[int]
    EXECUTABLE_FIELD_NUMBER: _ClassVar[int]
    GLOBAL_BANK_FIELD_NUMBER: _ClassVar[int]
    DATA_DIRECTORY_FIELD_NUMBER: _ClassVar[int]
    CONFIGURED_FIELD_NUMBER: _ClassVar[int]
    AVAILABLE_FIELD_NUMBER: _ClassVar[int]
    backend: str
    executable: str
    global_bank: str
    data_directory: str
    configured: bool
    available: bool
    def __init__(self, backend: _Optional[str] = ..., executable: _Optional[str] = ..., global_bank: _Optional[str] = ..., data_directory: _Optional[str] = ..., configured: _Optional[bool] = ..., available: _Optional[bool] = ...) -> None: ...

class VisionSettings(_message.Message):
    __slots__ = ("model_id",)
    MODEL_ID_FIELD_NUMBER: _ClassVar[int]
    model_id: str
    def __init__(self, model_id: _Optional[str] = ...) -> None: ...

class Settings(_message.Message):
    __slots__ = ("web", "memory", "vision", "terminal_images")
    WEB_FIELD_NUMBER: _ClassVar[int]
    MEMORY_FIELD_NUMBER: _ClassVar[int]
    VISION_FIELD_NUMBER: _ClassVar[int]
    TERMINAL_IMAGES_FIELD_NUMBER: _ClassVar[int]
    web: WebSettings
    memory: MemorySettings
    vision: VisionSettings
    terminal_images: TerminalImageMode
    def __init__(self, web: _Optional[_Union[WebSettings, _Mapping]] = ..., memory: _Optional[_Union[MemorySettings, _Mapping]] = ..., vision: _Optional[_Union[VisionSettings, _Mapping]] = ..., terminal_images: _Optional[_Union[TerminalImageMode, str]] = ...) -> None: ...

class SessionSummary(_message.Message):
    __slots__ = ("id", "workspace_id", "title", "active_provider_id", "active_model_id", "updated_at_ms")
    ID_FIELD_NUMBER: _ClassVar[int]
    WORKSPACE_ID_FIELD_NUMBER: _ClassVar[int]
    TITLE_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_PROVIDER_ID_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_MODEL_ID_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_MS_FIELD_NUMBER: _ClassVar[int]
    id: str
    workspace_id: str
    title: str
    active_provider_id: str
    active_model_id: str
    updated_at_ms: int
    def __init__(self, id: _Optional[str] = ..., workspace_id: _Optional[str] = ..., title: _Optional[str] = ..., active_provider_id: _Optional[str] = ..., active_model_id: _Optional[str] = ..., updated_at_ms: _Optional[int] = ...) -> None: ...

class AgentSession(_message.Message):
    __slots__ = ("id", "provider_id", "model_id", "role", "capabilities", "connection")
    ID_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_ID_FIELD_NUMBER: _ClassVar[int]
    MODEL_ID_FIELD_NUMBER: _ClassVar[int]
    ROLE_FIELD_NUMBER: _ClassVar[int]
    CAPABILITIES_FIELD_NUMBER: _ClassVar[int]
    CONNECTION_FIELD_NUMBER: _ClassVar[int]
    id: str
    provider_id: str
    model_id: str
    role: str
    capabilities: ProviderCapabilities
    connection: Connection
    def __init__(self, id: _Optional[str] = ..., provider_id: _Optional[str] = ..., model_id: _Optional[str] = ..., role: _Optional[str] = ..., capabilities: _Optional[_Union[ProviderCapabilities, _Mapping]] = ..., connection: _Optional[_Union[Connection, _Mapping]] = ...) -> None: ...

class Turn(_message.Message):
    __slots__ = ("id", "agent_session_id", "model_id", "status")
    ID_FIELD_NUMBER: _ClassVar[int]
    AGENT_SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    MODEL_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    id: str
    agent_session_id: str
    model_id: str
    status: TurnStatus
    def __init__(self, id: _Optional[str] = ..., agent_session_id: _Optional[str] = ..., model_id: _Optional[str] = ..., status: _Optional[_Union[TurnStatus, str]] = ...) -> None: ...

class ContextUsage(_message.Message):
    __slots__ = ("estimated_tokens", "context_window", "compacting")
    ESTIMATED_TOKENS_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_WINDOW_FIELD_NUMBER: _ClassVar[int]
    COMPACTING_FIELD_NUMBER: _ClassVar[int]
    estimated_tokens: int
    context_window: int
    compacting: bool
    def __init__(self, estimated_tokens: _Optional[int] = ..., context_window: _Optional[int] = ..., compacting: _Optional[bool] = ...) -> None: ...

class SessionState(_message.Message):
    __slots__ = ("id", "revision", "workspace_id", "title", "status_message", "diagnostic_count", "activity", "selected_provider_id", "selected_model_id", "active_agent_session", "active_turn", "context_usage", "transcript", "recoverable_prompt", "queue", "interactions", "todos", "runs", "runs_has_earlier", "notices")
    ID_FIELD_NUMBER: _ClassVar[int]
    REVISION_FIELD_NUMBER: _ClassVar[int]
    WORKSPACE_ID_FIELD_NUMBER: _ClassVar[int]
    TITLE_FIELD_NUMBER: _ClassVar[int]
    STATUS_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    DIAGNOSTIC_COUNT_FIELD_NUMBER: _ClassVar[int]
    ACTIVITY_FIELD_NUMBER: _ClassVar[int]
    SELECTED_PROVIDER_ID_FIELD_NUMBER: _ClassVar[int]
    SELECTED_MODEL_ID_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_AGENT_SESSION_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_TURN_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_USAGE_FIELD_NUMBER: _ClassVar[int]
    TRANSCRIPT_FIELD_NUMBER: _ClassVar[int]
    RECOVERABLE_PROMPT_FIELD_NUMBER: _ClassVar[int]
    QUEUE_FIELD_NUMBER: _ClassVar[int]
    INTERACTIONS_FIELD_NUMBER: _ClassVar[int]
    TODOS_FIELD_NUMBER: _ClassVar[int]
    RUNS_FIELD_NUMBER: _ClassVar[int]
    RUNS_HAS_EARLIER_FIELD_NUMBER: _ClassVar[int]
    NOTICES_FIELD_NUMBER: _ClassVar[int]
    id: str
    revision: int
    workspace_id: str
    title: str
    status_message: str
    diagnostic_count: int
    activity: SessionActivity
    selected_provider_id: str
    selected_model_id: str
    active_agent_session: AgentSession
    active_turn: Turn
    context_usage: ContextUsage
    transcript: TranscriptPage
    recoverable_prompt: RecoverablePrompt
    queue: _containers.RepeatedCompositeFieldContainer[QueueItem]
    interactions: _containers.RepeatedCompositeFieldContainer[Interaction]
    todos: _containers.RepeatedCompositeFieldContainer[TodoPhase]
    runs: _containers.RepeatedCompositeFieldContainer[RunState]
    runs_has_earlier: bool
    notices: _containers.RepeatedCompositeFieldContainer[Notice]
    def __init__(self, id: _Optional[str] = ..., revision: _Optional[int] = ..., workspace_id: _Optional[str] = ..., title: _Optional[str] = ..., status_message: _Optional[str] = ..., diagnostic_count: _Optional[int] = ..., activity: _Optional[_Union[SessionActivity, str]] = ..., selected_provider_id: _Optional[str] = ..., selected_model_id: _Optional[str] = ..., active_agent_session: _Optional[_Union[AgentSession, _Mapping]] = ..., active_turn: _Optional[_Union[Turn, _Mapping]] = ..., context_usage: _Optional[_Union[ContextUsage, _Mapping]] = ..., transcript: _Optional[_Union[TranscriptPage, _Mapping]] = ..., recoverable_prompt: _Optional[_Union[RecoverablePrompt, _Mapping]] = ..., queue: _Optional[_Iterable[_Union[QueueItem, _Mapping]]] = ..., interactions: _Optional[_Iterable[_Union[Interaction, _Mapping]]] = ..., todos: _Optional[_Iterable[_Union[TodoPhase, _Mapping]]] = ..., runs: _Optional[_Iterable[_Union[RunState, _Mapping]]] = ..., runs_has_earlier: _Optional[bool] = ..., notices: _Optional[_Iterable[_Union[Notice, _Mapping]]] = ...) -> None: ...

class TranscriptEntry(_message.Message):
    __slots__ = ("id", "kind", "title", "body", "body_start_byte", "body_total_bytes", "status", "artifact_ids")
    ID_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    TITLE_FIELD_NUMBER: _ClassVar[int]
    BODY_FIELD_NUMBER: _ClassVar[int]
    BODY_START_BYTE_FIELD_NUMBER: _ClassVar[int]
    BODY_TOTAL_BYTES_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    ARTIFACT_IDS_FIELD_NUMBER: _ClassVar[int]
    id: str
    kind: TranscriptEntryKind
    title: str
    body: str
    body_start_byte: int
    body_total_bytes: int
    status: TranscriptEntryStatus
    artifact_ids: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, id: _Optional[str] = ..., kind: _Optional[_Union[TranscriptEntryKind, str]] = ..., title: _Optional[str] = ..., body: _Optional[str] = ..., body_start_byte: _Optional[int] = ..., body_total_bytes: _Optional[int] = ..., status: _Optional[_Union[TranscriptEntryStatus, str]] = ..., artifact_ids: _Optional[_Iterable[str]] = ...) -> None: ...

class TranscriptPage(_message.Message):
    __slots__ = ("entries", "has_earlier", "stream_active", "stream_label")
    ENTRIES_FIELD_NUMBER: _ClassVar[int]
    HAS_EARLIER_FIELD_NUMBER: _ClassVar[int]
    STREAM_ACTIVE_FIELD_NUMBER: _ClassVar[int]
    STREAM_LABEL_FIELD_NUMBER: _ClassVar[int]
    entries: _containers.RepeatedCompositeFieldContainer[TranscriptEntry]
    has_earlier: bool
    stream_active: bool
    stream_label: str
    def __init__(self, entries: _Optional[_Iterable[_Union[TranscriptEntry, _Mapping]]] = ..., has_earlier: _Optional[bool] = ..., stream_active: _Optional[bool] = ..., stream_label: _Optional[str] = ...) -> None: ...

class TranscriptBodyWindow(_message.Message):
    __slots__ = ("entry_id", "body", "start_byte", "total_bytes", "has_earlier")
    ENTRY_ID_FIELD_NUMBER: _ClassVar[int]
    BODY_FIELD_NUMBER: _ClassVar[int]
    START_BYTE_FIELD_NUMBER: _ClassVar[int]
    TOTAL_BYTES_FIELD_NUMBER: _ClassVar[int]
    HAS_EARLIER_FIELD_NUMBER: _ClassVar[int]
    entry_id: str
    body: str
    start_byte: int
    total_bytes: int
    has_earlier: bool
    def __init__(self, entry_id: _Optional[str] = ..., body: _Optional[str] = ..., start_byte: _Optional[int] = ..., total_bytes: _Optional[int] = ..., has_earlier: _Optional[bool] = ...) -> None: ...

class QueueItem(_message.Message):
    __slots__ = ("id", "summary", "attachment_count")
    ID_FIELD_NUMBER: _ClassVar[int]
    SUMMARY_FIELD_NUMBER: _ClassVar[int]
    ATTACHMENT_COUNT_FIELD_NUMBER: _ClassVar[int]
    id: str
    summary: str
    attachment_count: int
    def __init__(self, id: _Optional[str] = ..., summary: _Optional[str] = ..., attachment_count: _Optional[int] = ...) -> None: ...

class RecoverablePrompt(_message.Message):
    __slots__ = ("id", "text", "attachments")
    ID_FIELD_NUMBER: _ClassVar[int]
    TEXT_FIELD_NUMBER: _ClassVar[int]
    ATTACHMENTS_FIELD_NUMBER: _ClassVar[int]
    id: str
    text: str
    attachments: _containers.RepeatedCompositeFieldContainer[PromptAttachment]
    def __init__(self, id: _Optional[str] = ..., text: _Optional[str] = ..., attachments: _Optional[_Iterable[_Union[PromptAttachment, _Mapping]]] = ...) -> None: ...

class InteractionOption(_message.Message):
    __slots__ = ("id", "label", "description", "recommended")
    ID_FIELD_NUMBER: _ClassVar[int]
    LABEL_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    RECOMMENDED_FIELD_NUMBER: _ClassVar[int]
    id: str
    label: str
    description: str
    recommended: bool
    def __init__(self, id: _Optional[str] = ..., label: _Optional[str] = ..., description: _Optional[str] = ..., recommended: _Optional[bool] = ...) -> None: ...

class Interaction(_message.Message):
    __slots__ = ("id", "revision", "kind", "status", "title", "detail", "options", "multiple")
    ID_FIELD_NUMBER: _ClassVar[int]
    REVISION_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    TITLE_FIELD_NUMBER: _ClassVar[int]
    DETAIL_FIELD_NUMBER: _ClassVar[int]
    OPTIONS_FIELD_NUMBER: _ClassVar[int]
    MULTIPLE_FIELD_NUMBER: _ClassVar[int]
    id: str
    revision: int
    kind: InteractionKind
    status: InteractionStatus
    title: str
    detail: str
    options: _containers.RepeatedCompositeFieldContainer[InteractionOption]
    multiple: bool
    def __init__(self, id: _Optional[str] = ..., revision: _Optional[int] = ..., kind: _Optional[_Union[InteractionKind, str]] = ..., status: _Optional[_Union[InteractionStatus, str]] = ..., title: _Optional[str] = ..., detail: _Optional[str] = ..., options: _Optional[_Iterable[_Union[InteractionOption, _Mapping]]] = ..., multiple: _Optional[bool] = ...) -> None: ...

class TodoItem(_message.Message):
    __slots__ = ("content", "status")
    CONTENT_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    content: str
    status: TodoStatus
    def __init__(self, content: _Optional[str] = ..., status: _Optional[_Union[TodoStatus, str]] = ...) -> None: ...

class TodoPhase(_message.Message):
    __slots__ = ("name", "tasks")
    NAME_FIELD_NUMBER: _ClassVar[int]
    TASKS_FIELD_NUMBER: _ClassVar[int]
    name: str
    tasks: _containers.RepeatedCompositeFieldContainer[TodoItem]
    def __init__(self, name: _Optional[str] = ..., tasks: _Optional[_Iterable[_Union[TodoItem, _Mapping]]] = ...) -> None: ...

class Notice(_message.Message):
    __slots__ = ("id", "level", "message")
    ID_FIELD_NUMBER: _ClassVar[int]
    LEVEL_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    id: str
    level: NoticeLevel
    message: str
    def __init__(self, id: _Optional[str] = ..., level: _Optional[_Union[NoticeLevel, str]] = ..., message: _Optional[str] = ...) -> None: ...

class RunOutcome(_message.Message):
    __slots__ = ("kind", "body")
    class Kind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        KIND_UNSPECIFIED: _ClassVar[RunOutcome.Kind]
        KIND_COMPLETED: _ClassVar[RunOutcome.Kind]
        KIND_FAILED: _ClassVar[RunOutcome.Kind]
        KIND_INTERRUPTED: _ClassVar[RunOutcome.Kind]
    KIND_UNSPECIFIED: RunOutcome.Kind
    KIND_COMPLETED: RunOutcome.Kind
    KIND_FAILED: RunOutcome.Kind
    KIND_INTERRUPTED: RunOutcome.Kind
    KIND_FIELD_NUMBER: _ClassVar[int]
    BODY_FIELD_NUMBER: _ClassVar[int]
    kind: RunOutcome.Kind
    body: str
    def __init__(self, kind: _Optional[_Union[RunOutcome.Kind, str]] = ..., body: _Optional[str] = ...) -> None: ...

class RunState(_message.Message):
    __slots__ = ("id", "agent_slug", "provider_id", "objective", "objective_start_byte", "objective_total_bytes", "status", "latest_activity", "latest_activity_start_byte", "latest_activity_total_bytes", "outcome", "outcome_start_byte", "outcome_total_bytes", "result", "result_start_byte", "result_total_bytes", "transcript")
    ID_FIELD_NUMBER: _ClassVar[int]
    AGENT_SLUG_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_ID_FIELD_NUMBER: _ClassVar[int]
    OBJECTIVE_FIELD_NUMBER: _ClassVar[int]
    OBJECTIVE_START_BYTE_FIELD_NUMBER: _ClassVar[int]
    OBJECTIVE_TOTAL_BYTES_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    LATEST_ACTIVITY_FIELD_NUMBER: _ClassVar[int]
    LATEST_ACTIVITY_START_BYTE_FIELD_NUMBER: _ClassVar[int]
    LATEST_ACTIVITY_TOTAL_BYTES_FIELD_NUMBER: _ClassVar[int]
    OUTCOME_FIELD_NUMBER: _ClassVar[int]
    OUTCOME_START_BYTE_FIELD_NUMBER: _ClassVar[int]
    OUTCOME_TOTAL_BYTES_FIELD_NUMBER: _ClassVar[int]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    RESULT_START_BYTE_FIELD_NUMBER: _ClassVar[int]
    RESULT_TOTAL_BYTES_FIELD_NUMBER: _ClassVar[int]
    TRANSCRIPT_FIELD_NUMBER: _ClassVar[int]
    id: str
    agent_slug: str
    provider_id: str
    objective: str
    objective_start_byte: int
    objective_total_bytes: int
    status: RunStatus
    latest_activity: str
    latest_activity_start_byte: int
    latest_activity_total_bytes: int
    outcome: RunOutcome
    outcome_start_byte: int
    outcome_total_bytes: int
    result: str
    result_start_byte: int
    result_total_bytes: int
    transcript: TranscriptPage
    def __init__(self, id: _Optional[str] = ..., agent_slug: _Optional[str] = ..., provider_id: _Optional[str] = ..., objective: _Optional[str] = ..., objective_start_byte: _Optional[int] = ..., objective_total_bytes: _Optional[int] = ..., status: _Optional[_Union[RunStatus, str]] = ..., latest_activity: _Optional[str] = ..., latest_activity_start_byte: _Optional[int] = ..., latest_activity_total_bytes: _Optional[int] = ..., outcome: _Optional[_Union[RunOutcome, _Mapping]] = ..., outcome_start_byte: _Optional[int] = ..., outcome_total_bytes: _Optional[int] = ..., result: _Optional[str] = ..., result_start_byte: _Optional[int] = ..., result_total_bytes: _Optional[int] = ..., transcript: _Optional[_Union[TranscriptPage, _Mapping]] = ...) -> None: ...

class RunTextWindow(_message.Message):
    __slots__ = ("run_id", "field", "text", "start_byte", "total_bytes", "has_earlier")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    FIELD_FIELD_NUMBER: _ClassVar[int]
    TEXT_FIELD_NUMBER: _ClassVar[int]
    START_BYTE_FIELD_NUMBER: _ClassVar[int]
    TOTAL_BYTES_FIELD_NUMBER: _ClassVar[int]
    HAS_EARLIER_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    field: RunTextField
    text: str
    start_byte: int
    total_bytes: int
    has_earlier: bool
    def __init__(self, run_id: _Optional[str] = ..., field: _Optional[_Union[RunTextField, str]] = ..., text: _Optional[str] = ..., start_byte: _Optional[int] = ..., total_bytes: _Optional[int] = ..., has_earlier: _Optional[bool] = ...) -> None: ...

class Artifact(_message.Message):
    __slots__ = ("id", "label", "media_type", "byte_length", "data")
    ID_FIELD_NUMBER: _ClassVar[int]
    LABEL_FIELD_NUMBER: _ClassVar[int]
    MEDIA_TYPE_FIELD_NUMBER: _ClassVar[int]
    BYTE_LENGTH_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    id: str
    label: str
    media_type: str
    byte_length: int
    data: bytes
    def __init__(self, id: _Optional[str] = ..., label: _Optional[str] = ..., media_type: _Optional[str] = ..., byte_length: _Optional[int] = ..., data: _Optional[bytes] = ...) -> None: ...

class DiagnosticsUsageTotals(_message.Message):
    __slots__ = ("inference_rounds", "compaction_rounds", "failed_rounds", "retry_count", "estimated_input_tokens", "reported_input_tokens", "reported_cached_input_tokens", "reported_cache_write_tokens", "reported_output_tokens", "request_bytes", "response_bytes", "inference_duration_ms", "requested_tool_calls", "executed_tool_calls", "failed_tool_calls", "full_tool_output_bytes", "model_tool_output_bytes", "tool_duration_ms")
    INFERENCE_ROUNDS_FIELD_NUMBER: _ClassVar[int]
    COMPACTION_ROUNDS_FIELD_NUMBER: _ClassVar[int]
    FAILED_ROUNDS_FIELD_NUMBER: _ClassVar[int]
    RETRY_COUNT_FIELD_NUMBER: _ClassVar[int]
    ESTIMATED_INPUT_TOKENS_FIELD_NUMBER: _ClassVar[int]
    REPORTED_INPUT_TOKENS_FIELD_NUMBER: _ClassVar[int]
    REPORTED_CACHED_INPUT_TOKENS_FIELD_NUMBER: _ClassVar[int]
    REPORTED_CACHE_WRITE_TOKENS_FIELD_NUMBER: _ClassVar[int]
    REPORTED_OUTPUT_TOKENS_FIELD_NUMBER: _ClassVar[int]
    REQUEST_BYTES_FIELD_NUMBER: _ClassVar[int]
    RESPONSE_BYTES_FIELD_NUMBER: _ClassVar[int]
    INFERENCE_DURATION_MS_FIELD_NUMBER: _ClassVar[int]
    REQUESTED_TOOL_CALLS_FIELD_NUMBER: _ClassVar[int]
    EXECUTED_TOOL_CALLS_FIELD_NUMBER: _ClassVar[int]
    FAILED_TOOL_CALLS_FIELD_NUMBER: _ClassVar[int]
    FULL_TOOL_OUTPUT_BYTES_FIELD_NUMBER: _ClassVar[int]
    MODEL_TOOL_OUTPUT_BYTES_FIELD_NUMBER: _ClassVar[int]
    TOOL_DURATION_MS_FIELD_NUMBER: _ClassVar[int]
    inference_rounds: int
    compaction_rounds: int
    failed_rounds: int
    retry_count: int
    estimated_input_tokens: int
    reported_input_tokens: int
    reported_cached_input_tokens: int
    reported_cache_write_tokens: int
    reported_output_tokens: int
    request_bytes: int
    response_bytes: int
    inference_duration_ms: int
    requested_tool_calls: int
    executed_tool_calls: int
    failed_tool_calls: int
    full_tool_output_bytes: int
    model_tool_output_bytes: int
    tool_duration_ms: int
    def __init__(self, inference_rounds: _Optional[int] = ..., compaction_rounds: _Optional[int] = ..., failed_rounds: _Optional[int] = ..., retry_count: _Optional[int] = ..., estimated_input_tokens: _Optional[int] = ..., reported_input_tokens: _Optional[int] = ..., reported_cached_input_tokens: _Optional[int] = ..., reported_cache_write_tokens: _Optional[int] = ..., reported_output_tokens: _Optional[int] = ..., request_bytes: _Optional[int] = ..., response_bytes: _Optional[int] = ..., inference_duration_ms: _Optional[int] = ..., requested_tool_calls: _Optional[int] = ..., executed_tool_calls: _Optional[int] = ..., failed_tool_calls: _Optional[int] = ..., full_tool_output_bytes: _Optional[int] = ..., model_tool_output_bytes: _Optional[int] = ..., tool_duration_ms: _Optional[int] = ...) -> None: ...

class DiagnosticsDailyUsage(_message.Message):
    __slots__ = ("date_utc", "provider_id", "totals")
    DATE_UTC_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_ID_FIELD_NUMBER: _ClassVar[int]
    TOTALS_FIELD_NUMBER: _ClassVar[int]
    date_utc: str
    provider_id: str
    totals: DiagnosticsUsageTotals
    def __init__(self, date_utc: _Optional[str] = ..., provider_id: _Optional[str] = ..., totals: _Optional[_Union[DiagnosticsUsageTotals, _Mapping]] = ...) -> None: ...

class DiagnosticsToolUsage(_message.Message):
    __slots__ = ("provider_id", "tool", "calls", "failures", "full_output_bytes", "model_output_bytes", "duration_ms")
    PROVIDER_ID_FIELD_NUMBER: _ClassVar[int]
    TOOL_FIELD_NUMBER: _ClassVar[int]
    CALLS_FIELD_NUMBER: _ClassVar[int]
    FAILURES_FIELD_NUMBER: _ClassVar[int]
    FULL_OUTPUT_BYTES_FIELD_NUMBER: _ClassVar[int]
    MODEL_OUTPUT_BYTES_FIELD_NUMBER: _ClassVar[int]
    DURATION_MS_FIELD_NUMBER: _ClassVar[int]
    provider_id: str
    tool: str
    calls: int
    failures: int
    full_output_bytes: int
    model_output_bytes: int
    duration_ms: int
    def __init__(self, provider_id: _Optional[str] = ..., tool: _Optional[str] = ..., calls: _Optional[int] = ..., failures: _Optional[int] = ..., full_output_bytes: _Optional[int] = ..., model_output_bytes: _Optional[int] = ..., duration_ms: _Optional[int] = ...) -> None: ...

class DiagnosticsSessionUsage(_message.Message):
    __slots__ = ("session_id", "provider_id", "model", "latest_activity_ms", "totals")
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_ID_FIELD_NUMBER: _ClassVar[int]
    MODEL_FIELD_NUMBER: _ClassVar[int]
    LATEST_ACTIVITY_MS_FIELD_NUMBER: _ClassVar[int]
    TOTALS_FIELD_NUMBER: _ClassVar[int]
    session_id: str
    provider_id: str
    model: str
    latest_activity_ms: int
    totals: DiagnosticsUsageTotals
    def __init__(self, session_id: _Optional[str] = ..., provider_id: _Optional[str] = ..., model: _Optional[str] = ..., latest_activity_ms: _Optional[int] = ..., totals: _Optional[_Union[DiagnosticsUsageTotals, _Mapping]] = ...) -> None: ...

class DiagnosticsReport(_message.Message):
    __slots__ = ("generated_at_ms", "period_days", "provider_filter", "sessions_scanned", "sessions_with_activity", "totals", "daily", "tools", "sessions", "notes")
    GENERATED_AT_MS_FIELD_NUMBER: _ClassVar[int]
    PERIOD_DAYS_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_FILTER_FIELD_NUMBER: _ClassVar[int]
    SESSIONS_SCANNED_FIELD_NUMBER: _ClassVar[int]
    SESSIONS_WITH_ACTIVITY_FIELD_NUMBER: _ClassVar[int]
    TOTALS_FIELD_NUMBER: _ClassVar[int]
    DAILY_FIELD_NUMBER: _ClassVar[int]
    TOOLS_FIELD_NUMBER: _ClassVar[int]
    SESSIONS_FIELD_NUMBER: _ClassVar[int]
    NOTES_FIELD_NUMBER: _ClassVar[int]
    generated_at_ms: int
    period_days: int
    provider_filter: str
    sessions_scanned: int
    sessions_with_activity: int
    totals: DiagnosticsUsageTotals
    daily: _containers.RepeatedCompositeFieldContainer[DiagnosticsDailyUsage]
    tools: _containers.RepeatedCompositeFieldContainer[DiagnosticsToolUsage]
    sessions: _containers.RepeatedCompositeFieldContainer[DiagnosticsSessionUsage]
    notes: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, generated_at_ms: _Optional[int] = ..., period_days: _Optional[int] = ..., provider_filter: _Optional[str] = ..., sessions_scanned: _Optional[int] = ..., sessions_with_activity: _Optional[int] = ..., totals: _Optional[_Union[DiagnosticsUsageTotals, _Mapping]] = ..., daily: _Optional[_Iterable[_Union[DiagnosticsDailyUsage, _Mapping]]] = ..., tools: _Optional[_Iterable[_Union[DiagnosticsToolUsage, _Mapping]]] = ..., sessions: _Optional[_Iterable[_Union[DiagnosticsSessionUsage, _Mapping]]] = ..., notes: _Optional[_Iterable[str]] = ...) -> None: ...
