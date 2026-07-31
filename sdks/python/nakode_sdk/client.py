"""High-level Python entrypoint over the generated Nakode API."""

from __future__ import annotations

import uuid
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import TypeVar

import grpc

from nakode.v1 import nakode_pb2, nakode_pb2_grpc

Response = TypeVar("Response")


@dataclass(frozen=True)
class HydratedSession:
    state: nakode_pb2.SessionState
    artifacts: dict[str, nakode_pb2.Artifact]


class NakodeClient:
    """Typed client whose mutations receive safe SDK-owned idempotency keys."""

    def __init__(self, channel: grpc.Channel) -> None:
        self._channel = channel
        self.stub = nakode_pb2_grpc.NakodeServiceStub(channel)

    @classmethod
    def connect_unix(cls, path: str) -> NakodeClient:
        # gRPC Core otherwise derives an HTTP/2 authority from the percent-
        # encoded socket path, which strict Rust HTTP/2 servers reject.
        channel = grpc.insecure_channel(
            f"unix://{path}",
            options=(("grpc.default_authority", "nakode.local"),),
        )
        return cls(channel)

    def close(self) -> None:
        self._channel.close()

    @staticmethod
    def mutation(expected_revision: int | None = None) -> nakode_pb2.MutationOptions:
        mutation = nakode_pb2.MutationOptions(idempotency_key=str(uuid.uuid4()))
        if expected_revision is not None:
            mutation.expected_revision = expected_revision
        return mutation

    @staticmethod
    def _mutate(
        call: Callable[[object], Response], request: object
    ) -> Response:
        """Retry one immutable request, preserving its idempotency key."""
        while True:
            try:
                return call(request)
            except grpc.RpcError as error:
                if error.code() not in (
                    grpc.StatusCode.UNAVAILABLE,
                    grpc.StatusCode.UNKNOWN,
                ):
                    raise
                time.sleep(0.1)

    def get_workspace(self, workspace: str) -> nakode_pb2.WorkspaceState:
        return self.stub.GetWorkspace(
            nakode_pb2.GetWorkspaceRequest(workspace=workspace)
        ).state

    def open_workspace_session(
        self, workspace: str, requested_session: str | None = None
    ) -> tuple[nakode_pb2.WorkspaceState, str]:
        state = self.get_workspace(workspace)
        if requested_session is not None:
            session_id = self.open_session(requested_session)
        elif state.sessions:
            session_id = self.open_session(state.sessions[0].id)
        else:
            session_id = self.create_session(state.workspace_id)
        return state, session_id

    def create_session(self, workspace_id: str, title: str | None = None) -> str:
        request = nakode_pb2.CreateSessionRequest(
            mutation=self.mutation(), workspace_id=workspace_id
        )
        if title is not None:
            request.title = title
        result = self._mutate(self.stub.CreateSession, request)
        if not result.HasField("resource_id"):
            raise RuntimeError("server omitted the created session identifier")
        return result.resource_id

    def open_session(self, session_id: str) -> str:
        request = nakode_pb2.OpenSessionRequest(
            mutation=self.mutation(), session_id=session_id
        )
        result = self._mutate(self.stub.OpenSession, request)
        if not result.HasField("resource_id"):
            raise RuntimeError("server omitted the opened session identifier")
        return result.resource_id

    def get_session(self, session_id: str) -> nakode_pb2.SessionState:
        return self.stub.GetSession(
            nakode_pb2.GetSessionRequest(session_id=session_id)
        ).state

    def get_hydrated_session(
        self, session_id: str, limit: int = 2_000
    ) -> HydratedSession:
        """Materialize bounded history, complete bodies, runs, and artifacts."""
        state = self.get_session(session_id)
        limit = max(1, limit)
        self._hydrate_transcript(
            nakode_pb2.TRANSCRIPT_OWNER_KIND_SESSION,
            state.id,
            state.transcript,
            limit,
        )
        while state.runs_has_earlier and len(state.runs) < limit:
            if not state.runs:
                break
            page = self.stub.ListRuns(
                nakode_pb2.ListRunsRequest(
                    session_id=state.id,
                    before_run_id=state.runs[0].id,
                    limit=max(1, limit - len(state.runs)),
                )
            )
            previous = len(state.runs)
            self._prepend_unique(state.runs, page.runs, limit)
            state.runs_has_earlier = page.has_earlier or len(state.runs) == limit
            if len(state.runs) == previous:
                break
        for run in state.runs:
            self._hydrate_transcript(
                nakode_pb2.TRANSCRIPT_OWNER_KIND_RUN,
                run.id,
                run.transcript,
                limit,
            )
        artifact_ids = {
            artifact_id
            for entry in state.transcript.entries
            for artifact_id in entry.artifact_ids
        }
        artifact_ids.update(
            artifact_id
            for run in state.runs
            for entry in run.transcript.entries
            for artifact_id in entry.artifact_ids
        )
        artifacts = {
            artifact_id: self.stub.GetArtifact(
                nakode_pb2.GetArtifactRequest(artifact_id=artifact_id)
            )
            for artifact_id in artifact_ids
        }
        return HydratedSession(state=state, artifacts=artifacts)

    def _hydrate_transcript(
        self,
        owner_kind: int,
        owner_id: str,
        transcript: nakode_pb2.TranscriptPage,
        limit: int,
    ) -> None:
        while transcript.has_earlier and len(transcript.entries) < limit:
            if not transcript.entries:
                break
            page = self.stub.GetTranscriptPage(
                nakode_pb2.GetTranscriptPageRequest(
                    owner_kind=owner_kind,
                    owner_id=owner_id,
                    before_entry_id=transcript.entries[0].id,
                    limit=max(1, limit - len(transcript.entries)),
                )
            )
            previous = len(transcript.entries)
            self._prepend_unique(transcript.entries, page.entries, limit)
            transcript.has_earlier = page.has_earlier or len(transcript.entries) == limit
            if len(transcript.entries) == previous:
                break
        for entry in transcript.entries:
            while entry.body_start_byte > 0:
                expected_end = entry.body_start_byte
                window = self.stub.GetTranscriptBodyWindow(
                    nakode_pb2.GetTranscriptBodyWindowRequest(
                        owner_kind=owner_kind,
                        owner_id=owner_id,
                        entry_id=entry.id,
                        before_byte=expected_end,
                        limit_bytes=256 * 1024,
                    )
                )
                returned_end = window.start_byte + len(window.body.encode("utf-8"))
                if (
                    window.entry_id != entry.id
                    or window.total_bytes != entry.body_total_bytes
                    or returned_end != expected_end
                    or window.start_byte >= expected_end
                ):
                    raise RuntimeError(f"non-contiguous transcript body {entry.id}")
                entry.body = window.body + entry.body
                entry.body_start_byte = window.start_byte

    @staticmethod
    def _prepend_unique(current: object, older: object, limit: int) -> None:
        combined = list(older)
        positions = {value.id: index for index, value in enumerate(combined)}
        for value in current:
            if value.id in positions:
                combined[positions[value.id]] = value
            else:
                combined.append(value)
        del current[:]
        current.extend(combined[-limit:])

    def send_prompt(
        self,
        session_id: str,
        prompt: nakode_pb2.PromptInput,
        expected_revision: int | None = None,
    ) -> nakode_pb2.MutationResult:
        request = nakode_pb2.SendPromptRequest(
            mutation=self.mutation(expected_revision),
            session_id=session_id,
            prompt=prompt,
        )
        return self._mutate(self.stub.SendPrompt, request)

    def watch_workspace(
        self, workspace_id: str
    ) -> Iterator[nakode_pb2.WorkspaceState]:
        after = None
        while True:
            request = nakode_pb2.WatchWorkspaceRequest(workspace_id=workspace_id)
            if after is not None:
                request.after.CopyFrom(after)
            try:
                for snapshot in self.stub.WatchWorkspace(request):
                    after = snapshot.cursor
                    yield snapshot.state
            except grpc.RpcError as error:
                if error.code() not in (
                    grpc.StatusCode.UNAVAILABLE,
                    grpc.StatusCode.UNKNOWN,
                ):
                    raise
                time.sleep(0.1)

    def watch_session(self, session_id: str) -> Iterator[nakode_pb2.SessionState]:
        after = None
        while True:
            request = nakode_pb2.WatchSessionRequest(session_id=session_id)
            if after is not None:
                request.after.CopyFrom(after)
            try:
                for snapshot in self.stub.WatchSession(request):
                    after = snapshot.cursor
                    yield snapshot.state
            except grpc.RpcError as error:
                if error.code() not in (
                    grpc.StatusCode.UNAVAILABLE,
                    grpc.StatusCode.UNKNOWN,
                ):
                    raise
                time.sleep(0.1)

    def watch_hydrated_session(
        self, session_id: str, limit: int = 2_000
    ) -> Iterator[HydratedSession]:
        for _state in self.watch_session(session_id):
            yield self.get_hydrated_session(session_id, limit)
