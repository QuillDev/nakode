"""Exercise the generated Python client against the native Rust server."""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

from nakode_sdk import NakodeClient


def run(binary: str) -> None:
    with tempfile.TemporaryDirectory() as workspace_raw:
        workspace = Path(workspace_raw)
        control = workspace / "control"
        environment = os.environ | {
            "NAKODE_CONTROL_DIR": str(control),
            "HOME": str(workspace / "home"),
            "XDG_DATA_HOME": str(workspace / "data"),
        }
        (workspace / "home").mkdir()
        (workspace / "data").mkdir()
        descriptor = subprocess.run(
            [binary, "--workspace", str(workspace), "service", "endpoint"],
            check=True,
            capture_output=True,
            text=True,
            env=environment,
        )
        endpoint = json.loads(descriptor.stdout)
        assert endpoint["transport"] == "grpc+unix"
        client = NakodeClient.connect_unix(endpoint["endpoint"])
        try:
            canonical_workspace = endpoint["workspace"]
            state = client.get_workspace(canonical_workspace)
            assert state.workspace_path == canonical_workspace
            watched_workspace = next(client.watch_workspace(state.workspace_id))
            assert watched_workspace.workspace_id == state.workspace_id
            _, initial_session_id = client.open_workspace_session(canonical_workspace)
            assert client.get_hydrated_session(initial_session_id).state.id == initial_session_id
            session_id = client.create_session(state.workspace_id, "Python conformance")
            session = client.get_session(session_id)
            assert session.id == session_id
            watched_session = next(client.watch_session(session_id))
            assert watched_session.id == session_id
            hydrated = client.get_hydrated_session(session_id)
            assert hydrated.state.id == session_id
            watched_hydrated = next(client.watch_hydrated_session(session_id))
            assert watched_hydrated.state.id == session_id
            assert client.open_session(session_id) == session_id
        finally:
            client.close()
            subprocess.run(
                [binary, "--workspace", str(workspace), "service", "shutdown"],
                check=True,
                env=environment,
            )


if __name__ == "__main__":
    run(sys.argv[1])
