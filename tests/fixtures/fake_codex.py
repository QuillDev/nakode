#!/usr/bin/env python3
"""Deterministic JSONL fixture for the Codex app-server client tests."""

import json
import os
import stat
import sys
import tempfile
from pathlib import Path


def send(message):
    sys.stdout.write(json.dumps(message, separators=(",", ":")) + "\n")
    sys.stdout.flush()


def fail(request_id, message):
    send({"id": request_id, "error": {"code": -32602, "message": message}})


initialized = False
init_gate = None
if "--init-gate" in sys.argv:
    gate_index = sys.argv.index("--init-gate") + 1
    if gate_index >= len(sys.argv):
        sys.stderr.write("--init-gate requires a FIFO path\n")
        sys.exit(2)
    candidate = os.path.realpath(sys.argv[gate_index])
    temp_root = os.path.realpath(tempfile.gettempdir())
    if os.path.commonpath((candidate, temp_root)) != temp_root:
        sys.stderr.write("--init-gate must be inside the system temporary directory\n")
        sys.exit(2)
    try:
        if not stat.S_ISFIFO(os.stat(candidate).st_mode):
            sys.stderr.write("--init-gate must reference a FIFO\n")
            sys.exit(2)
    except OSError as error:
        sys.stderr.write(f"invalid initialization gate: {error}\n")
        sys.exit(2)
    init_gate = Path(candidate)
thread_id = "thread-fixture"
turn_id = "turn-fixture"

for raw_line in sys.stdin:
    try:
        message = json.loads(raw_line)
    except json.JSONDecodeError as error:
        sys.stderr.write(f"invalid JSON from client: {error}\n")
        sys.stderr.flush()
        continue
    method = message.get("method")
    request_id = message.get("id")

    if method == "initialize":
        params = message.get("params", {})
        capabilities = params.get("capabilities", {})
        client = params.get("clientInfo", {})
        if client.get("name") != "flock" or not capabilities.get("experimentalApi"):
            fail(request_id, "invalid initialization")
            continue
        if init_gate:
            try:
                with init_gate.open("rb") as gate:
                    gate.read(1)
            except OSError as error:
                sys.stderr.write(f"failed to wait on initialization gate: {error}\n")
                sys.exit(2)
        send(
            {
                "id": request_id,
                "result": {
                    "userAgent": "fake-codex/0.144.5",
                    "codexHome": "/tmp/fake-codex-home",
                    "platformFamily": "unix",
                    "platformOs": "test",
                },
            }
        )
    elif method == "initialized":
        initialized = True
    elif method == "model/list":
        if not initialized:
            fail(request_id, "initialized notification missing")
            continue
        send(
            {
                "id": request_id,
                "result": {
                    "data": [
                        {
                            "id": "fixture-model",
                            "model": "fixture-model",
                            "displayName": "Fixture Model",
                            "description": "Deterministic test model",
                            "isDefault": True,
                        }
                    ],
                    "nextCursor": None,
                },
            }
        )
    elif method == "thread/start":
        params = message.get("params", {})
        if (
            not params.get("cwd")
            or params.get("model") != "fixture-model"
            or "dynamicTools" in params
            or "developerInstructions" in params
            or "config" in params
            or "environments" in params
            or "selectedCapabilityRoots" in params
        ):
            fail(request_id, "thread/start fields do not match")
            continue
        send(
            {
                "id": request_id,
                "result": {
                    "thread": {"id": thread_id},
                    "model": "fixture-model",
                },
            }
        )
        send({"method": "thread/started", "params": {"thread": {"id": thread_id}}})
    elif method == "thread/resume":
        params = message.get("params", {})
        if (
            params.get("threadId") != thread_id
            or not params.get("cwd")
            or "dynamicTools" in params
            or "developerInstructions" in params
            or "config" in params
            or "environments" in params
            or "selectedCapabilityRoots" in params
        ):
            fail(request_id, "thread/resume fields do not match")
            continue
        send(
            {
                "id": request_id,
                "result": {
                    "thread": {
                        "id": thread_id,
                        "turns": [
                            {
                                "id": "turn-history",
                                "items": [
                                    {
                                        "type": "userMessage",
                                        "id": "user-history",
                                        "content": [{"type": "text", "text": "saved prompt"}],
                                    },
                                    {
                                        "type": "agentMessage",
                                        "id": "agent-history",
                                        "text": "saved response",
                                    },
                                ],
                            }
                        ],
                    },
                    "model": "fixture-model",
                },
            }
        )
    elif method == "thread/unsubscribe":
        if message.get("params", {}).get("threadId") != thread_id:
            fail(request_id, "thread/unsubscribe fields do not match")
            continue
        send({"id": request_id, "result": {"status": "notLoaded"}})
    elif method == "turn/start":
        params = message.get("params", {})
        inputs = params.get("input", [])
        if (
            params.get("threadId") != thread_id
            or not params.get("clientUserMessageId")
            or len(inputs) != 1
            or inputs[0].get("type") != "text"
        ):
            fail(request_id, "turn/start fields do not match")
            continue
        prompt = inputs[0].get("text")
        send(
            {
                "id": request_id,
                "result": {
                    "turn": {"id": turn_id, "status": "inProgress", "items": []}
                },
            }
        )
        send(
            {
                "method": "turn/started",
                "params": {"threadId": thread_id, "turn": {"id": turn_id}},
            }
        )
        if prompt == "hello fixture":
            send(
                {
                    "method": "item/started",
                    "params": {
                        "threadId": thread_id,
                        "turnId": turn_id,
                        "startedAtMs": 1,
                        "item": {
                            "type": "agentMessage",
                            "id": "assistant-fixture",
                            "text": "",
                            "phase": None,
                            "memoryCitation": None,
                        },
                    },
                }
            )
            for delta in ("hello ", "world"):
                send(
                    {
                        "method": "item/agentMessage/delta",
                        "params": {
                            "threadId": thread_id,
                            "turnId": turn_id,
                            "itemId": "assistant-fixture",
                            "delta": delta,
                        },
                    }
                )
            send(
                {
                    "method": "item/completed",
                    "params": {
                        "threadId": thread_id,
                        "turnId": turn_id,
                        "completedAtMs": 2,
                        "item": {
                            "type": "agentMessage",
                            "id": "assistant-fixture",
                            "text": "hello world",
                            "phase": None,
                            "memoryCitation": None,
                        },
                    },
                }
            )
            send(
                {
                    "jsonrpc": "2.0",
                    "id": "approval-fixture",
                    "method": "item/commandExecution/requestApproval",
                    "params": {
                        "threadId": thread_id,
                        "turnId": turn_id,
                        "itemId": "command-fixture",
                        "startedAtMs": 3,
                        "command": "cargo test",
                        "cwd": "/tmp",
                        "reason": "fixture approval",
                    },
                }
            )
        else:
            fail(request_id, "unexpected turn prompt")
    elif method == "turn/steer":
        params = message.get("params", {})
        if (
            params.get("threadId") != thread_id
            or params.get("expectedTurnId") != turn_id
            or not params.get("clientUserMessageId")
        ):
            fail(request_id, "turn/steer fields do not match")
            continue
        send({"id": request_id, "result": {"turnId": turn_id}})
    elif request_id == "approval-fixture" and method is None:
        if message.get("result", {}).get("decision") != "accept":
            sys.stderr.write("approval was not accepted\n")
            sys.exit(2)
        send(
            {
                "method": "turn/completed",
                "params": {
                    "threadId": thread_id,
                    "turn": {
                        "id": turn_id,
                        "status": "completed",
                        "items": [],
                        "error": None,
                    },
                },
            }
        )
    elif method == "turn/interrupt":
        send({"id": request_id, "result": {}})
    else:
        fail(request_id, "unexpected message")
