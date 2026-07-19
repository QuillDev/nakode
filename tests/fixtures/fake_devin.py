#!/usr/bin/env python3
"""Deterministic ACP fixture for the Devin backend adapter tests."""

import json
import sys


def send(message):
    sys.stdout.write(json.dumps(message, separators=(",", ":")) + "\n")
    sys.stdout.flush()


def result(request_id, value):
    send({"jsonrpc": "2.0", "id": request_id, "result": value})


def fail(request_id, message):
    send(
        {
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {"code": -32602, "message": message},
        }
    )


session_id = "devin-session-fixture"
current_model = "devin-fixture-model"
prompt_request_id = None
waiting_request_id = None


def config_options():
    return [
        {
            "id": "model",
            "name": "Model",
            "category": "model",
            "type": "select",
            "currentValue": current_model,
            "options": [
                {
                    "value": "devin-fixture-model",
                    "name": "Fixture Model",
                    "description": "Default fixture model",
                },
                {
                    "value": "devin-second-model",
                    "name": "Second Model",
                    "description": "Alternate fixture model",
                },
            ],
        }
    ]

for raw_line in sys.stdin:
    try:
        message = json.loads(raw_line)
    except json.JSONDecodeError as error:
        sys.stderr.write(f"invalid JSON from client: {error}\n")
        sys.stderr.flush()
        continue

    method = message.get("method")
    request_id = message.get("id")
    params = message.get("params", {})

    if method == "initialize":
        if params.get("protocolVersion") != 1:
            fail(request_id, "unsupported protocol version")
            continue
        client = params.get("clientInfo", {})
        if client.get("name") != "nako-agent":
            fail(request_id, "unexpected client")
            continue
        result(
            request_id,
            {
                "protocolVersion": 1,
                "agentCapabilities": {
                    "loadSession": True,
                    "sessionCapabilities": {"close": {}},
                    "mcpCapabilities": {"http": True},
                },
                "agentInfo": {
                    "name": "oh-my-pi",
                    "title": "Fake Devin",
                    "version": "1.0.0-fixture",
                },
                "authMethods": [],
            },
        )
    elif method == "session/new":
        if not params.get("cwd") or params.get("mcpServers") != []:
            fail(request_id, "invalid session/new")
            continue
        result(
            request_id,
            {"sessionId": session_id, "configOptions": config_options()},
        )
    elif method == "session/set_config_option":
        if (
            params.get("sessionId") != session_id
            or params.get("configId") != "model"
            or params.get("value")
            not in {"devin-fixture-model", "devin-second-model"}
        ):
            fail(request_id, "invalid session/set_config_option")
            continue
        current_model = params["value"]
        result(request_id, {"configOptions": config_options()})
    elif method == "session/load":
        if params.get("sessionId") != session_id or not params.get("cwd"):
            fail(request_id, "invalid session/load")
            continue
        for update in (
            {
                "sessionUpdate": "user_message_chunk",
                "messageId": "history-user",
                "content": {"type": "text", "text": "saved ACP prompt"},
            },
            {
                "sessionUpdate": "agent_message_chunk",
                "messageId": "history-agent",
                "content": {"type": "text", "text": "saved ACP response"},
            },
        ):
            send(
                {
                    "jsonrpc": "2.0",
                    "method": "session/update",
                    "params": {"sessionId": session_id, "update": update},
                }
            )
        result(request_id, {"configOptions": config_options()})
    elif method == "session/prompt":
        prompts = params.get("prompt", [])
        text = prompts[0].get("text") if prompts else None
        if params.get("sessionId") != session_id:
            fail(request_id, "invalid prompt session")
        elif text == "hello devin":
            prompt_request_id = request_id
            send(
                {
                    "jsonrpc": "2.0",
                    "method": "session/update",
                    "params": {
                        "sessionId": session_id,
                        "update": {
                            "sessionUpdate": "agent_message_chunk",
                            "messageId": "devin-agent-message",
                            "content": {"type": "text", "text": "hello "},
                        },
                    },
                }
            )
            send(
                {
                    "jsonrpc": "2.0",
                    "method": "session/update",
                    "params": {
                        "sessionId": session_id,
                        "update": {
                            "sessionUpdate": "agent_message_chunk",
                            "messageId": "devin-agent-message",
                            "content": {"type": "text", "text": "from Devin"},
                        },
                    },
                }
            )
            send(
                {
                    "jsonrpc": "2.0",
                    "method": "session/update",
                    "params": {
                        "sessionId": session_id,
                        "update": {
                            "sessionUpdate": "tool_call",
                            "toolCallId": "devin-tool",
                            "title": "Run tests",
                            "kind": "execute",
                            "status": "pending",
                            "rawInput": {"command": "cargo test"},
                        },
                    },
                }
            )
            send(
                {
                    "jsonrpc": "2.0",
                    "id": "devin-permission",
                    "method": "session/request_permission",
                    "params": {
                        "sessionId": session_id,
                        "toolCall": {
                            "toolCallId": "devin-tool",
                            "title": "Run tests",
                            "kind": "execute",
                            "rawInput": {"command": "cargo test"},
                        },
                        "options": [
                            {
                                "optionId": "allow-once",
                                "name": "Allow once",
                                "kind": "allow_once",
                            },
                            {
                                "optionId": "allow-always",
                                "name": "Always allow",
                                "kind": "allow_always",
                            },
                            {
                                "optionId": "reject-once",
                                "name": "Reject",
                                "kind": "reject_once",
                            },
                        ],
                    },
                }
            )
        elif text == "wait for cancel":
            waiting_request_id = request_id
        elif text == "fail prompt":
            fail(request_id, "fixture prompt failure")
        else:
            fail(request_id, "unexpected prompt")
    elif request_id == "devin-permission" and method is None:
        outcome = message.get("result", {}).get("outcome", {})
        if outcome.get("outcome") != "selected" or outcome.get("optionId") != "allow-always":
            sys.stderr.write("permission was not permanently accepted\n")
            sys.exit(2)
        send(
            {
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {
                    "sessionId": session_id,
                    "update": {
                        "sessionUpdate": "tool_call_update",
                        "toolCallId": "devin-tool",
                        "status": "completed",
                        "content": [
                            {
                                "type": "content",
                                "content": {"type": "text", "text": "tests passed"},
                            }
                        ],
                    },
                },
            }
        )
        result(prompt_request_id, {"stopReason": "end_turn"})
        prompt_request_id = None
    elif method == "session/cancel":
        if params.get("sessionId") != session_id or waiting_request_id is None:
            sys.stderr.write("invalid session/cancel\n")
            sys.exit(2)
        result(waiting_request_id, {"stopReason": "cancelled"})
        waiting_request_id = None
    elif method == "session/close":
        if params.get("sessionId") != session_id:
            fail(request_id, "invalid session/close")
            continue
        result(request_id, {})
    else:
        fail(request_id, f"unexpected message: {method}")
