# Nakode

Nakode is the provider-neutral agent server and execution runtime. It owns canonical workspace and session state, provider adapters, tools, persistence, orchestration, approvals, credentials, process supervision, delegated runs, and resumability. Its public boundary is the versioned `nakode.v1` Protobuf/gRPC service, with reusable client behavior in `crates/nakode-sdk/`.

The server is the product authority. The built-in TUI and external frontends are replaceable clients that issue typed commands and render authoritative replacement snapshots; they do not own provider execution or lifecycle policy. Provider-specific behavior stays inside adapter modules.

FStack is a Nakode client: it may discover and operate logical sessions through the public protocol, but it must not read Nakode persistence, launch providers directly, address opaque provider resources, or duplicate Nakode orchestration. Work belongs here when it changes agent lifecycle semantics, provider integration, server state, the protocol/SDK, tools, persistence, or the built-in client projection.
