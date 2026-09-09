# Native request tracing

`nakode-telemetry` supplies bounded OpenTelemetry OTLP/HTTP JSON export and W3C trace context.
The embedding application calls `init` before starting its async runtime, retains the provider,
and shuts it down when the process exits. Export is disabled unless explicitly initialized.
FStack supplies its existing authenticated device relay endpoint and credentials; Nakode never
requires a public collector or a separate collector secret.

SDK transports inject context on every gRPC call; service listeners extract it. RPC spans retain
ownership through response body consumption and watch cancellation. Reconnecting SDK tasks inherit
the initiating context without attaching a thread-local guard across an await. Names are restricted
to the checked public RPC catalogue. Errors record codes, not messages or request data.

Broker spans distinguish capacity admission from queued time. The runtime continues the request
context through execution and synchronous query handling. Session repository find/list reads record
combined lock and SQL duration. Detached provider turns and tool execution are outside this request
trace; this is not a CPU profiler. Long-lived watches finish their spans when their streams close.

Export uses a 512-span bounded queue, batches of 64, one-second flushes and a two-second HTTP timeout.
Collector failure drops diagnostics without failing the application request. No payloads, resource
paths, SQL, provider data or credentials are added to spans.

`cargo test -p nakode-sdk` includes an actual Unix gRPC test checking success and failure requests
have independent, fully linked client/server/broker traces. `cargo build -p nakode-sdk --example
tracing-server` builds a no-data gRPC peer for FStack's five-service HTTP integration test. That peer
accepts `FSTACK_TRACE_ENDPOINT`, `FSTACK_TRACE_AUTHORIZATION`, and `FSTACK_TRACE_FIXTURE_SOCKET`, returns
NotFound through a real request broker, and never starts a provider or opens session storage.
