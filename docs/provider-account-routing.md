# Provider account credentials and routing

Status: implemented initial policy and verification record

## 1. Current architecture

### 1.1 Credential authority

Nakode stores one credential per provider in the protected application SQLite database. `provider_credentials.provider` is the primary key; `CredentialStore` reads, replaces, or deletes that provider-wide JSON secret. `SecretValue` redacts `Debug`, credential metadata projections contain only kind and timestamps, and the database/directory are restricted to mode `0600`/`0700` on Unix. The current self-contained store does not use a platform keychain or field-level encryption, so this change must not create a second secret path or project raw credential JSON.

At server startup, configured provider credentials are loaded into a provider-keyed in-memory map. The backend registry constructs one provider-control adapter and lazily constructs session adapters from that provider credential. OpenAI Codex OAuth credentials contain access token, refresh token, expiry, ChatGPT account ID, and optional email. Refresh is adapter-owned; a refreshed credential returns through a normalized authentication-completed event and the server persists it. Local logout deletes the credential; there is no upstream OpenAI revocation operation.

### 1.2 Providers and models

The durable `providers` table owns provider enablement. Model catalogues, model preferences/options, and filters are provider-scoped. Adapter construction and provider-specific authentication/model discovery remain in provider modules. Shared state consumes normalized backend events. Current model capability filtering establishes whether the provider/model can serve a request, but there is no account-level capability projection.

### 1.3 Sessions and resumability

A logical `sessions` row persists Nakode session ID, provider, opaque provider session ID, workspace/process roots, model/options, turn attribution, and skill authorization. It does not persist a credential/account identity. Session backend handles are keyed by `(session_id, provider)`, native runtime state by `(provider, provider_session_id)`, and provider-native history by provider/session identity. Resume reconstructs the persisted provider session and assumes the current provider-wide credential is the original credential. Replacing a provider credential can therefore make an existing provider conversation unsafe or impossible to resume.

### 1.4 Retries, health, telemetry, and logs

Native adapters perform bounded request retries. Codex/Kimi/GLM honor numeric `Retry-After` and avoid retrying after visible output. The server has a process-local 15-minute provider-wide cooldown based on normalized failure text; only subagent admission currently consults it. Successful turns clear it. Usage is retained in native runtime telemetry or normalized provider usage projections, not as a uniform account ledger. Diagnostics summarize errors; secrets must remain absent from errors, snapshots, transcripts, and tracing.

### 1.5 Public and built-in client surfaces

The versioned gRPC API currently exposes provider-only begin-authentication, set-credential, clear-credential, and reload mutations. The Rust SDK wraps those mutations with idempotency behavior. Workspace snapshots project provider status and safe credential metadata. The built-in TUI renders that projection and emits typed SDK actions; its logout action clears the provider-wide credential. It has no privileged credential access.

## 2. Proposed durable model

### 2.1 Account identity and metadata

Add `provider_accounts` with:

- opaque stable `account_id` (server-generated for new accounts; deterministic `legacy-<provider>` only for migration);
- owning `provider`;
- human-readable `label`;
- `enabled` and `is_default` flags (at most one default per provider);
- safe provider identity such as an email only when deliberately extracted by the adapter;
- credential kind and creation/update timestamps;
- adapter-declared routing mode: `automatic` or `explicit_only`.

The account ID and safe label are not credentials. They may appear in authoritative snapshots and diagnostics. Raw access tokens, refresh tokens, API keys, complete OAuth payloads, and credential JSON may not.

Add `provider_account_credentials`, keyed by `account_id`, in the same protected SQLite authority. Secret JSON remains accessible only through `CredentialStore` and `SecretValue`. Account metadata queries never join or deserialize secret JSON. Provider-specific validation, identity extraction, OAuth refresh, revocation support, and error classification remain adapter responsibilities.

### 2.2 Migration and compatibility

On repository open, migrate each legacy `provider_credentials` row transactionally:

1. create one enabled/default account with ID `legacy-<provider>` and label `Default`;
2. copy the credential kind/JSON/timestamp into `provider_account_credentials` without serializing it through logs;
3. backfill existing `sessions.account_id` for that provider;
4. retain the legacy table for forward compatibility but stop writing it after successful migration; removing a migrated `legacy-<provider>` account also deletes its source row so repository reopen cannot recreate it.

A single-account installation therefore keeps the same credential, provider enablement, default selection, session affinity, and first-session behavior without reauthentication. Reopening is idempotent. A legacy session whose provider had no credential remains unbound rather than receiving a fabricated account. Because its provider-native conversation may belong to any later-added account, Nakode never least-load routes that resume; it returns an actionable error requiring a new/restarted session with an explicit original-account selection.

### 2.3 Session affinity

Add nullable `account_id` to logical sessions and account attribution to owned/delegated native sessions where needed. Resolve an account exactly once before a new native session is created, then persist it with the logical session/provider transition. Session backend keys become `(session_id, provider, account_id)`. Resume must use the persisted account and must fail actionably if that account is disabled, removed, unauthenticated, cooling down, or no longer serves the requested model.

Changing a provider default, disabling an account, or adding a healthier account never mutates an established session's account. Nakode does not transparently retry an established session against another credential. A user must start/restart a new provider-native session or perform an explicit handoff. This preserves provider conversation ownership and opaque state.

## 3. Ephemeral routing model

Keep runtime routing state in memory, separate from durable account configuration:

- active logical/native session count per account;
- bounded health state with class, safe reason, and expiry;
- optional model scope when an adapter classifies a model-local failure.

No cooldown survives restart. Restart begins with unknown/healthy runtime state; expired state is discarded on access. This avoids stale persisted rate-limit guesses. Durable enablement/authentication/default/affinity do survive restart.

Provider adapters classify normalized failures as one of: account authentication, account quota, account rate limit (with optional `Retry-After`), provider-wide transient outage, model-wide/model-local unavailability, or session-local failure. The shared router applies only account-scoped classes to that account. Unclassified failures do not disable sibling accounts. Cooldowns are capped to a bounded duration; a valid `Retry-After` is honored within that cap. Successful work clears only the selected account's transient health state.

## 4. Deterministic admission policy

Routing is serialized by the authoritative server runtime so concurrent starts cannot observe and increment load independently.

For a new native session:

1. derive the provider from the provider-qualified model;
2. filter to accounts owned by that provider that are enabled, credentialed/authenticated, not in an active account cooldown, adapter-compatible with automatic routing, and able to serve the requested model/capabilities;
3. if an explicit account override is present, validate it with the same eligibility checks and choose it without fallback;
4. otherwise prefer the configured default only when it is tied on effective load; select by least active-session load, then stable account ID;
5. reserve/increment the selected account's load before releasing the runtime lock and before spawning the adapter;
6. persist affinity with native session creation; roll back the reservation if creation fails before persistence.

This is deterministic least-loaded routing, not random rotation. Diagnostics report account ID, safe label, and one of `explicit override`, `only eligible account`, `preferred account tie-break`, or `least loaded`.

If no account is eligible, return a structured diagnostic listing only safe account IDs/labels and reasons such as disabled, authentication required, cooling down for N seconds, model unsupported, or automatic routing unsupported. Never include provider response bodies or credential material.

## 5. Provider safety gate

Each adapter declares whether fresh-session automatic account routing is safe. Adapters without a reliable account-isolated credential/session boundary are `explicit_only`: Nakode still stores multiple accounts and accepts an explicit account override, but automatic selection returns an honest unsupported-routing diagnostic when ambiguity exists. No implementation rewrites provider state, shares opaque conversations across credentials, or uses quota/rate-limit failures to swap an in-flight session.

OpenAI Codex native sessions have an account-isolated bearer token plus ChatGPT account header and Nakode-owned normalized resume state. Automatic selection is permitted only for creation of a fresh provider-native session. Resume and every subsequent turn remain pinned. Compatibility/process adapters and external-login-marker adapters default to `explicit_only` until they can prove account-isolated construction.

## 6. Public contract and client projection

The protocol adds safe `ProviderAccount` state under each provider, account-management mutations (add/authenticate, set label, enable/disable, set default, remove/clear/reload), optional account overrides on session creation/open where semantically valid, and selected-account routing diagnostics on session state/summary. Ephemeral authentication state is projected on the exact account so concurrent challenges cannot overwrite or be misattributed to a sibling account; the provider-scoped authentication field remains only for backward-compatible provider-wide operations. SDK methods mirror those operations and retain mutation idempotency.

The built-in TUI renders authoritative account rows with safe labels/identity, enabled/default/health state, and continues to send credential operations through the SDK-backed provider commands, which now target the authoritative default account. Full account CRUD is available through the SDK/API. Credential entry remains masked and provider authentication remains adapter-driven. The client does not route accounts or inspect persistence.

## 7. Verification and supported-policy record

The initial implementation now provides:

- additive protocol, gRPC, SDK, and redacted snapshot types for account CRUD, authentication, credential lifecycle, explicit account selection, selected-account identity, routing reason, and health;
- protected per-account credential rows with deterministic legacy migration and durable session affinity;
- serialized deterministic least-load selection with default/account-ID tie-breaking and strict adapter safety gates (`openai-codex` automatic, other adapters explicit-only when multiple accounts are eligible);
- account-specific adapter construction, provider-control OAuth refresh persistence, local logout, and removal cleanup;
- strict resume affinity and actionable refusal of a mid-session account change;
- Codex-adapter failure classification for authentication, quota, rate limit, provider-wide, and model-wide failures, including bounded `Retry-After` account cooldowns;
- process-local health projections that reset to `unknown` after restart, plus safe selected-account routing diagnostics;
- built-in provider settings projection of safe account label/ID, enablement, default, routing mode, credential state, and health. Existing built-in login/logout continues to operate on the authoritative default account and does not overwrite sibling accounts.

Focused safe-fixture tests cover account CRUD/restart redaction, atomic and concurrent affinity binding, blocked removal while pinned, explicit selection, deterministic balancing, disabled/unauthenticated filtering, account-local cooldown isolation, provider/model-wide non-poisoning, bounded `Retry-After`, and normalized failure propagation. The two-account routing fixture demonstrates that successive live session reservations select different least-loaded accounts while the first session remains bound to its original account.

Current safety boundaries are deliberate:

- account capability eligibility is inherited from the provider adapter/model catalogue because the initial adapters do not expose account-varying capability catalogues;
- automatic routing is disabled for ambiguous multi-account providers unless the adapter declares account-isolated construction;
- logout is local credential revocation; no unsupported upstream OAuth revocation is implied;
- no automatic mid-session failover is attempted.

Verification commands executed during implementation:

```text
cargo fmt --all -- --check
cargo check --all-targets --all-features
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-features account
cargo test --locked --all-features provider_failure
cargo test --locked --all-features --package nakode-protocol
cargo test --locked --all-features --package nakode-sdk
cargo test --locked --all-features --package nakode-server
cargo run -- tui-eval --scenario tests/tui_scenarios/agent_smoke.jsonl
```

The full all-target suite was also started and completed the primary 761-test library pass without an observed failure, but the duplicate all-target execution exceeded the one-hour command budget; this is recorded honestly rather than represented as a completed full-suite run.
