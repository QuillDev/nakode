# Codex authentication and session recovery

## Confirmed code defects

- Startup loaded all durable logical sessions, but `NativeServerRuntime::refresh_catalogs` replaced that inventory with the newest 100 rows from only the server workspace. `OpenSession` searches that inventory. An excluded row therefore returned `NotFound` before the provider adapter was consulted. The SQLite row and provider continuation were not deleted by this path.
- Codex supervisors capture their inference credentials. Account sign-out stopped session supervisors; subsequent commands could reach a fresh supervisor without its native-session map. Reauthentication refreshed account/catalogue controls, not existing inference providers.
- `DomainState::provider_disabled` cleared provider/native identities and model selection. Authentication availability must not determine the identity of existing work.
- A logical session is not checkpointed until provider creation or its first prompt. A newly accepted, never-started session can disappear on restart. This separate persistence limitation is not changed here: early checkpointing needs to preserve first-prompt naming and explicit titles across restart.
- Account refresh failures ignored typed authentication classification, successful checks did not clear prior health failures, and the legacy dashboard row hid sign-in whenever credentials were configured. Subscription read failures silently retained apparently authoritative connected state.

These are source- and fixture-backed causes, not a diagnosis of the owner's live database. This checkout says “resource was not found”; the reported “entity not found” wording is from another build. No live credentials or session stores were inspected.

## Changed behavior

Catalogue refresh keeps the complete durable inventory. Session creation and first-prompt persistence retain their existing behavior.

Codex account credential changes update existing session supervisors in place. Subsequent inference uses the replacement credential while retaining native sessions and tool brokers. Sign-out cancels active Codex work and blocks authenticated execution without shutting down its session supervisor. Completion retains the ordinary native-session checkpoint path. Existing provider-control generation fences continue rejecting events from superseded authentication controls.

Codex provider disablement retains an established session's logical/native IDs, account affinity and selected model. Other providers retain their existing disablement behavior because their session adapters are still stopped. Refresh reports failed readiness, uses typed authentication failures for account health, and clears health on successful model discovery. Public workspace snapshots remain the credential-safe client contract; no frontend persistence access is introduced.

A missing native continuation reports a resume failure rather than claiming the logical session was deleted. No empty replacement session or transcript reconstruction is performed.

## Non-destructive recovery

After deploying the fixed runtime to the **original execution machine**, use public session discovery and open the original logical ID with its original authorized tool configuration. Authenticate the original provider account on that same machine, then retry the explicit operation. A lost in-memory inventory entry can recover from its existing durable row. A retained native continuation is loaded through Nakode's normal adapter store on restart.

Do not clear credentials, delete accounts/sessions, reset databases, re-enroll machines, or create replacement blank sessions as recovery steps. The owner must approve any live recovery or service activation separately.

If the logical row genuinely never existed (for example, an accepted session that has never started), or the original data directory/native continuation is absent, this fix cannot invent its history. Locating an approved backup or correcting the original service's data-directory configuration requires a separate recovery proposal. A hard process kill during an in-flight turn can retain only checkpoints already written; recovery does not promise reconstruction of uncheckpointed provider output.

## Validation

- Nakode, narrowed pre-publication patch: `cargo test --all-targets --all-features` — 979 passed (956 unit tests plus 23 integration tests); `cargo clippy --all-targets --all-features -- -D warnings` passed. Formatting and both repository diff-whitespace checks passed.
- Dashboard: focused provider/auth/resume suite — 68 passed; after the final button grouping, all 16 focused provider-auth tests passed again. Production build passed.
- Dashboard check exited successfully, but React Doctor still reported the unrelated unused `tests/dot-brand.electron.mjs` diagnostic. A post-build check also scanned generated bundles; those generated outputs were removed before checking again. No lint suppression was added.
- Gallery scenes exercise real machine selection. The corrected legacy Codex row displays Connected, Check and Sign in again on one aligned row without clipping. Account-scoped collapsed rows retain their existing disclosure behavior.
- A focused static investigation found and corrected non-Codex disable/re-enable regression risk by limiting adapter/identity preservation to Codex. Early creation checkpointing was deferred to preserve existing title behavior. The broader independent reviews did not complete (timeout/transport failure), so they are not counted as passed. No live account verification was performed.

## Integration limits

FStack consumes existing public Nakode snapshot/command types; no protocol schema bump is needed. Its runtime/SDK revision, lockfile and release metadata must pin the same published Nakode revision containing this fix. Merging source does not authorize manual live recovery, credential changes or service restarts.
