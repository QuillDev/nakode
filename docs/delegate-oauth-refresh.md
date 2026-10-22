# Native delegate credential lifecycle

Native delegate refresh events persist through the server credential store using the account captured at launch. Changing the default later does not reattribute refreshes or cooldowns. Unknown and stopped runs cannot publish credentials; explicitly unbound legacy handles retain the provider-credential path. Replacement and revocation reach only matching live delegates.

Codex startup refreshes share an account-scoped coordinator across control, primary, and delegate handles in one runtime. Simultaneous handles reuse successful rotations instead of presenting a spent refresh token again. Cached consumers also publish the rotation so persistence does not depend solely on the first handle surviving. Recent aliases follow subsequent rotations. Retained secret material is bounded to 32 entries per account; matching requires the exact credential and token endpoint. Coordinators never cross accounts or runtimes. Brokered credentials still never refresh locally.

This repairs established source defects, not proof of the cause of every historical 401. It cannot recover previously lost refresh tokens or revive revoked sign-ins. Those require the supported sign-in-again flow after updating, not retries with an unchanged invalid credential. No token values or protected lifecycle logs are required for diagnosis.

Synthetic coverage lives in `src/codex/native/tests/refresh.rs`, `src/server/runtime/tests/delegated_credentials.rs`, and `src/server/runtime/tests/delegated_auth.rs`. Live authentication and post-update delegate startup remain separate verification steps.

Same-provider native delegates inherit their parent session’s exact provider account. Cross-provider delegates use the target provider's configured default, independent of load. Both bindings fail explicitly when disabled, uncredentialed, or cooling down; they never fall back to a sibling. Only configurations without either binding retain the existing legacy routing behavior. Ordinary independent-session load balancing is unchanged.

## Brokered fresh-session versus delegate regression

FStack's `ProviderCredentialBroker.sessionAccount` supplies the exact synchronized cloud-default account when creating a durable session. Cloud archetype delegates previously called `BackendRegistry::select_account(provider, None)` for a different provider. Codex's least-load policy could select an old local OAuth account instead of the loaded, healthy brokered default. The adapter then attempted local refresh on that account, while a fresh explicitly bound session worked.

The native-path regression reproduces this difference with synthetic transport: a fresh brokered session starts successfully, then the unchanged cross-provider delegate path fails with `token refresh returned 401 Unauthorized`. With default binding, Claude→Codex and Codex→Codex child sessions complete a synthetic marker turn without an OAuth request. Tests cover cloud and local archetypes, unavailable defaults, expired brokered credentials, in-place replacement, revocation, account isolation, and credential-free diagnostics. The fixture transport and endpoint injection are test-only; no installed credentials or provider endpoints are used.

A genuine local OAuth refresh 401 still fails without retry or account substitution, with guidance to reconnect that account. Expired brokered credentials instead request a new access token from their broker. Neither failure proves another account is invalid.

This source-level reproduction does not establish which account a historical production failure selected. Production marker verification must use the normally released runtime and a newly authorized delegate invocation; synthetic marker output is not live authentication evidence.
