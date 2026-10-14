# Native delegate credential lifecycle

Native delegate refresh events persist through the server credential store using the account captured at launch. Changing the default later does not reattribute refreshes or cooldowns. Unknown and stopped runs cannot publish credentials; explicitly unbound legacy handles retain the provider-credential path. Replacement and revocation reach only matching live delegates.

Codex startup refreshes share an account-scoped coordinator across control, primary, and delegate handles in one runtime. Simultaneous handles reuse successful rotations instead of presenting a spent refresh token again. Cached consumers also publish the rotation so persistence does not depend solely on the first handle surviving. Recent aliases follow subsequent rotations. Retained secret material is bounded to 32 entries per account; matching requires the exact credential and token endpoint. Coordinators never cross accounts or runtimes. Brokered credentials still never refresh locally.

This repairs established source defects, not proof of the cause of every historical 401. It cannot recover previously lost refresh tokens or revive revoked sign-ins. Those require the supported sign-in-again flow after updating, not retries with an unchanged invalid credential. No token values or protected lifecycle logs are required for diagnosis.

Synthetic coverage lives in `src/codex/native/tests/refresh.rs` and `src/server/runtime/tests/delegated_credentials.rs`. Live authentication and post-update delegate startup remain separate verification steps.
