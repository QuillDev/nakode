# Embeddable Nakode runtime

This headless Rust crate links the Nakode agent engine into an embedding application.
It excludes the standalone terminal frontend and clipboard integration.

Call `run(arguments, executable_prefix, build_revision)` from the application's runtime
subcommand inside a Tokio runtime. Child services, activation helpers, and confined
code-mode workers replay that executable prefix. Run the service in an independently
supervised process so restarting the containing application's host does not terminate agents.

The caller supplies the immutable source revision used to build the engine. Lifecycle
and API reports expose that revision. Installation and updates belong to the embedding
application; the standalone self-updater is disabled for embedded services.

Existing `NAKODE_HOME`, credentials, SQLite sessions, remote identity and sockets retain
their layout. Embedding does not copy or recreate persistent state.

The Required workflow validates both standalone and headless configurations on `dev`
and publishes an immutable `runtime-<commit>` GitHub release containing the complete
crate workspace and checksum provenance. This is Git-based crate distribution,
not a crates.io publication. Consumers pin the public repository's exact commit in
Cargo.toml and commit Cargo.lock.
