const TUI_SOURCES: [(&str, &str); 10] = [
    (
        "api_projection.rs",
        include_str!("../src/api_projection.rs"),
    ),
    ("app.rs", include_str!("../src/app.rs")),
    ("clipboard.rs", include_str!("../src/clipboard.rs")),
    ("herdr.rs", include_str!("../src/herdr.rs")),
    ("render.rs", include_str!("../src/render.rs")),
    (
        "terminal_image.rs",
        include_str!("../src/terminal_image.rs"),
    ),
    ("transcript.rs", include_str!("../src/transcript.rs")),
    ("tui_client.rs", include_str!("../src/tui_client.rs")),
    ("tui_input.rs", include_str!("../src/tui_input.rs")),
    ("tui_state.rs", include_str!("../src/tui_state.rs")),
];

const FRONTEND_CRATE_MANIFESTS: [(&str, &str); 1] = [(
    "nakode-sdk",
    include_str!("../crates/nakode-sdk/Cargo.toml"),
)];

const SERVER_DOMAIN_SOURCES: [(&str, &str); 9] = [
    (
        "domain_transcript.rs",
        include_str!("../src/domain_transcript.rs"),
    ),
    ("handoff.rs", include_str!("../src/handoff.rs")),
    ("runtime.rs", include_str!("../src/runtime.rs")),
    ("server.rs", include_str!("../src/server.rs")),
    (
        "server/runtime.rs",
        include_str!("../src/server/runtime.rs"),
    ),
    ("service.rs", include_str!("../src/service.rs")),
    ("session.rs", include_str!("../src/session.rs")),
    ("state.rs", include_str!("../src/state.rs")),
    (
        "state/projection.rs",
        include_str!("../src/state/projection.rs"),
    ),
];

#[test]
fn production_tui_cannot_import_server_owned_subsystems() {
    let forbidden = [
        "crate::backend",
        "crate::codex",
        "crate::credential",
        "crate::cursor",
        "crate::devin",
        "crate::glm",
        "crate::kimi",
        "crate::runtime",
        "crate::server",
        "crate::service",
        "crate::session",
        "crate::shell",
        "crate::state",
        "crate::tools",
        "BackendCommand",
        "BackendEvent",
        "DomainState",
        "ServiceEngine",
        "SessionRepository",
        "ShellProcesses",
        "SqliteSessionRepository",
        "ToolRegistry",
        "nakode_protocol::Command",
        "view::Command",
        "nakode_server",
    ];

    for (path, source) in TUI_SOURCES {
        for symbol in forbidden {
            assert!(
                !source.contains(symbol),
                "{path} crosses the thin-client boundary through {symbol}"
            );
        }
    }
}

#[test]
fn server_domain_cannot_import_tui_or_renderer_modules() {
    let forbidden = [
        "crate::app",
        "app::{",
        "crate::render",
        "render::{",
        "crate::transcript",
        "\n    transcript::{",
        "crate::tui_client",
        "tui_client::{",
        "crate::tui_input",
        "tui_input::{",
        "crate::tui_state",
        "tui_state::{",
        "crate::terminal_image",
        "crossterm",
        "ratatui",
        "LineTone",
        "ProjectedLine",
        "TuiState",
        "VisibleTranscript",
    ];

    for (path, source) in SERVER_DOMAIN_SOURCES {
        for symbol in forbidden {
            assert!(
                !source.contains(symbol),
                "{path} crosses the server boundary through {symbol}"
            );
        }
    }
}

#[test]
fn canonical_transcript_contains_no_renderer_state() {
    let source = include_str!("../src/domain_transcript.rs");
    for field in [
        "cache_width",
        "cache_revision",
        "expanded_tools",
        "image_previews_enabled",
        "show_all_tools",
        "ProjectedLine",
        "render_markdown",
    ] {
        assert!(
            !source.contains(field),
            "canonical transcript contains renderer concern {field}"
        );
    }
}

#[test]
fn diagnostics_cli_queries_the_native_server_instead_of_opening_persistence() {
    let source = include_str!("../src/diagnostics.rs");
    let cli = source
        .split_once("pub async fn run(")
        .and_then(|(_, remainder)| remainder.split_once("pub(crate) fn collect"))
        .map(|(run, _)| run)
        .expect("diagnostics CLI run function precedes the server collector");

    assert!(cli.contains("native_client::connect"));
    assert!(cli.contains("get_diagnostics"));
    for persistence_api in ["Connection::open", "SessionRepository", "rusqlite::"] {
        assert!(
            !cli.contains(persistence_api),
            "diagnostics CLI opens server persistence through {persistence_api}"
        );
    }
}

#[test]
fn reusable_frontend_crates_have_no_terminal_or_server_implementation_dependencies() {
    let forbidden = [
        "crossterm",
        "ratatui",
        "rusqlite",
        "portable-pty",
        "reqwest",
        "nakode-server",
    ];

    for (name, manifest) in FRONTEND_CRATE_MANIFESTS {
        let production_manifest = manifest
            .split_once("[dev-dependencies]")
            .map_or(manifest, |(production, _)| production);
        for dependency in forbidden {
            assert!(
                !production_manifest.contains(dependency),
                "{name} depends on forbidden implementation crate {dependency}"
            );
        }
    }
}
