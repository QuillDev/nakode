# Claude image input

Nakode normalizes Claude Agent SDK discovery in `src/claude/mod.rs::models_event`.
`ModelCapabilities.accepts_image_input` is persisted with the catalogue, defaults to
false for old cache entries, and reaches every client through the existing public
`ModelConfiguration.accepts_image_input` projection. Discovery refresh replaces old
capabilities; an unavailable or unknown selection remains fail-closed.

The SDK reports selectable `value` and optional `resolvedModel`, not input
modalities. The adapter recognizes the documented `claude-opus-5-5` contract and
SDK `opus` / `opus[1m]` aliases; an explicit resolved identity takes precedence.
Display names never establish support. Other unresolved model identities are not
globally enabled. This is a deliberately bounded provider-adapter mapping, not a
client-side model catalogue.

The pinned Agent SDK is 0.3.280 (bundled Claude Code 2.1.280), the documented
minimum for Opus 5.5. A machine-configured Claude Code executable still owns its
installation and must meet that minimum too.

Validated images cross the Rust bridge as base64 image content blocks. The SDK
receives an async iterable of `SDKUserMessage` with images in `message.content`,
plus a text block only when owner text is present. Text-only turns retain string
input. Session IDs, resume, permissions, process supervision and tools use the
same send path. Claude no longer replaces owner images with vision-add-on prose.
The public server retains native capability checks, MIME/content validation,
artifact authorization, eight-image, 5 MiB/image and 20 MiB/turn limits.

## Evidence and validation boundary

- [Opus 5.5](https://platform.claude.com/docs/en/models/opus-5-5/overview): exact ID and image support.
- [Claude Code model configuration](https://code.claude.com/docs/en/model-config): aliases and minimum version.
- [SDK user input](https://code.claude.com/docs/en/agent-sdk/user-input): images belong in `message.content`.
- A credential-free public `query(...).supportedModels()` observation using isolated
  SDK 0.3.280 returned `default` and `opus[1m]`, both resolving to
  `claude-opus-5-5[1m]`. No inference prompt or provider credential was supplied.
  These exact rows are regression inputs; discovery is not inference acceptance.
- Public npm tarball manifests identify SDK 0.3.268 as CLI 2.1.268 and SDK
  0.3.280 as CLI 2.1.280.

Rust tests cover discovery identities, alias resolution, legacy caches, generic
projection, refreshed selected-model admission and exact serialized bytes.
`src/claude/image_transport.test.mjs` executes the production send function with
an isolated synthetic SDK query and process lifecycle, covering image+text,
image-only, new/resumed sessions and unchanged text-only input. The Rust suite
runs this test too. These are mocked transport tests, not evidence of live
Anthropic inference acceptance. Deployment requires the updated embedded runtime,
compatible Claude Code, and a refreshed Nakode catalogue; no production session
or provider credential is changed by these tests.
