# Child materials and execution locality

## Public contract

`ServerInfo.execution_location` and `GetSessionRouting` expose launcher-bound
`ExecutionMachine { authority, id }`, installation `server_id`, and `runtime_epoch`.
Installation identity, hostnames and filesystem paths do not establish machine
locality. Missing, malformed or differently scoped machine identities remain
**unknown**. These facts do not grant access or disclose endpoints/credentials.

Launchers supply `NAKODE_EXECUTION_MACHINE_AUTHORITY` and
`NAKODE_EXECUTION_MACHINE_ID` before service startup. Standalone launches without
those facts remain unknown. FStack supplies `fstack.host.v1` and its persisted Host
ID. Listener metadata advertises only the applicable material operations and
transport: direct Unix service or authenticated remote TLS. This is not yet a
routing catalogue for every command.

The SDK's `MaterialClient::bind` accepts already-authorized clients, compares exact
machine identities and rechecks the source session and service incarnation.
Same-machine calls require the direct client; no proxy fallback is permitted.
Remote calls require the authenticated client; no host switching follows failure.
Each material query fences the runtime epoch. An unsupported operation, stale
route, missing resource or transport error is not a successful empty result.
Nakode TLS initialization chooses ring when no process provider was installed;
this avoids the production dependency graph's ring/aws-lc ambiguity without
replacing an embedder's provider.

## Discovery and retrieval

The `ChildMaterials` and `ArtifactTransfer` capabilities enable:

- `ListChildMaterials`: exact parent + source session, optional exact native run,
  exclusive artifact cursor and page size 1–64. Metadata contains bounded labels,
  IDs, media types and byte lengths, never image bytes. Missing/removed cursors
  refuse rather than silently restarting a page.
- `GetChildMaterial`: exact source and image reference, optional bounded crop or
  downscale. The original must belong to that exact session/run transcript.
  Retrieval reuses Nakode's image handoff validation: PNG/JPEG/GIF/WebP, 5 MiB,
  40 megapixels, 16384 pixels per side; no enlargement. Not every original format
  supports transforms. Unavailable and oversized images fail explicitly.

Canonical persisted parent relationships, current governing profile/workspace
ownership and open parent state authorize every read. An unrelated child or a run
from another session is refused. Closed children remain readable as retained
evidence without reopening providers. A parent may inspect its own native runs.
Delegated tool callers may inspect only their own exact run, not acquire their
primary session's supervision authority.

Native `list_child_materials` binds the parent from the tool owner. Use its selected
reference with `prepare_image { source, image_reference, inspect: true }` to copy
validated bytes into the parent turn's existing returned-image/history path.
Returned metadata keeps the original scope; the image label includes child/task
attribution. The copied attachment can subsequently be read from parent history
without requiring continued access to the original. Revocation prevents new reads;
it does not erase previously authorized evidence.

Native returned images create a transcript row and a persistence checkpoint, so
run material discovery and retained recovery do not depend on a later text event.

## Explicit boundaries

- Relationships are **same runtime only**. A remote TLS client can retrieve those
  materials, but this does not support a parent on runtime A supervising a child
  on runtime B. No remote relationship registry or cross-host relay was added.
- Discovery covers canonical transcript images, **not filesystem Gallery**, text,
  SVG, HTML or arbitrary files. FStack's managed stack Gallery remains a separate
  public integration surface to extend, not a filesystem bypass.
- FStack's optional built-in allowlist and delivered prompt recognize the new
  tool; older runtimes omit it. The bundled FStack Nakode revision is unchanged,
  so sibling source changes are not shipping dashboard integration.
- Existing dashboard image rendering can consume copied parent artifacts, but no
  actual two-host retrieval-to-browser rendering E2E is claimed. UDS/TLS transport,
  canonical authorization, retained native-run images and parent returned-image
  persistence are tested as bounded seams.
- Parent continuation, report delivery and durable follow-up batching are outside
  this read-only capability. Reading evidence grants no mutation/approval authority.

Focused coverage lives in `src/server/runtime/tests/child_creation/materials*`,
`crates/nakode-sdk/src/materials/tests.rs` and the parent attachment regression in
`src/runtime.rs`.
