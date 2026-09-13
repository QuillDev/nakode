# Explicit image handoffs

Images remain Nakode transcript artifacts. A reference authorizes no access by itself: public image reads resolve inside the addressed logical session; native tools and delegation resolve inside the initiating run. A child cannot select a sibling's or its parent's images unless they were explicitly attached to that child's task.

## Tools

User-image turns include an inert `Nakode Image References` block naming available artifacts. Select only relevant images; never forward every attachment automatically.

```json
{"image_reference":"<original reference>","crop":{"x":128,"y":96,"width":2048,"height":1024},"max_width":1024,"max_height":1024,"inspect":true}
```

Call `prepare_image` with that object. Coordinates are source pixels, with the origin at the top-left. Crop happens before downscaling. The result is 1024×512, not stretched to a square. Metadata includes a reusable `image_reference`, dimensions, MIME format, encoded bytes, and crop/source provenance. `inspect: true` attaches a preview to the ordinary assistant transcript for owner inspection; it does not claim that the invoking model receives another visual inference input. Omit transform fields to inspect an original or derived reference.

Then explicitly delegate:

```json
{"agent":"<configured slug>","title":"Read document detail","task":"Read the selected document region precisely.","image_references":["<returned reference>"]}
```

The images accompany the **first** child task through normal provider attachments. Runtime-owned session, parent run, turn and tool-call identities remain authoritative. Text-only calls omit `image_references` and retain existing behavior. Images and their transcript associations persist with delegated runs.

## Public API

`GetSessionImage(session_id, image_reference, transform?)` resolves an original or derived image through the existing ArtifactTransfer capability. Optional `ImageTransform` contains `ImageCrop` and `max_width`/`max_height`. The public SDK exposes `get_session_image` and `transform_image`; the returned artifact ID is reusable. `Delegate.image_references` and SDK `delegate_with_images` support explicit first-turn selection. `PromptAttachment::Artifact` also accepts a derived reference within its authorized source session.

Derived `image-v1:` references encode bounded recipes over originals, not image bytes or local paths. Clients should treat them as opaque, not construct internal JSON. Nested recipes are rejected: choose the original source for a new transform. Deleting the source session removes reference availability. Normal transcript inspection and artifact hydration remain the UI path.

## Limits and practical defaults

- PNG, JPEG, GIF and WebP originals: 1 byte–5 MiB each; at most eight images and 20 MiB per prompt/delegation/preview turn.
- At most 16,384 pixels on either side and 40 million pixels before transformation; decoder allocation budget 256 MiB. These are decoding bounds, not a process-wide memory guarantee.
- Format must match MIME type; the first frame must decode successfully. Originals retain their bytes and animation. Subsequent animation frames are not exhaustively validated by preparation.
- Transforms support static PNG/JPEG only. GIF/WebP and animated PNG transformations fail explicitly rather than flatten animation. Derived output is PNG with a streaming 5 MiB encoding bound.
- Crops must be nonempty and wholly inside the source; optional downscale bounds are 1–16,384 pixels. Aspect ratio is preserved, images never enlarge, and no transformation is implicit.
- There is no default resizing. For a broad overview, explicitly request a longest side around 1024; for dense text, keep the original or choose a focused crop first. Inspect the result before forwarding when legibility matters.
- Pixel and byte reductions are **not exact visual-token savings**. Provider accounting differs, and a smaller raster can even encode into more PNG bytes. No token-saving guarantee is made.
- Selected models must advertise image input. Image-bearing delegation does not retry through a text-only fallback. The Claude compatibility adapter currently refuses native image attachments (including at dispatch) instead of silently discarding them. Optional compatibility harnesses are not established to expose `prepare_image`; native-tool support is required.

## Fixture validation

`src/image_handoff/tests.rs` generates a dense 4096×3072 ruled document, crops and downscales it, verifies dimensions/provenance/original preservation, and covers invalid bounds, formats, malformed data and output limits. `server::tests::image_handoff_public_transform_reaches_a_new_agents_first_task` exercises that full fixture through the public query/command domain boundary: original prompt → typed transform → reusable derived query → new agent's first prompt → provider attachment and transcript artifact. State tests cover original and derived first-child-turn delivery, inaccessible references and restored transcript image visibility. Session repository tests persist actual PNG bytes. Runtime tests send nine preview calls through normal tool dispatch, refuse the ninth, retain provider attribution and serialized images, and permit a preview in the next turn. Server prompt tests retain image-only and artifact-backed recovery behavior. These fixtures do not establish a live-provider or installed-machine exchange.

Run the focused example and regressions with `cargo test --all-features image_handoff`. FStack's `image-handoffs.test.ts` covers ticket, stack aliases and general starts, and `agents-image-handoff` / `agents-image-handoff-phone` render selected originals/crops as session-scoped first-task artifact associations. UI assets are synthetic document stand-ins, not provider results or byte-for-byte evidence of Nakode's resampling algorithm.
