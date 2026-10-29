# Images on external tool results

A client that executes an external tool may return images with its result, for example a screenshot from a client-owned browser.

## Contract

`SubmitExternalToolResult` takes an optional `images` list of `ExternalToolResultImage { mime_type, data }`. The server advertises `ExternalToolResultImages` when it accepts them; the SDK's `submit_external_tool_result_with_images` checks that capability before sending a non-empty list. Text-only results are unchanged.

The server rejects the whole result as invalid when:

- it carries more than eight images;
- any image is larger than 5 MiB;
- an image's bytes are not PNG, JPEG, GIF or WebP, or do not match its declared `mime_type`.

## Retention

Accepted images become one returned transcript image for the call (`ImageReturned`, id `<turn>:image:<call>`, labels `Tool image N`), so clients and history see them like `return_image` output. The per-turn limits of `return_image` still apply: eight images and 20 MiB per turn, counted together. Past them the call keeps its images in order while they fit, and the text result names the kept and the dropped ones (`Kept 3 of this result's 5 images in the transcript, as Tool images 1–3; images 4–5 not kept …`, or `None of this result's 2 images were kept …` once the turn is full). The model still sees every image in that result; a dropped image is never stored, so nothing can retrieve it later. Delivery failure (the transcript receiver gone) appends `Its images were not attached: …` and keeps nothing.

Kept images are ordinary transcript artifacts that persist with the session and go when it is deleted:

- Owners and clients read them through session history, and by artifact id through `GetSessionImage`.
- A parent session, or a native delegated run inspecting its own run, lists them with `list_child_materials` (exact source session and optional run) and copies one into its own turn with `prepare_image { source, image_reference, inspect: true }`, under that turn's own image limits.
- The calling conversation cannot name them yet. Its artifact id is derived from the transcript entry that records the image, which is created after the tool result text is written, so the result carries labels (`Tool image N`) rather than a reference, and `prepare_image` without `source` needs a reference it does not have. Closing this gap needs the result to carry the artifact references, which means a deterministic entry id for returned images, or a self-scoped material listing.

The tool result text gains a short "attached to the assistant transcript" note when images are kept.

## Model visibility

- **Claude:** the images also reach the model as MCP image content in the tool result. The SDK keeps only the text of an MCP error result, so a failed result with images is delivered as a non-error result whose text begins "The tool call failed."; a failed result without images stays an error.
- **Native runtime providers that read images directly (Codex, Kimi, GLM):** the images are kept on the tool result in session history. Providers take images in user messages, not tool results, so each request carries them as one user message right after the run of tool results they came with, naming the calls in order. Context accounting counts them like prompt images, and compaction drops them with the rest of the compacted history.
- **Native runtime providers without direct image input (Devin):** with the vision add-on configured, its description of the images is appended to the tool result text; otherwise the text says the model cannot view them.

## Input validation on Claude

The Claude bridge converts each external tool's JSON Schema to zod. A `oneOf` or `anyOf` whose object branches share a required constant property (such as `action`) becomes a discriminated union, also as the items of an array, so a bad input reports only the matching branch's issue or the allowed values rather than every branch's issues.
