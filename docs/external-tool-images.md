# Images on external tool results

A client that executes an external tool may return images with its result, for example a screenshot from a client-owned browser.

## Contract

`SubmitExternalToolResult` takes an optional `images` list of `ExternalToolResultImage { mime_type, data }`. The server advertises `ExternalToolResultImages` when it accepts them; the SDK's `submit_external_tool_result_with_images` checks that capability before sending a non-empty list. Text-only results are unchanged.

The server rejects the whole result as invalid when:

- it carries more than eight images;
- any image is larger than 5 MiB;
- an image's bytes are not PNG, JPEG, GIF or WebP, or do not match its declared `mime_type`.

## Retention

Accepted images become one returned transcript image for the call (`ImageReturned`, id `<turn>:image:<call>`, labels `Tool image N`), so clients and history see them like `return_image` output. The per-turn limits of `return_image` still apply: eight images and 20 MiB per turn, counted together. Past them the call keeps its images in order while they fit, and the text result names the kept and the dropped ones (`Kept 3 of this result's 5 images in the transcript, as Tool images 1–3; images 4–5 not kept …`, or `None of this result's 2 images were kept …` once the turn is full). The model still sees every image in that result. A dropped image never becomes a transcript artifact, so no client or material tool can retrieve it; native runtimes that read images directly still carry it in the provider history the model is sent, until compaction (see below). On native runtimes a transcript delivery failure appends `Its images were not attached: …` and counts nothing against the turn; the Claude adapter counts the images before it sends them and reports no such failure.

Kept images are ordinary transcript artifacts that persist with the session and go when it is deleted:

- Owners and clients read them through session history, and by artifact id through `GetSessionImage`.
- On native runtimes whose tools include `list_child_materials` and `prepare_image`, a session lists its own retained images with `list_child_materials { source: { session_id: <its own session id> } }` (a delegated run adds its own `run_id`), and a parent lists a child's the same way; `prepare_image { source, image_reference, inspect: true }` then copies one into the current turn under that turn's own image limits.
- The Claude adapter does not expose `list_child_materials` or `prepare_image`, so a Claude session cannot list or re-attach its retained tool images itself; it has the labels (`Tool image N`) in the result text only. Closing that gap means exposing those two tools over the Claude bridge.

The tool result text gains a short "attached to the assistant transcript" note when images are kept.

## Model visibility

- **Claude:** the images also reach the model as MCP image content in the tool result. The SDK keeps only the text of an MCP error result, so a failed result with images is delivered as a non-error result whose text begins "The tool call failed."; a failed result without images stays an error.
- **Native runtime providers that read images directly (Codex, Kimi, GLM):** the images are kept on the tool result in session history. Providers take images in user messages, not tool results, so each request carries them as one user message right after the run of tool results they came with, naming the calls in order. Context accounting counts them like prompt images, and compaction drops them with the rest of the compacted history.
- **Native runtime providers without direct image input (Devin):** with the vision add-on configured, its description of the images is appended to the tool result text; otherwise the text says the model cannot view them.

## Input validation on Claude

The Claude bridge converts each external tool's JSON Schema to zod. A `oneOf` or `anyOf` whose object branches share a required constant property (such as `action`) becomes a discriminated union, also as the items of an array, so a bad input reports only the matching branch's issue or the allowed values rather than every branch's issues.
