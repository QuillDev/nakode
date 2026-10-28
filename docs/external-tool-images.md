# Images on external tool results

A client that executes an external tool may return images with its result, for example a screenshot from a client-owned browser.

## Contract

`SubmitExternalToolResult` takes an optional `images` list of `ExternalToolResultImage { mime_type, data }`. The server advertises `ExternalToolResultImages` when it accepts them; the SDK's `submit_external_tool_result_with_images` checks that capability before sending a non-empty list. Text-only results are unchanged.

The server rejects the whole result as invalid when:

- it carries more than eight images;
- any image is larger than 5 MiB;
- an image's bytes are not PNG, JPEG, GIF or WebP, or do not match its declared `mime_type`.

## Retention

Accepted images become one returned transcript image for the call (`ImageReturned`, id `<turn>:image:<call>`, labels `Tool image N`), so clients and history see them like `return_image` output. The per-turn limits of `return_image` still apply: eight images and 20 MiB per turn, counted together. Past them the images are not kept, and the text result says so; Claude still sees them in that result.

The tool result text gains a short "attached to the assistant transcript" note when images are kept.

## Model visibility

- **Claude:** the images also reach the model as MCP image content in the tool result.
- **Native runtime providers that read images directly (Codex, Kimi, GLM):** the images are kept on the tool result in session history. Providers take images in user messages, not tool results, so each request carries them as one user message right after the run of tool results they came with, naming the calls in order. Context accounting counts them like prompt images, and compaction drops them with the rest of the compacted history.
- **Native runtime providers without direct image input (Devin):** with the vision add-on configured, its description of the images is appended to the tool result text; otherwise the text says the model cannot view them.
