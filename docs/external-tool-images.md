# Images on external tool results

A client that executes an external tool may return images with its result, for example a screenshot from a client-owned browser.

## Contract

`SubmitExternalToolResult` takes an optional `images` list of `ExternalToolResultImage { mime_type, data }`. The server advertises `ExternalToolResultImages` when it accepts them; the SDK's `submit_external_tool_result_with_images` checks that capability before sending a non-empty list. Text-only results are unchanged.

The server rejects the whole result as invalid when:

- it carries more than eight images;
- any image is larger than 5 MiB;
- an image's bytes are not PNG, JPEG, GIF or WebP, or do not match its declared `mime_type`.

## Retention

Accepted images become one returned transcript image for the call (`ImageReturned`, id `<turn>:image:<call>`, labels `Tool image N`), so clients and history see them like `return_image` output. The per-turn limits of `return_image` still apply: eight images and 20 MiB per turn, counted together. When a result would exceed them, the text result is still delivered with a note that its images were not attached.

The tool result text gains a short "attached to the assistant transcript" note when images are kept.

## Model visibility

- **Claude:** the images also reach the model as MCP image content in the tool result.
- **Native runtime providers:** tool results are text-only, so the model sees only the note. The images are retained for the owner, but the model cannot inspect them.
