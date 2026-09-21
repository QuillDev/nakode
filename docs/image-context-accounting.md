# Image context estimates

Native context accounting separates **encoded bytes** from **estimated image tokens**.
For ordinary supported attachments, changing PNG compression or switching between PNG,
JPEG, GIF and WebP at the same dimensions does not change the image-token estimate.
Images are never resized, re-encoded or removed by estimation.

## Paths and behavior

- `runtime/context_estimate.rs` sums the existing text-byte heuristic (four bytes/token)
  separately from image estimates. Bounded header inspection reuses `image_handoff`;
  no pixel decoding or allocation of full images is needed.
- `backend::estimate_image_tokens` dispatches to adapter-owned model rules.
- `codex/image_tokens.rs` estimates omitted (`auto`) detail using the public
  [OpenAI image sizing rules](https://platform.openai.com/docs/guides/images-vision).
  Exact listed model families and dated snapshots are recognized; unknown variants do
  not inherit a family's rules through a broad prefix match. Native Codex currently
  omits `detail`. If that payload changes, update these estimates and tests together.
- Model-specific patch limits use an upper bound when provider resizing would apply;
  tile sizing rounds conservatively. These are **not exact billing predictions** and
  public API rules are not a verification of authenticated Codex endpoint behavior.
- Unknown model/provider combinations use unscaled 32×32 patches at two tokens/patch.
  This is an explicit safety-margin heuristic, not a universal provider formula or a
  guaranteed upper bound. Provider-specific mappings can be added at the adapter boundary.
- Corrupt, unsupported or out-of-policy attachments cannot use bounded dimension
  accounting. Their fallback is the greater of 32,768 and encoded bytes/4. This preserves
  the old conservative allowance for large legacy/internal images rather than silently
  discounting an attachment whose dimensions could not be safely inspected.

`RuntimeSession::estimated_context_tokens` drives context reporting and proactive
compaction. The same per-image logic drives the recent-history retention budget when
choosing the compaction cut point. Provider/model are read from the current session,
so restore or a model switch does not reuse a stale persisted estimate.

`estimated_context_bytes` and `InferenceMetric.input_bytes` still count encoded image
bytes plus the existing text representation; they are not vision-token estimates or an
exact serialized HTTP payload size. Provider-reported input/cached usage remains
separate in `InferenceMetric.usage`. It is not reclassified as an image-only measurement.

## Boundaries

This fix does not change provider payloads, upload limits, original retention, detail
selection, replay, compaction prompts, or provider-specific image input validation.
In particular, `original`/`auto` models that reject more than 30,000 patches do not
implicitly resize to that rejection limit; the estimate is not clamped to pretend
otherwise. The provider can still reject such inputs. No automatic downscaling is added.

Text remains a heuristic, tool-schema overhead is not newly incorporated, and opaque
provider state is still estimated as before. The existing request reserve and one-shot
provider-overflow recovery remain important, especially for unknown models. Prompt
caching may reduce repeated input billing but does not make active images free context.

## Regression coverage

Tests cover compression-independent estimates with different encoded file sizes,
supported image encodings, model-specific patch/tile boundaries, unknown model and
invalid/out-of-policy fallbacks, unchanged original bytes, session serialization and
model switches, repeated estimation versus explicitly duplicated attachments, false
compaction from multiple >300 KB images, legitimate large-text compaction, and corrected
recent-history retention. Existing compaction overflow and failure tests remain in place.

FStack pins its Nakode runtime revision. A local source fix does not update an installed
FStack runtime; publication and repinning are separate release work.
