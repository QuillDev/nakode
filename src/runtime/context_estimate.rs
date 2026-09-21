//! Image context accounting is separate from encoded payload size.

use super::ConversationItem;
use crate::backend::PromptImage;

// Unknown models use unscaled 32px patches with a safety multiplier, NOT encoded bytes/4.
// This is a heuristic, not a provider guarantee; the request reserve and overflow recovery
// remain necessary. Invalid/unsupported headers retain a conservative nonzero allowance.
const UNKNOWN_PATCH_TOKENS: u64 = 2;
const UNREADABLE_IMAGE_TOKENS: usize = 32_768;

pub(super) fn history_tokens(
    instructions: &str,
    history: &[ConversationItem],
    provider: &str,
    model: &str,
) -> usize {
    let text = history
        .iter()
        .map(text_bytes)
        .fold(instructions.len(), usize::saturating_add);
    let images = history
        .iter()
        .map(|item| item_image_tokens(item, provider, model))
        .fold(0, usize::saturating_add);
    text.div_ceil(4).saturating_add(images)
}

pub(super) fn item_tokens(item: &ConversationItem, provider: &str, model: &str) -> usize {
    text_bytes(item)
        .div_ceil(4)
        .saturating_add(item_image_tokens(item, provider, model))
}

fn item_image_tokens(item: &ConversationItem, provider: &str, model: &str) -> usize {
    let ConversationItem::User { attachments, .. } = item else {
        return 0;
    };
    attachments
        .iter()
        .filter_map(|attachment| attachment.image.as_ref())
        .map(|image| image_tokens(image, provider, model))
        .fold(0, usize::saturating_add)
}

fn image_tokens(image: &PromptImage, provider: &str, model: &str) -> usize {
    // Bounded header inspection only: no pixel decoding, resampling, or mutation of originals.
    let Ok((width, height)) = crate::image_handoff::dimensions(&image.data) else {
        // Do not lower the old allowance for corrupt or out-of-policy legacy attachments.
        // They are not valid candidates for dimension-based accounting.
        return UNREADABLE_IMAGE_TOKENS.max(image.data.len().div_ceil(4));
    };
    crate::backend::estimate_image_tokens(provider, model, width, height).unwrap_or_else(|| {
        let patches = u64::from(width).div_ceil(32) * u64::from(height).div_ceil(32);
        usize::try_from(patches * UNKNOWN_PATCH_TOKENS).unwrap_or(usize::MAX)
    })
}

pub(super) fn text_bytes(item: &ConversationItem) -> usize {
    match item {
        ConversationItem::User { text, .. } => text.len(),
        ConversationItem::Assistant {
            text,
            reasoning,
            tool_calls,
            provider_state,
            ..
        } => text
            .len()
            .saturating_add(reasoning.len())
            .saturating_add(
                tool_calls
                    .iter()
                    .map(|call| {
                        call.name
                            .len()
                            .saturating_add(call.arguments.to_string().len())
                    })
                    .fold(0, usize::saturating_add),
            )
            .saturating_add(
                provider_state
                    .iter()
                    .map(|state| state.to_string().len())
                    .fold(0, usize::saturating_add),
            ),
        ConversationItem::ToolResult {
            output,
            model_output,
            ..
        } => model_output.as_deref().unwrap_or(output).len(),
        ConversationItem::Compaction { summary } => summary.len(),
        ConversationItem::CompactionEvent { .. } => 0,
    }
}

#[cfg(test)]
mod tests {
    use image::{
        ImageEncoder,
        codecs::png::{CompressionType, FilterType, PngEncoder},
    };

    use super::*;
    use crate::backend::{CODEX_PROVIDER, PromptAttachment};
    use crate::runtime::RuntimeSession;

    fn screenshot(compression: CompressionType) -> PromptAttachment {
        let pixels = image::RgbImage::from_pixel(1600, 900, image::Rgb([24, 30, 42]));
        let mut data = Vec::new();
        PngEncoder::new_with_quality(&mut data, compression, FilterType::NoFilter)
            .write_image(pixels.as_raw(), 1600, 900, image::ExtendedColorType::Rgb8)
            .expect("encode screenshot");
        PromptAttachment {
            label: "screenshot.png".to_owned(),
            path: None,
            image: Some(PromptImage {
                mime_type: "image/png".to_owned(),
                data,
            }),
        }
    }

    fn session(attachment: PromptAttachment) -> RuntimeSession {
        let mut session = RuntimeSession::new("gpt-6-astra".to_owned(), "abcd".to_owned());
        session.provider_id = CODEX_PROVIDER.to_owned();
        session.history.push(ConversationItem::User {
            text: "efgh".to_owned(),
            attachments: vec![attachment],
        });
        session
    }

    #[test]
    fn file_compression_changes_bytes_but_not_context_or_originals() {
        let fast = screenshot(CompressionType::Fast);
        let best = screenshot(CompressionType::Best);
        let fast_data = fast.image.as_ref().unwrap().data.clone();
        let best_data = best.image.as_ref().unwrap().data.clone();
        assert_ne!(fast_data.len(), best_data.len());
        assert_eq!(
            image::load_from_memory(&fast_data).unwrap(),
            image::load_from_memory(&best_data).unwrap()
        );
        let fast_session = session(fast);
        let best_session = session(best);
        assert_ne!(
            fast_session.estimated_context_bytes(),
            best_session.estimated_context_bytes()
        );
        for (session, original) in [(&fast_session, &fast_data), (&best_session, &best_data)] {
            assert_eq!(session.estimated_context_tokens(), 1742);
            assert_eq!(
                item_tokens(&session.history[0], CODEX_PROVIDER, "gpt-6-astra"),
                1741
            );
            let ConversationItem::User { attachments, .. } = &session.history[0] else {
                panic!("user")
            };
            assert_eq!(&attachments[0].image.as_ref().unwrap().data, original);
        }
    }

    #[test]
    fn unknown_models_and_bad_headers_have_explicit_nonzero_fallbacks() {
        let attachment = screenshot(CompressionType::Fast);
        let image = attachment.image.as_ref().unwrap();
        assert_eq!(image_tokens(image, "unknown", "gpt-6-astra"), 2900);
        assert_eq!(image_tokens(image, CODEX_PROVIDER, "unlisted-model"), 2900);
        for (data, expected) in [
            (Vec::new(), UNREADABLE_IMAGE_TOKENS),
            (vec![0; 300_000], 75_000),
        ] {
            assert_eq!(
                image_tokens(
                    &PromptImage {
                        mime_type: "image/png".to_owned(),
                        data
                    },
                    CODEX_PROVIDER,
                    "gpt-6-astra"
                ),
                expected
            );
        }
    }

    #[test]
    fn out_of_policy_legacy_images_do_not_get_a_smaller_fallback_allowance() {
        let mut attachment = screenshot(CompressionType::Fast);
        let image = attachment.image.as_mut().unwrap();
        image
            .data
            .resize(crate::image_handoff::MAX_IMAGE_BYTES + 1, 0);
        assert!(crate::image_handoff::dimensions(&image.data).is_err());
        assert_eq!(
            image_tokens(image, CODEX_PROVIDER, "gpt-6-astra"),
            image.data.len().div_ceil(4)
        );
    }

    #[test]
    fn supported_encodings_with_the_same_dimensions_get_the_same_image_estimate() {
        let pixels = image::DynamicImage::ImageRgb8(image::RgbImage::from_pixel(
            64,
            64,
            image::Rgb([24, 30, 42]),
        ));
        for format in [
            image::ImageFormat::Png,
            image::ImageFormat::Jpeg,
            image::ImageFormat::Gif,
            image::ImageFormat::WebP,
        ] {
            let mut output = std::io::Cursor::new(Vec::new());
            pixels.write_to(&mut output, format).expect("encode image");
            let image = PromptImage {
                mime_type: format.to_mime_type().to_owned(),
                data: output.into_inner(),
            };
            assert_eq!(
                image_tokens(&image, CODEX_PROVIDER, "gpt-6-astra"),
                5,
                "{format:?}"
            );
        }
    }

    #[test]
    fn restored_sessions_and_model_switches_reestimate_without_persisted_policy() {
        let session = session(screenshot(CompressionType::Fast));
        let mut restored: RuntimeSession =
            serde_json::from_str(&serde_json::to_string(&session).unwrap()).unwrap();
        assert_eq!(
            restored.estimated_context_tokens(),
            session.estimated_context_tokens()
        );
        restored.model = "gpt-4o".to_owned();
        assert_eq!(restored.estimated_context_tokens(), 1107);
        restored.provider_id = "unknown".to_owned();
        assert_eq!(restored.estimated_context_tokens(), 2902);
    }

    fn large_screenshot() -> PromptAttachment {
        let mut state = 0x1234_5678_u32;
        let pixels = image::RgbImage::from_fn(400, 300, |_, _| {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            let bytes = state.to_le_bytes();
            image::Rgb([bytes[0], bytes[1], bytes[2]])
        });
        let mut data = Vec::new();
        PngEncoder::new(&mut data)
            .write_image(pixels.as_raw(), 400, 300, image::ExtendedColorType::Rgb8)
            .expect("encode incompressible fixture");
        assert!(data.len() > 300_000);
        PromptAttachment {
            label: "large.png".to_owned(),
            path: None,
            image: Some(PromptImage {
                mime_type: "image/png".to_owned(),
                data,
            }),
        }
    }

    #[test]
    fn large_encoded_files_do_not_trigger_false_compaction_but_large_text_still_does() {
        let attachment = large_screenshot();
        let mut session = session(attachment.clone()).with_context_window(Some(272_000));
        if let ConversationItem::User { attachments, .. } = &mut session.history[0] {
            attachments.extend([attachment.clone(), attachment.clone(), attachment]);
        }
        assert!(session.estimated_context_bytes().div_ceil(4) > 272_000);
        assert_eq!(session.estimated_context_tokens(), 626);
        assert!(!session.should_compact(crate::runtime::DEFAULT_COMPACTION_THRESHOLD_PERCENT));
        assert!(!session.exceeds_safe_request_budget());
        session.history.push(ConversationItem::User {
            text: "x".repeat(1_000_000),
            attachments: Vec::new(),
        });
        assert!(session.should_compact(crate::runtime::DEFAULT_COMPACTION_THRESHOLD_PERCENT));
        assert!(session.exceeds_safe_request_budget());
    }

    #[test]
    fn recent_history_retention_counts_images_by_tokens_not_compressed_file_size() {
        let mut session = session(large_screenshot());
        session.history.insert(
            0,
            ConversationItem::User {
                text: "x".repeat(80_000),
                attachments: Vec::new(),
            },
        );
        session.history.insert(
            1,
            ConversationItem::User {
                text: "x".repeat(40_000),
                attachments: Vec::new(),
            },
        );
        session.history.insert(
            2,
            ConversationItem::User {
                text: "x".repeat(40_000),
                attachments: Vec::new(),
            },
        );
        assert_eq!(
            crate::runtime::compaction_cut_index(
                &session.history,
                &session.provider_id,
                &session.model
            ),
            Some(1)
        );
        // The old byte-based path would stop on the final image alone and keep only index 3.
        assert!(super::super::estimate_item_bytes(&session.history[3]).div_ceil(4) > 20_000);
    }

    #[test]
    fn repeated_estimation_does_not_reappend_images_but_explicit_copies_add_up() {
        let attachment = screenshot(CompressionType::Fast);
        let mut session = session(attachment.clone());
        let before = serde_json::to_string(&session).unwrap();
        for _ in 0..3 {
            assert_eq!(session.estimated_context_tokens(), 1742);
        }
        assert_eq!(serde_json::to_string(&session).unwrap(), before);
        if let ConversationItem::User { attachments, .. } = &mut session.history[0] {
            attachments.push(attachment);
        }
        assert_eq!(session.estimated_context_tokens(), 3482);
    }
}
