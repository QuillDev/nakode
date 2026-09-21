//! Conservative image-token estimates for the native adapter's omitted (`auto`) detail.
//!
//! Public API sizing rules: <https://platform.openai.com/docs/guides/images-vision>
//! These are estimates, not measured Codex usage. Keep model matching explicit: an unknown
//! Codex model must not silently inherit another model's image preprocessing or multiplier.

pub(crate) fn estimate(model: &str, width: u32, height: u32) -> Option<usize> {
    let model = model.strip_prefix("openai-codex/").unwrap_or(model);
    let model = without_snapshot(model);
    let (width, height) = (u64::from(width), u64::from(height));
    let tokens = match model {
        "gpt-6-astra" | "gpt-5.6-sol" | "gpt-5.6-terra" | "gpt-5.6-luna" => {
            // Original/auto does not shrink to the separate 30,000-patch rejection limit.
            // Do not clamp the estimate to that limit: estimating context is not input validation,
            // and the unchanged provider request can still be rejected for an oversized image.
            patches(width, height, 65_535, u64::MAX, 120)
        }
        "gpt-5.5" => patches(width, height, 6_000, 10_000, 120),
        "gpt-5.4" | "gpt-5.4-mini" | "gpt-5.4-nano" => patches(width, height, 2_048, 2_500, 120),
        "gpt-5.2" => patches(width, height, 2_048, 6_144, 120),
        "gpt-4.1-mini" => patches(width, height, 2_048, 6_144, 162),
        "gpt-5.1" => tiles(width, height, 70, 140),
        "gpt-4.1" | "gpt-4o" => tiles(width, height, 85, 170),
        "gpt-4o-mini" => tiles(width, height, 2_833, 5_667),
        _ => return None,
    };
    Some(usize::try_from(tokens).unwrap_or(usize::MAX))
}

fn without_snapshot(model: &str) -> &str {
    let Some((base, suffix)) = model.split_once("-20") else {
        return model;
    };
    let bytes = suffix.as_bytes();
    if bytes.len() == 8
        && bytes[2] == b'-'
        && bytes[5] == b'-'
        && bytes
            .iter()
            .enumerate()
            .all(|(index, byte)| matches!(index, 2 | 5) || byte.is_ascii_digit())
    {
        base
    } else {
        model
    }
}

fn fit(width: u64, height: u64, max_side: u64) -> (u64, u64) {
    let longest = width.max(height);
    if longest <= max_side {
        return (width, height);
    }
    // Round up rather than undercount a patch at a preprocessing boundary.
    (
        (width * max_side).div_ceil(longest),
        (height * max_side).div_ceil(longest),
    )
}

fn patches(width: u64, height: u64, max_side: u64, budget: u64, multiplier: u64) -> u64 {
    let (width, height) = fit(width, height, max_side);
    // When resizing is necessary, the documented budget is an upper bound on the resulting
    // patch count. Using the bound avoids pretending to reproduce provider rounding exactly.
    let count = (width.div_ceil(32) * height.div_ceil(32)).min(budget);
    (count * multiplier).div_ceil(100)
}

fn tiles(width: u64, height: u64, base: u64, per_tile: u64) -> u64 {
    let (mut width, mut height) = fit(width, height, 2_048);
    let shortest = width.min(height);
    if shortest > 768 {
        width = (width * 768).div_ceil(shortest);
        height = (height * 768).div_ceil(shortest);
    }
    base + width.div_ceil(512) * height.div_ceil(512) * per_tile
}

#[cfg(test)]
mod tests {
    use super::estimate;

    #[test]
    fn auto_detail_is_model_specific() {
        for (model, width, height, expected) in [
            ("gpt-6-astra", 1660, 646, 1311),
            ("gpt-6-astra", 1600, 623, 1200),
            ("gpt-6-astra", 1280, 498, 768),
            ("gpt-6-astra", 2560, 1440, 4320),
            ("gpt-5.6-sol", 2560, 1440, 4320),
            ("gpt-5.5", 4096, 4096, 12_000),
            ("gpt-5.4", 2560, 1440, 2765),
            ("gpt-5.4-mini", 4096, 4096, 3000),
            ("gpt-5.2", 1024, 1024, 1229),
            ("gpt-4.1-mini", 1024, 1024, 1659),
            ("gpt-4o", 2560, 1440, 1105),
            ("gpt-4.1", 1280, 720, 1105),
            ("gpt-5.1", 1280, 720, 910),
            ("gpt-4o-mini", 1280, 720, 36_835),
        ] {
            assert_eq!(estimate(model, width, height), Some(expected), "{model}");
        }
    }

    #[test]
    fn exact_families_and_dated_snapshots_do_not_guess_unknown_variants() {
        assert_eq!(estimate("openai-codex/gpt-6-astra", 32, 32), Some(2));
        assert_eq!(estimate("gpt-4o-2024-08-06", 32, 32), Some(255));
        for model in [
            "other/gpt-4o",
            "gpt-4o-custom",
            "gpt-4o-2024-08-06-extra",
            "gpt-4o-20xx-08-06",
            "gpt-5.4-codex",
            "future-model",
        ] {
            assert_eq!(estimate(model, 1024, 1024), None, "{model}");
        }
    }

    #[test]
    fn patch_boundaries_and_extreme_aspect_ratios_are_bounded() {
        assert_eq!(estimate("gpt-6-astra", 32, 32), Some(2));
        assert_eq!(estimate("gpt-6-astra", 33, 32), Some(3));
        assert_eq!(estimate("gpt-6-astra", 33, 33), Some(5));
        assert_eq!(estimate("gpt-5.4", 16_384, 1), Some(77));
        assert_eq!(estimate("gpt-4o", 16_384, 1), Some(765));
        // The 30,000-patch rejection limit is not a resizing budget.
        assert_eq!(estimate("gpt-6-astra", 6324, 6324), Some(47_045));
    }
}
