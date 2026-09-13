use super::*;
use crate::domain_transcript::{DomainTranscript, EntryKind, EntryStatus};

pub(crate) fn png(width: u32, height: u32) -> Vec<u8> {
    let image = image::RgbImage::from_fn(width, height, |x, y| {
        // Dense ruled document with repeated glyph-like marks, not a blank low-detail image.
        if x % 128 == 0 || y % 48 == 0 || (y % 48 > 12 && y % 48 < 25 && x % 12 < 5) {
            image::Rgb([20, 35, 50])
        } else {
            image::Rgb([250, 250, 245])
        }
    });
    let mut bytes = Cursor::new(Vec::new());
    image
        .write_to(&mut bytes, ImageFormat::Png)
        .expect("fixture PNG");
    bytes.into_inner()
}

fn artifact(width: u32, height: u32) -> ArtifactView {
    let data = png(width, height);
    ArtifactView {
        id: ArtifactId::from("original"),
        label: "Detailed document".to_owned(),
        media_type: "image/png".to_owned(),
        byte_length: u64::try_from(data.len()).expect("fixture length"),
        data,
        width: None,
        height: None,
    }
}

#[test]
fn detailed_large_image_crop_downscale_preserves_original_and_aspect_ratio() {
    let original = artifact(4096, 3072);
    let recipe = Recipe {
        source: original.id.to_string(),
        crop: Some(Crop {
            x: 128,
            y: 96,
            width: 2048,
            height: 1024,
        }),
        max_width: Some(1024),
        max_height: Some(1024),
    };
    let derived = transform(original.clone(), &recipe).expect("crop and downscale");
    assert_eq!(dimensions(&original.data).unwrap(), (4096, 3072));
    assert_eq!(dimensions(&derived.data).unwrap(), (1024, 512));
    assert_eq!((derived.width, derived.height), (Some(1024), Some(512)));
    assert_eq!(derived.id.as_str(), recipe.reference().unwrap());
    assert!(derived.data.len() <= MAX_IMAGE_BYTES);
    assert_eq!(derived.media_type, "image/png");
    assert_eq!(
        Recipe::parse(derived.id.as_str())
            .unwrap()
            .unwrap()
            .crop
            .unwrap()
            .x,
        128
    );
}

#[test]
fn original_and_derived_references_are_session_scoped_and_reusable() {
    let mut transcript = DomainTranscript::new(100);
    transcript.upsert(
        "owner",
        EntryKind::User,
        "YOU",
        "Read this document",
        EntryStatus::Complete,
    );
    let original = artifact(64, 32);
    transcript.set_labeled_images(
        "owner",
        vec![(
            original.label.clone(),
            crate::media::ImageData {
                mime_type: original.media_type.clone(),
                data: original.data.clone(),
            },
        )],
    );
    let reference =
        crate::state::projection::transcript_artifact_id(&transcript.entries()[0].id, 0)
            .to_string();
    let resolved = resolve_transcript(&transcript, &reference).unwrap();
    assert_eq!(resolved.data, original.data);
    let recipe = Recipe {
        source: reference,
        crop: None,
        max_width: Some(32),
        max_height: None,
    };
    let reference = recipe.reference().unwrap();
    let one = resolve_transcript(&transcript, &reference).unwrap();
    assert_eq!(one, resolve_transcript(&transcript, &reference).unwrap());
    assert!(resolve_transcript(&DomainTranscript::new(100), &reference).is_err());
    assert_eq!(dimensions(&one.data).unwrap(), (32, 16));
}

#[test]
fn invalid_crop_and_downscale_bounds_fail_without_changing_source() {
    let original = artifact(32, 16);
    for crop in [
        Crop {
            x: 31,
            y: 0,
            width: 2,
            height: 1,
        },
        Crop {
            x: u32::MAX,
            y: 0,
            width: 2,
            height: 1,
        },
        Crop {
            x: 0,
            y: 0,
            width: 0,
            height: 1,
        },
    ] {
        let recipe = Recipe {
            source: "original".to_owned(),
            crop: Some(crop),
            max_width: None,
            max_height: None,
        };
        assert!(transform(original.clone(), &recipe).is_err());
    }
    for bound in [0, 16385, u32::MAX] {
        let recipe = Recipe {
            source: "original".to_owned(),
            crop: None,
            max_width: Some(bound),
            max_height: None,
        };
        assert!(transform(original.clone(), &recipe).is_err());
    }
    let recipe = Recipe {
        source: "original".to_owned(),
        crop: None,
        max_width: Some(128),
        max_height: Some(128),
    };
    assert_eq!(
        dimensions(&transform(original, &recipe).unwrap().data).unwrap(),
        (32, 16)
    );
}

#[test]
fn malformed_data_mime_mismatch_and_output_growth_fail_closed() {
    let valid = png(32, 16);
    assert!(validate_image(&valid, "image/png").is_ok());
    assert!(validate_image(&valid, "image/jpeg").is_err());
    assert!(validate_image(&valid[..valid.len() / 2], "image/png").is_err());
    let mut output = BoundedOutput(vec![0; MAX_IMAGE_BYTES - 1]);
    assert!(output.write_all(&[1, 2]).is_err());
    assert_eq!(output.0.len(), MAX_IMAGE_BYTES - 1);
}

#[test]
fn animated_format_transforms_fail_and_originals_remain_available() {
    for format in [ImageFormat::Gif, ImageFormat::WebP] {
        let mut bytes = Cursor::new(Vec::new());
        image::RgbImage::new(16, 8)
            .write_to(&mut bytes, format)
            .unwrap();
        let mut original = artifact(16, 8);
        original.data = bytes.into_inner();
        original.media_type = format.to_mime_type().to_owned();
        assert!(validate_image(&original.data, &original.media_type).is_ok());
        let recipe = Recipe {
            source: "original".to_owned(),
            crop: None,
            max_width: Some(8),
            max_height: None,
        };
        assert!(
            transform(original, &recipe)
                .unwrap_err()
                .contains("originals unchanged")
        );
    }
}

#[test]
fn unsupported_oversized_and_malformed_inputs_fail_closed() {
    assert!(dimensions(b"<svg/>").is_err());
    assert!(dimensions(&vec![0; MAX_IMAGE_BYTES + 1]).is_err());
    assert!(Recipe::parse("image-v1:!invalid!").is_err());
    let nested = Recipe {
        source: "image-v1:other".to_owned(),
        crop: None,
        max_width: Some(1),
        max_height: None,
    }
    .reference()
    .unwrap();
    assert!(Recipe::parse(&nested).is_err());
    assert!(dimensions(&png(16385, 1)).is_err());
}
