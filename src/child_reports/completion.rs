//! Bounded terminal-turn evidence. Never searches a previous turn for a replacement answer.
use serde::{Deserialize, Serialize};

pub(crate) const FINAL_BYTES: usize = 12 * 1024;

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct Completion {
    pub version: u32,
    pub turn_id: String,
    pub final_text: Option<String>,
    pub final_total_bytes: usize,
    pub truncated: bool,
}

impl Completion {
    pub(crate) fn capture(turn: &str, text: Option<&str>) -> Self {
        let text = text.filter(|text| !text.trim().is_empty());
        let total = text.map_or(0, str::len);
        let final_text = text.map(|text| {
            let mut encoded_bytes = 0;
            let mut end = 0;
            for character in text.chars() {
                let encoded = serde_json::to_string(&character.to_string())
                    .expect("string serialization")
                    .len()
                    - 2;
                if encoded_bytes + encoded > FINAL_BYTES {
                    break;
                }
                encoded_bytes += encoded;
                end += character.len_utf8();
            }
            text[..end].to_owned()
        });
        let truncated = final_text.as_ref().is_some_and(|text| text.len() < total);
        Self {
            version: 1,
            turn_id: turn.to_owned(),
            final_text,
            final_total_bytes: total,
            truncated,
        }
    }
}

pub(crate) fn display(body: &str) -> String {
    let Ok(completion) = serde_json::from_str::<Completion>(body) else {
        return body.to_owned();
    };
    let mut text = completion.final_text.unwrap_or_else(|| "No final assistant response was captured for this turn. Inspect the child transcript for activity and failure details.".to_owned());
    if completion.truncated {
        text.push_str(
            "\n\n[Response truncated; the complete response remains in the child transcript.]",
        );
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn final_is_utf8_bounded_and_empty_is_explicit() {
        let long = "🦀".repeat(5000);
        let final_message = Completion::capture("exact", Some(&long));
        assert_eq!(final_message.turn_id, "exact");
        assert_eq!(final_message.final_total_bytes, long.len());
        assert_eq!(
            final_message.final_text.as_ref().unwrap().len(),
            FINAL_BYTES
        );
        assert!(final_message.truncated);
        assert!(
            Completion::capture("empty", Some("  "))
                .final_text
                .is_none()
        );
    }
}
