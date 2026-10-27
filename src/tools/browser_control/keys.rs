//! Key chords such as `Enter`, `ctrl+a` or `shift+ArrowLeft`, as `DevTools` key events.

const ALT: u32 = 1;
const CONTROL: u32 = 2;
const META: u32 = 4;
const SHIFT: u32 = 8;

const MAX_CHORDS: usize = 16;

/// One key press: the `Input.dispatchKeyEvent` fields for its down and up events.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Press {
    pub key: String,
    pub code: String,
    pub key_code: u32,
    pub modifiers: u32,
    /// Text the press inserts; absent for named keys and while ctrl/alt/meta is held.
    pub text: Option<String>,
}

/// Named keys: name, DOM `key`, DOM `code`, Windows key code, inserted text.
const NAMED: &[(&str, &str, &str, u32, Option<&str>)] = &[
    ("enter", "Enter", "Enter", 13, Some("\r")),
    ("return", "Enter", "Enter", 13, Some("\r")),
    ("tab", "Tab", "Tab", 9, None),
    ("escape", "Escape", "Escape", 27, None),
    ("esc", "Escape", "Escape", 27, None),
    ("backspace", "Backspace", "Backspace", 8, None),
    ("delete", "Delete", "Delete", 46, None),
    ("space", " ", "Space", 32, Some(" ")),
    ("arrowleft", "ArrowLeft", "ArrowLeft", 37, None),
    ("arrowup", "ArrowUp", "ArrowUp", 38, None),
    ("arrowright", "ArrowRight", "ArrowRight", 39, None),
    ("arrowdown", "ArrowDown", "ArrowDown", 40, None),
    ("left", "ArrowLeft", "ArrowLeft", 37, None),
    ("up", "ArrowUp", "ArrowUp", 38, None),
    ("right", "ArrowRight", "ArrowRight", 39, None),
    ("down", "ArrowDown", "ArrowDown", 40, None),
    ("home", "Home", "Home", 36, None),
    ("end", "End", "End", 35, None),
    ("pageup", "PageUp", "PageUp", 33, None),
    ("pagedown", "PageDown", "PageDown", 34, None),
];

/// Parses space-separated chords into presses, refusing anything it does not know.
pub(crate) fn parse(keys: &str) -> Result<Vec<Press>, String> {
    let chords: Vec<&str> = keys.split_whitespace().collect();
    if chords.is_empty() {
        return Err("`keys` must name at least one key, such as Enter or ctrl+a".to_owned());
    }
    if chords.len() > MAX_CHORDS {
        return Err(format!("press at most {MAX_CHORDS} chords at a time"));
    }
    chords.into_iter().map(chord).collect()
}

fn chord(chord: &str) -> Result<Press, String> {
    let mut parts: Vec<&str> = chord.split('+').collect();
    let key = parts.pop().filter(|key| !key.is_empty());
    let Some(key) = key else {
        return Err(format!("{chord:?} does not end in a key"));
    };
    let mut modifiers = 0;
    for part in parts {
        modifiers |= match part.to_ascii_lowercase().as_str() {
            "alt" | "option" => ALT,
            "ctrl" | "control" => CONTROL,
            "meta" | "cmd" | "command" | "super" => META,
            "shift" => SHIFT,
            _ => return Err(format!("unknown modifier {part:?} in {chord:?}")),
        };
    }
    let lower = key.to_ascii_lowercase();
    if let Some((_, name, code, key_code, text)) =
        NAMED.iter().find(|(name, ..)| *name == lower.as_str())
    {
        return Ok(Press {
            key: (*name).to_owned(),
            code: (*code).to_owned(),
            key_code: *key_code,
            modifiers,
            text: text
                .filter(|_| modifiers & (ALT | CONTROL | META) == 0)
                .map(str::to_owned),
        });
    }
    let mut characters = key.chars();
    let (Some(character), None) = (characters.next(), characters.next()) else {
        return Err(format!("unknown key {key:?}"));
    };
    if !character.is_ascii_alphanumeric() {
        return Err(format!("unknown key {key:?}; type text with action type"));
    }
    let upper = character.to_ascii_uppercase();
    let code = if character.is_ascii_digit() {
        format!("Digit{character}")
    } else {
        format!("Key{upper}")
    };
    let shifted = modifiers & SHIFT != 0 || character.is_ascii_uppercase();
    let produced = if shifted {
        upper
    } else {
        character.to_ascii_lowercase()
    };
    Ok(Press {
        key: produced.to_string(),
        code,
        key_code: u32::from(upper),
        modifiers: if character.is_ascii_uppercase() {
            modifiers | SHIFT
        } else {
            modifiers
        },
        text: (modifiers & (ALT | CONTROL | META) == 0).then(|| produced.to_string()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn named_keys_and_chords_become_key_events() {
        let presses = parse("Enter ctrl+a shift+Tab").expect("keys");
        assert_eq!(presses[0].key, "Enter");
        assert_eq!(presses[0].text.as_deref(), Some("\r"));
        assert_eq!(presses[1].key, "a");
        assert_eq!(presses[1].code, "KeyA");
        assert_eq!(presses[1].modifiers, CONTROL);
        assert_eq!(presses[1].text, None, "a held control inserts nothing");
        assert_eq!(presses[2].modifiers, SHIFT);
    }

    #[test]
    fn letters_digits_and_case_insert_their_text() {
        let presses = parse("x shift+x X 7").expect("keys");
        let texts: Vec<_> = presses.iter().map(|press| press.text.as_deref()).collect();
        assert_eq!(texts, [Some("x"), Some("X"), Some("X"), Some("7")]);
        assert_eq!(presses[3].code, "Digit7");
    }

    #[test]
    fn unknown_keys_and_modifiers_are_refused() {
        assert!(parse("").is_err());
        assert!(parse("hyper+a").is_err());
        assert!(parse("F13").is_err());
        assert!(parse("ctrl+").is_err());
        assert!(parse("!").is_err());
        assert!(parse(&"a ".repeat(17)).is_err());
    }
}
