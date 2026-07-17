use std::ops::Range;

pub const SKILL_PREFIX: &str = "/skill:";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommandPlacement {
    PromptStart,
    Anywhere,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CommandSpec {
    pub invocation: &'static str,
    pub description: &'static str,
    pub placement: CommandPlacement,
}

pub const COMMANDS: [CommandSpec; 5] = [
    CommandSpec {
        invocation: "/new",
        description: "start a fresh session",
        placement: CommandPlacement::PromptStart,
    },
    CommandSpec {
        invocation: "/providers",
        description: "manage enabled agent backends",
        placement: CommandPlacement::PromptStart,
    },
    CommandSpec {
        invocation: "/reload",
        description: "refresh backend metadata and models",
        placement: CommandPlacement::PromptStart,
    },
    CommandSpec {
        invocation: "/resume",
        description: "choose or name a session to resume",
        placement: CommandPlacement::PromptStart,
    },
    CommandSpec {
        invocation: SKILL_PREFIX,
        description: "reference a skill anywhere in the prompt",
        placement: CommandPlacement::Anywhere,
    },
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ParsedPromptCommand<'a> {
    New,
    Providers,
    Reload,
    Resume(Option<&'a str>),
}

#[must_use]
pub fn matching(prefix: &str, at_prompt_start: bool) -> Vec<&'static CommandSpec> {
    if !prefix.starts_with('/') {
        return Vec::new();
    }

    COMMANDS
        .iter()
        .filter(|command| {
            (at_prompt_start || command.placement == CommandPlacement::Anywhere)
                && command.invocation.starts_with(prefix)
        })
        .collect()
}

#[must_use]
pub fn parse_prompt_command(input: &str) -> Option<ParsedPromptCommand<'_>> {
    let command = input.trim_end();
    match command {
        "/new" => Some(ParsedPromptCommand::New),
        "/providers" => Some(ParsedPromptCommand::Providers),
        "/reload" => Some(ParsedPromptCommand::Reload),
        "/resume" => Some(ParsedPromptCommand::Resume(None)),
        _ => command
            .strip_prefix("/resume ")
            .map(str::trim)
            .filter(|session| !session.is_empty())
            .map(|session| ParsedPromptCommand::Resume(Some(session))),
    }
}

#[must_use]
pub fn highlighted_ranges(line: &str, first_prompt_line: bool) -> Vec<Range<usize>> {
    let mut ranges = Vec::new();
    let first_token_end = line.find(char::is_whitespace).unwrap_or(line.len());
    if first_prompt_line
        && COMMANDS.iter().any(|command| {
            command.placement == CommandPlacement::PromptStart
                && command.invocation == &line[..first_token_end]
        })
    {
        ranges.push(0..first_token_end);
    }

    let mut search_start = 0;
    while let Some(relative_start) = line[search_start..].find(SKILL_PREFIX) {
        let start = search_start + relative_start;
        let end = line[start..]
            .find(char::is_whitespace)
            .map_or(line.len(), |length| start + length);
        if !ranges.iter().any(|range| range.start == start) {
            ranges.push(start..end);
        }
        search_start = end.max(start + SKILL_PREFIX.len());
    }
    ranges.sort_unstable_by_key(|range| range.start);
    ranges
}

#[cfg(test)]
mod tests {
    use super::{CommandPlacement, ParsedPromptCommand, highlighted_ranges, matching};

    #[test]
    fn ordinary_commands_only_complete_at_the_prompt_start() {
        assert!(matching("/re", true).len() >= 2);
        assert!(matching("/re", false).is_empty());
        assert!(
            matching("/s", false)
                .iter()
                .all(|command| command.placement == CommandPlacement::Anywhere)
        );
    }

    #[test]
    fn parser_recognizes_commands_with_and_without_arguments() {
        assert_eq!(
            super::parse_prompt_command("/new"),
            Some(ParsedPromptCommand::New)
        );
        assert_eq!(
            super::parse_prompt_command("/resume session-1"),
            Some(ParsedPromptCommand::Resume(Some("session-1")))
        );
        assert_eq!(super::parse_prompt_command("hello /new"), None);
        assert_eq!(super::parse_prompt_command(" /new"), None);
    }

    #[test]
    fn highlighting_respects_command_placement() {
        assert_eq!(highlighted_ranges("/new prompt", true), vec![0..4]);
        assert!(highlighted_ranges("hello /new", true).is_empty());
        assert_eq!(
            highlighted_ranges("hello(/skill:review now", false),
            vec![6..19]
        );
    }
}
