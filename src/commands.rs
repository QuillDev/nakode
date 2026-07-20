use std::ops::Range;

pub use crate::controls::{CommandPlacement, SKILL_PREFIX, SlashControl as CommandSpec};
use crate::controls::{SlashAction, slash_controls};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ParsedPromptCommand<'a> {
    Agents,
    Compress,
    Models,
    New,
    Providers,
    Reload,
    Resume(Option<&'a str>),
    Switch,
}

#[must_use]
pub fn matching(prefix: &str, at_prompt_start: bool) -> Vec<&'static CommandSpec> {
    if !prefix.starts_with('/') {
        return Vec::new();
    }

    slash_controls()
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
    if let Some(control) = slash_controls()
        .iter()
        .find(|control| control.invocation == command)
    {
        return match control.action {
            SlashAction::Agents => Some(ParsedPromptCommand::Agents),
            SlashAction::Compress => Some(ParsedPromptCommand::Compress),
            SlashAction::Models => Some(ParsedPromptCommand::Models),
            SlashAction::New => Some(ParsedPromptCommand::New),
            SlashAction::Providers => Some(ParsedPromptCommand::Providers),
            SlashAction::Reload => Some(ParsedPromptCommand::Reload),
            SlashAction::Resume => Some(ParsedPromptCommand::Resume(None)),
            SlashAction::Switch => Some(ParsedPromptCommand::Switch),
            SlashAction::Skill => None,
        };
    }
    command
        .strip_prefix("/resume ")
        .map(str::trim)
        .filter(|session| !session.is_empty())
        .map(|session| ParsedPromptCommand::Resume(Some(session)))
}

#[must_use]
pub fn highlighted_ranges(line: &str, first_prompt_line: bool) -> Vec<Range<usize>> {
    let mut ranges = Vec::new();
    let first_token_end = line.find(char::is_whitespace).unwrap_or(line.len());
    if first_prompt_line
        && slash_controls().iter().any(|command| {
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
            super::parse_prompt_command("/agents"),
            Some(ParsedPromptCommand::Agents)
        );
        assert_eq!(
            super::parse_prompt_command("/compress"),
            Some(ParsedPromptCommand::Compress)
        );
        assert_eq!(
            super::parse_prompt_command("/new"),
            Some(ParsedPromptCommand::New)
        );
        assert_eq!(
            super::parse_prompt_command("/models"),
            Some(ParsedPromptCommand::Models)
        );
        assert_eq!(
            super::parse_prompt_command("/switch"),
            Some(ParsedPromptCommand::Switch)
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
