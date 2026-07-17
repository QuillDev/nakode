use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Position, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, BorderType, Borders, Clear, List, ListItem, Paragraph},
};

use crate::{
    backend::ApprovalRequest,
    commands,
    selection::{ScreenPoint, ScreenSnapshot},
    state::AppState,
    transcript::{LineTone, ProjectedLine},
};

// Flock shares the opaque pink-on-black visual language used across Quill's apps.
// Pink communicates interaction and focus; green, amber, and red are reserved for
// semantic state so the interface remains calm and immediately scannable.
const BACKGROUND: Color = Color::Rgb(10, 10, 13);
const SURFACE: Color = Color::Rgb(18, 19, 25);
const SURFACE_RAISED: Color = Color::Rgb(27, 29, 38);
const BORDER: Color = Color::Rgb(42, 45, 58);
const TEXT: Color = Color::Rgb(232, 233, 238);
const MUTED: Color = Color::Rgb(139, 144, 160);
const ACCENT: Color = Color::Rgb(246, 92, 142);
const ACCENT_BRIGHT: Color = Color::Rgb(255, 122, 165);
const ACCENT_DEEP: Color = Color::Rgb(216, 69, 111);
const SUCCESS: Color = Color::Rgb(74, 222, 128);
const WARNING: Color = Color::Rgb(250, 204, 21);
const DANGER: Color = Color::Rgb(248, 113, 113);

fn panel_block<'a>(title: impl Into<Line<'a>>) -> Block<'a> {
    Block::default()
        .title(title)
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(BORDER))
        .style(Style::default().bg(BACKGROUND).fg(TEXT))
}

fn overlay_block<'a>(title: impl Into<Line<'a>>, border: Color) -> Block<'a> {
    Block::default()
        .title(title)
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(border))
        .style(Style::default().bg(SURFACE).fg(TEXT))
}

pub fn draw(frame: &mut Frame<'_>, state: &mut AppState) {
    let area = frame.area();
    frame.render_widget(
        Block::new().style(Style::default().bg(BACKGROUND).fg(TEXT)),
        area,
    );

    let queue_height = if state.queue.is_empty() {
        0
    } else {
        u16::try_from(state.queue.len())
            .unwrap_or(u16::MAX)
            .saturating_add(2)
            .min(5)
    };
    let regions = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Min(4),
            Constraint::Length(queue_height),
            Constraint::Length(5),
            Constraint::Length(1),
        ])
        .split(area);

    render_header(frame, regions[0]);
    render_transcript(frame, regions[1], state);
    if queue_height > 0 {
        render_queue(frame, regions[2], state);
    }
    let cursor = render_composer(frame, regions[3], state);
    render_footer(frame, regions[4], state);

    let has_modal = state.approvals.front().is_some()
        || state.show_help
        || state.session_picker.is_some()
        || state.provider_picker.is_some()
        || state.model_picker.is_some();
    if !has_modal {
        render_command_completions(frame, regions[3], state);
    }

    if let Some(approval) = state.approvals.front() {
        render_approval(frame, area, approval);
    } else if state.show_help {
        render_help(frame, area);
    } else if state.session_picker.is_some() {
        render_session_picker(frame, area, state);
    } else if state.provider_picker.is_some() {
        render_provider_picker(frame, area, state);
    } else if state.model_picker.is_some() {
        render_model_picker(frame, area, state);
    } else if let Some(position) = cursor {
        frame.set_cursor_position(position);
    }

    let selectable_regions = if state.approvals.front().is_some() {
        vec![bordered_inner(centered(area, 76, 12))]
    } else if state.show_help {
        vec![bordered_inner(centered(area, 76, 25))]
    } else if state.session_picker.is_some() {
        vec![bordered_inner(centered(area, 78, 18))]
    } else if state.provider_picker.is_some() {
        vec![bordered_inner(centered(area, 64, 14))]
    } else if state.model_picker.is_some() {
        vec![bordered_inner(centered(area, 72, 18))]
    } else {
        let mut selectable = vec![bordered_inner(regions[1]), bordered_inner(regions[3])];
        if queue_height > 0 {
            selectable.push(bordered_inner(regions[2]));
        }
        selectable
    };
    capture_and_highlight_selection(frame, state, area, selectable_regions);
}

fn capture_and_highlight_selection(
    frame: &mut Frame<'_>,
    state: &mut AppState,
    area: Rect,
    selectable_regions: Vec<Rect>,
) {
    let selection = state
        .text_selection
        .filter(|selection| selection.is_range());
    let highlight_area = selection
        .and_then(|selection| {
            selectable_regions.iter().copied().find(|region| {
                rect_contains(*region, selection.anchor) && rect_contains(*region, selection.head)
            })
        })
        .unwrap_or(area);
    let snapshot = {
        let buffer = frame.buffer_mut();
        let snapshot = ScreenSnapshot::capture(buffer, area, selectable_regions);
        if let Some(selection) = selection {
            for row in highlight_area.y..highlight_area.bottom() {
                for column in highlight_area.x..highlight_area.right() {
                    if selection.contains(ScreenPoint::new(column, row)) {
                        buffer[(column, row)].modifier.insert(Modifier::REVERSED);
                    }
                }
            }
        }
        snapshot
    };
    state.set_screen_snapshot(snapshot);
}

fn bordered_inner(area: Rect) -> Rect {
    Rect::new(
        area.x.saturating_add(1),
        area.y.saturating_add(1),
        area.width.saturating_sub(2),
        area.height.saturating_sub(2),
    )
}

fn rect_contains(area: Rect, point: ScreenPoint) -> bool {
    area.width > 0
        && area.height > 0
        && point.column >= area.x
        && point.column < area.right()
        && point.row >= area.y
        && point.row < area.bottom()
}

fn render_header(frame: &mut Frame<'_>, area: Rect) {
    let line = Line::from(Span::styled(
        " FLOCK ",
        Style::default().bg(ACCENT).fg(BACKGROUND).bold(),
    ));
    frame.render_widget(
        Paragraph::new(line).style(Style::default().bg(SURFACE)),
        area,
    );
}

fn render_transcript(frame: &mut Frame<'_>, area: Rect, state: &mut AppState) {
    let block = panel_block(Line::default());
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let width = usize::from(inner.width.max(1));
    let height = usize::from(inner.height);
    let max_scroll = state.transcript.max_scroll(width, height);
    state.scroll_from_bottom = state.scroll_from_bottom.min(max_scroll);
    let visible = state
        .transcript
        .visible(width, height, state.scroll_from_bottom);

    let lines = visible
        .lines
        .into_iter()
        .map(transcript_line)
        .collect::<Vec<_>>();
    frame.render_widget(Paragraph::new(Text::from(lines)), inner);
}

fn render_queue(frame: &mut Frame<'_>, area: Rect, state: &AppState) {
    let items = state
        .queue
        .iter()
        .enumerate()
        .map(|(index, prompt)| {
            let selected = state.queue_selection == Some(index);
            let marker = if selected { "›" } else { " " };
            let summary = prompt.text.lines().next().unwrap_or_default();
            ListItem::new(Line::from(vec![
                Span::styled(
                    format!("{marker} {}  ", index + 1),
                    Style::default().fg(if selected { ACCENT_BRIGHT } else { MUTED }),
                ),
                Span::styled(summary, Style::default().fg(TEXT)),
            ]))
            .style(Style::default().bg(if selected {
                SURFACE_RAISED
            } else {
                BACKGROUND
            }))
        })
        .collect::<Vec<_>>();
    let block = panel_block(" Queue · Alt+↑/↓ select · Alt+Delete remove ");
    frame.render_widget(List::new(items).block(block), area);
}

fn render_composer(frame: &mut Frame<'_>, area: Rect, state: &AppState) -> Option<Position> {
    let block = overlay_block(
        " Prompt ",
        if state.editor.is_blank() {
            BORDER
        } else {
            ACCENT
        },
    );
    let inner = block.inner(area);
    frame.render_widget(block, area);

    if inner.width == 0 || inner.height == 0 {
        return None;
    }
    let window = state.editor.window(inner.height, inner.width);
    let lines = window
        .lines
        .into_iter()
        .enumerate()
        .map(|(index, line)| {
            styled_composer_line(
                line,
                window.first_row + index == 0 && window.horizontal_offset == 0,
            )
        })
        .collect::<Vec<_>>();
    frame.render_widget(Paragraph::new(Text::from(lines)), inner);
    Some(Position::new(
        inner.x.saturating_add(window.cursor_x),
        inner.y.saturating_add(window.cursor_y),
    ))
}

fn styled_composer_line(line: String, first_prompt_line: bool) -> Line<'static> {
    let ranges = commands::highlighted_ranges(&line, first_prompt_line);
    if ranges.is_empty() {
        return Line::styled(line, Style::default().fg(TEXT));
    }

    let mut spans = Vec::with_capacity(ranges.len().saturating_mul(2).saturating_add(1));
    let mut offset = 0;
    for range in ranges {
        if offset < range.start {
            spans.push(Span::styled(
                line[offset..range.start].to_owned(),
                Style::default().fg(TEXT),
            ));
        }
        spans.push(Span::styled(
            line[range.clone()].to_owned(),
            Style::default().fg(ACCENT_BRIGHT).bold(),
        ));
        offset = range.end;
    }
    if offset < line.len() {
        spans.push(Span::styled(
            line[offset..].to_owned(),
            Style::default().fg(TEXT),
        ));
    }
    Line::from(spans)
}

fn render_command_completions(frame: &mut Frame<'_>, composer_area: Rect, state: &AppState) {
    let completions = state.command_completions();
    if completions.is_empty() || composer_area.width < 4 {
        return;
    }

    let selected = state.selected_command_completion();
    let height = u16::try_from(completions.len())
        .unwrap_or(u16::MAX)
        .saturating_add(2);
    let popup = Rect::new(
        composer_area.x.saturating_add(1),
        composer_area.y.saturating_sub(height),
        composer_area.width.saturating_sub(2).min(68),
        height,
    );
    frame.render_widget(Clear, popup);

    let items = completions.into_iter().map(|completion| {
        let is_selected = selected == Some(completion);
        ListItem::new(Line::from(vec![
            Span::styled(
                if is_selected { " › " } else { "   " },
                Style::default().fg(ACCENT),
            ),
            Span::styled(
                format!("{:<12}", completion.invocation),
                Style::default().fg(ACCENT_BRIGHT).bold(),
            ),
            Span::styled(completion.description, Style::default().fg(MUTED)),
        ]))
        .style(Style::default().bg(if is_selected { SURFACE_RAISED } else { SURFACE }))
    });
    let block = overlay_block(" Commands · ↑/↓ select · Tab complete ", ACCENT);
    frame.render_widget(List::new(items).block(block), popup);
}

fn render_footer(frame: &mut Frame<'_>, area: Rect, state: &AppState) {
    frame.render_widget(
        Paragraph::new(format!(" {}", state.status_message))
            .style(Style::default().fg(MUTED).bg(SURFACE)),
        area,
    );
}

fn render_help(frame: &mut Frame<'_>, area: Rect) {
    let popup = centered(area, 76, 25);
    frame.render_widget(Clear, popup);
    let lines = vec![
        Line::styled("Compose", Style::default().fg(ACCENT_BRIGHT).bold()),
        Line::raw("  Enter       send while idle, steer during an active turn"),
        Line::raw("  Alt+Enter   send while idle, queue during an active turn"),
        Line::raw("  Shift+Enter / Ctrl+J   newline"),
        Line::default(),
        Line::styled("Active turn", Style::default().fg(ACCENT_BRIGHT).bold()),
        Line::raw("  Ctrl+Q   queue draft     Ctrl+S   steer now"),
        Line::raw("  Ctrl+C   interrupt       Ctrl+C again   exit"),
        Line::default(),
        Line::styled("Navigate", Style::default().fg(ACCENT_BRIGHT).bold()),
        Line::raw("  Alt+←/→   previous/next word     Ctrl/Cmd+←/→   line edge"),
        Line::raw("  Ctrl/Cmd+↑/↓   prompt edge       PageUp/PageDown   transcript"),
        Line::raw("  Alt+Backspace   previous word    Ctrl/Cmd+Backspace   line start"),
        Line::raw("  Ctrl+L   latest   F2 models   Alt+↑/↓ queue   Alt+Delete remove"),
        Line::raw("  Mouse drag   select and auto-copy rendered text"),
        Line::default(),
        Line::styled("Sessions", Style::default().fg(ACCENT_BRIGHT).bold()),
        Line::raw("  /resume   picker    /resume ID   resume    /new   fresh"),
        Line::raw("  /reload   refresh backend metadata and models"),
        Line::raw("  /providers manage enabled agent backends"),
        Line::raw("  /skill:name reference a skill anywhere in the prompt"),
        Line::default(),
        Line::styled(
            "Ctrl+?, F1, or Esc closes this help.",
            Style::default().fg(MUTED),
        ),
    ];
    let block = overlay_block(" Help ", ACCENT);
    frame.render_widget(Paragraph::new(lines).block(block), popup);
}

fn render_provider_picker(frame: &mut Frame<'_>, area: Rect, state: &AppState) {
    let popup = centered(area, 64, 14);
    frame.render_widget(Clear, popup);
    let picker = state.provider_picker.as_ref().expect("picker checked");
    let mut lines = vec![
        Line::styled(
            "↑/↓ select · Enter or Space toggle · Esc close",
            Style::default().fg(MUTED),
        ),
        Line::default(),
    ];
    if picker.loading {
        lines.push(Line::styled(
            "Loading providers…",
            Style::default().fg(MUTED),
        ));
    } else if picker.providers.is_empty() {
        lines.push(Line::styled(
            "No providers registered.",
            Style::default().fg(MUTED),
        ));
    } else {
        for (index, provider) in picker.providers.iter().enumerate() {
            let selected = index == picker.selected;
            let marker = if selected { "› " } else { "  " };
            let state_label = if provider.enabled {
                "enabled"
            } else {
                "disabled"
            };
            lines.push(
                Line::from(vec![
                    Span::styled(
                        marker,
                        Style::default().fg(if selected { ACCENT } else { MUTED }),
                    ),
                    Span::styled(
                        &provider.display_name,
                        Style::default()
                            .fg(if selected { TEXT } else { MUTED })
                            .add_modifier(if selected {
                                Modifier::BOLD
                            } else {
                                Modifier::empty()
                            }),
                    ),
                    Span::raw("  "),
                    Span::styled(
                        state_label,
                        Style::default().fg(if provider.enabled { SUCCESS } else { MUTED }),
                    ),
                ])
                .style(Style::default().bg(if selected {
                    SURFACE_RAISED
                } else {
                    SURFACE
                })),
            );
        }
    }
    let block = overlay_block(" Providers ", ACCENT);
    frame.render_widget(Paragraph::new(lines).block(block), popup);
}

fn render_approval(frame: &mut Frame<'_>, area: Rect, approval: &ApprovalRequest) {
    let popup = centered(area, 76, 12);
    frame.render_widget(Clear, popup);
    let controls = " y accept once · a accept for session · n decline ";
    let text = Text::from(vec![
        Line::styled(&approval.detail, Style::default().fg(TEXT)),
        Line::default(),
        Line::styled(controls, Style::default().fg(WARNING).bold()),
    ]);
    let block = overlay_block(format!(" {} ", approval.title), WARNING);
    frame.render_widget(
        Paragraph::new(text)
            .block(block)
            .wrap(ratatui::widgets::Wrap { trim: false }),
        popup,
    );
}

fn render_session_picker(frame: &mut Frame<'_>, area: Rect, state: &AppState) {
    let popup = centered(area, 78, 18);
    frame.render_widget(Clear, popup);
    let picker = state.session_picker.as_ref().expect("picker checked");
    let mut lines = Vec::new();
    if picker.loading {
        lines.push(Line::styled(
            "Loading sessions…",
            Style::default().fg(MUTED),
        ));
    } else if picker.sessions.is_empty() {
        lines.push(Line::styled(
            "No saved sessions for this workspace.",
            Style::default().fg(MUTED),
        ));
    } else {
        let visible_count = usize::from(popup.height.saturating_sub(5));
        let first = picker
            .selected
            .saturating_sub(visible_count.saturating_sub(1));
        for (index, session) in picker
            .sessions
            .iter()
            .enumerate()
            .skip(first)
            .take(visible_count)
        {
            let selected = index == picker.selected;
            let marker = if selected { "› " } else { "  " };
            let short_id = session.id.get(..8).unwrap_or(&session.id);
            lines.push(
                Line::from(vec![
                    Span::styled(
                        marker,
                        Style::default().fg(if selected { ACCENT } else { MUTED }),
                    ),
                    Span::styled(
                        &session.title,
                        Style::default()
                            .fg(if selected { TEXT } else { MUTED })
                            .add_modifier(if selected {
                                Modifier::BOLD
                            } else {
                                Modifier::empty()
                            }),
                    ),
                    Span::styled(
                        format!("  {short_id}  {}", relative_time(session.updated_at)),
                        Style::default().fg(MUTED),
                    ),
                ])
                .style(Style::default().bg(if selected {
                    SURFACE_RAISED
                } else {
                    SURFACE
                })),
            );
        }
    }
    lines.push(Line::default());
    lines.push(Line::styled(
        "↑/↓ select · Enter resume · Esc cancel",
        Style::default().fg(MUTED),
    ));
    let block = overlay_block(" Resume session ", ACCENT);
    frame.render_widget(Paragraph::new(lines).block(block), popup);
}

fn relative_time(timestamp: i64) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .try_into()
        .unwrap_or(i64::MAX);
    let age = now.saturating_sub(timestamp);
    match age {
        0..=59 => "now".to_owned(),
        60..=3_599 => format!("{}m ago", age / 60),
        3_600..=86_399 => format!("{}h ago", age / 3_600),
        _ => format!("{}d ago", age / 86_400),
    }
}

fn render_model_picker(frame: &mut Frame<'_>, area: Rect, state: &AppState) {
    let popup = centered(area, 72, 18);
    frame.render_widget(Clear, popup);
    let picker = state.model_picker.as_ref().expect("picker checked");
    let filtered = state.filtered_models();
    let mut lines = vec![Line::from(vec![
        Span::styled("Filter: ", Style::default().fg(MUTED)),
        Span::styled(&picker.filter, Style::default().fg(TEXT)),
    ])];
    lines.push(Line::default());
    for (index, model) in filtered.iter().enumerate() {
        let selected = index == picker.selected;
        let marker = if selected { "› " } else { "  " };
        let qualified = model.qualified_id();
        let current = if state.selected_model.as_deref() == Some(qualified.as_str()) {
            "  current"
        } else if model.is_default {
            "  default"
        } else {
            ""
        };
        lines.push(
            Line::from(vec![
                Span::styled(
                    marker,
                    Style::default().fg(if selected { ACCENT } else { MUTED }),
                ),
                Span::styled(
                    qualified,
                    Style::default()
                        .fg(if selected { TEXT } else { MUTED })
                        .add_modifier(if selected {
                            Modifier::BOLD
                        } else {
                            Modifier::empty()
                        }),
                ),
                Span::styled(current, Style::default().fg(MUTED)),
            ])
            .style(Style::default().bg(if selected {
                SURFACE_RAISED
            } else {
                SURFACE
            })),
        );
    }
    if filtered.is_empty() {
        lines.push(Line::styled(
            "  No matching models",
            Style::default().fg(DANGER),
        ));
    }
    lines.push(Line::default());
    lines.push(Line::styled(
        "Type to filter · ↑/↓ select · Enter apply · Esc cancel",
        Style::default().fg(MUTED),
    ));

    let block = overlay_block(" Models ", ACCENT);
    frame.render_widget(Paragraph::new(lines).block(block), popup);
}

fn transcript_line(line: ProjectedLine) -> Line<'static> {
    let color = match line.tone {
        LineTone::Muted | LineTone::Reasoning => MUTED,
        LineTone::User => ACCENT_BRIGHT,
        LineTone::Steering => ACCENT_DEEP,
        LineTone::Tool | LineTone::Warning => WARNING,
        LineTone::DiffAdd => SUCCESS,
        LineTone::Error | LineTone::DiffRemove => DANGER,
        LineTone::Assistant | LineTone::Body | LineTone::Code | LineTone::DiffHeader => TEXT,
    };
    let mut style = Style::default().fg(color);
    if line.bold {
        style = style.add_modifier(Modifier::BOLD);
    }
    Line::styled(line.text, style)
}

fn centered(area: Rect, width_percent: u16, height: u16) -> Rect {
    let width = area
        .width
        .saturating_mul(width_percent)
        .saturating_div(100)
        .max(24);
    let width = width.min(area.width);
    let height = height.min(area.height);
    Rect::new(
        area.x + area.width.saturating_sub(width) / 2,
        area.y + area.height.saturating_sub(height) / 2,
        width,
        height,
    )
}

#[cfg(test)]
mod tests {
    use ratatui::{Terminal, backend::TestBackend};

    use crate::{
        backend::{BackendCapabilities, BackendEvent, BackendIdentity, CODEX_PROVIDER},
        state::AppState,
    };

    #[test]
    fn main_view_renders_into_a_test_backend() {
        let backend = TestBackend::new(100, 28);
        let mut terminal = Terminal::new(backend).expect("create test terminal");
        let mut state = AppState::new("/tmp/project", Some("fixture-model".to_owned()), 100);
        state.handle_backend(BackendEvent::Ready(BackendIdentity {
            provider: CODEX_PROVIDER.to_owned(),
            display_name: "fake-codex".to_owned(),
            version: None,
            capabilities: BackendCapabilities::default(),
        }));

        terminal
            .draw(|frame| super::draw(frame, &mut state))
            .expect("render Flock view");

        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(ratatui::buffer::Cell::symbol)
            .collect::<String>();
        assert!(rendered.contains("FLOCK"));
        assert!(rendered.contains("Prompt"));
        assert!(!rendered.contains("Transcript"));
        assert!(!rendered.contains("fixture-model"));
        assert!(!rendered.contains("queue 0"));
        assert!(!rendered.contains("F1 help"));

        let buffer = terminal.backend().buffer();
        assert_eq!(buffer[(0, 0)].bg, super::ACCENT);
        assert_eq!(buffer[(0, 0)].fg, super::BACKGROUND);
        assert_eq!(buffer[(0, 1)].symbol(), "╭");
        assert_eq!(buffer[(0, 1)].fg, super::BORDER);
    }

    #[test]
    fn help_overlay_lists_core_turn_controls() {
        let backend = TestBackend::new(100, 28);
        let mut terminal = Terminal::new(backend).expect("create test terminal");
        let mut state = AppState::new("/tmp/project", None, 100);
        state.show_help = true;

        terminal
            .draw(|frame| super::draw(frame, &mut state))
            .expect("render help overlay");

        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(ratatui::buffer::Cell::symbol)
            .collect::<String>();
        assert!(rendered.contains("Active turn"));
        assert!(rendered.contains("Ctrl+S"));
        assert!(rendered.contains("Ctrl+?"));
        assert!(rendered.contains("F1"));
    }

    #[test]
    fn slash_input_renders_command_completions() {
        let backend = TestBackend::new(100, 28);
        let mut terminal = Terminal::new(backend).expect("create test terminal");
        let mut state = AppState::new("/tmp/project", None, 100);
        state.editor.set_text("/");

        terminal
            .draw(|frame| super::draw(frame, &mut state))
            .expect("render command completions");

        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(ratatui::buffer::Cell::symbol)
            .collect::<String>();
        assert!(rendered.contains("Commands"));
        assert!(rendered.contains("/providers"));
        assert!(rendered.contains("/skill:"));
    }

    #[test]
    fn valid_commands_are_styled_with_the_accent() {
        let command = super::styled_composer_line("/new prompt".to_owned(), true);
        assert_eq!(command.spans[0].content, "/new");
        assert_eq!(command.spans[0].style.fg, Some(super::ACCENT_BRIGHT));

        let invalid = super::styled_composer_line("inside /new".to_owned(), true);
        assert_eq!(invalid.style.fg, Some(super::TEXT));

        let skill = super::styled_composer_line("inside(/skill:review".to_owned(), false);
        assert_eq!(skill.spans[1].content, "/skill:review");
        assert_eq!(skill.spans[1].style.fg, Some(super::ACCENT_BRIGHT));
    }
}
