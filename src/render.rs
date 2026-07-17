use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Position, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, Borders, Clear, List, ListItem, Paragraph},
};

use crate::{
    backend::ApprovalRequest,
    selection::{ScreenPoint, ScreenSnapshot},
    state::AppState,
    transcript::{LineTone, ProjectedLine},
};

const BG: Color = Color::Rgb(12, 15, 20);
const PANEL: Color = Color::Rgb(18, 23, 31);
const BORDER: Color = Color::Rgb(51, 63, 78);
const TEXT: Color = Color::Rgb(211, 219, 229);
const MUTED: Color = Color::Rgb(118, 131, 148);
const CYAN: Color = Color::Rgb(87, 201, 221);
const BLUE: Color = Color::Rgb(114, 159, 255);
const GREEN: Color = Color::Rgb(104, 211, 145);
const YELLOW: Color = Color::Rgb(239, 198, 92);
const RED: Color = Color::Rgb(246, 112, 116);
const MAGENTA: Color = Color::Rgb(202, 141, 255);

pub fn draw(frame: &mut Frame<'_>, state: &mut AppState) {
    let area = frame.area();
    frame.render_widget(Block::new().style(Style::default().bg(BG).fg(TEXT)), area);

    let queue_height = if state.queue.is_empty() {
        0
    } else {
        (state.queue.len() as u16 + 2).min(5)
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

    render_header(frame, regions[0], state);
    render_transcript(frame, regions[1], state);
    if queue_height > 0 {
        render_queue(frame, regions[2], state);
    }
    let cursor = render_composer(frame, regions[3], state);
    render_footer(frame, regions[4], state);

    if let Some(approval) = state.approvals.front() {
        render_approval(frame, area, approval);
    } else if state.show_help {
        render_help(frame, area);
    } else if state.session_picker.is_some() {
        render_session_picker(frame, area, state);
    } else if state.model_picker.is_some() {
        render_model_picker(frame, area, state);
    } else if let Some(position) = cursor {
        frame.set_cursor_position(position);
    }

    let selectable_regions = if state.approvals.front().is_some() {
        vec![bordered_inner(centered(area, 76, 12))]
    } else if state.show_help {
        vec![bordered_inner(centered(area, 72, 22))]
    } else if state.session_picker.is_some() {
        vec![bordered_inner(centered(area, 78, 18))]
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

fn render_header(frame: &mut Frame<'_>, area: Rect, state: &AppState) {
    let model = state.selected_model.as_deref().unwrap_or("catalog…");
    let activity = if let Some(turn) = &state.active_turn {
        if turn.cancelling {
            "cancelling"
        } else {
            "working"
        }
    } else if state.is_busy() {
        "starting"
    } else {
        state.connection.label()
    };
    let line = Line::from(vec![
        Span::styled(" FLOCK ", Style::default().bg(CYAN).fg(BG).bold()),
        Span::styled(
            format!("  {activity}"),
            Style::default().fg(status_color(state)),
        ),
        Span::styled("  model ", Style::default().fg(MUTED)),
        Span::styled(model, Style::default().fg(BLUE)),
        Span::styled(
            format!("  queue {}", state.queue.len()),
            Style::default().fg(MUTED),
        ),
    ]);
    frame.render_widget(Paragraph::new(line).style(Style::default().bg(PANEL)), area);
}

fn render_transcript(frame: &mut Frame<'_>, area: Rect, state: &mut AppState) {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(BORDER))
        .style(Style::default().bg(BG));
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let width = usize::from(inner.width.max(1));
    let height = usize::from(inner.height);
    let max_scroll = state.transcript.max_scroll(width, height);
    state.scroll_from_bottom = state.scroll_from_bottom.min(max_scroll);
    let visible = state
        .transcript
        .visible(width, height, state.scroll_from_bottom);
    let title = if visible.total_lines > 0 {
        format!(
            " Transcript · {}–{} / {} ",
            visible.first_line.saturating_add(1),
            visible.first_line + visible.lines.len(),
            visible.total_lines
        )
    } else {
        " Transcript ".to_owned()
    };
    frame.render_widget(
        Paragraph::new(title).style(Style::default().fg(MUTED).bg(BG)),
        Rect::new(
            area.x.saturating_add(2),
            area.y,
            area.width.saturating_sub(4),
            1,
        ),
    );

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
            let marker = if state.queue_selection == Some(index) {
                "›"
            } else {
                " "
            };
            let summary = prompt.text.lines().next().unwrap_or_default();
            ListItem::new(Line::from(vec![
                Span::styled(
                    format!("{marker} {}  ", index + 1),
                    Style::default().fg(YELLOW),
                ),
                Span::styled(summary, Style::default().fg(TEXT)),
            ]))
        })
        .collect::<Vec<_>>();
    let block = Block::default()
        .title(" Queue · Alt+↑/↓ select · Alt+Delete remove ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(BORDER));
    frame.render_widget(List::new(items).block(block), area);
}

fn render_composer(frame: &mut Frame<'_>, area: Rect, state: &AppState) -> Option<Position> {
    let busy_label = if state.is_busy() {
        " Prompt · Enter queue · Ctrl+S steer "
    } else {
        " Prompt · Enter send "
    };
    let block = Block::default()
        .title(busy_label)
        .borders(Borders::ALL)
        .border_style(Style::default().fg(if state.editor.is_blank() {
            BORDER
        } else {
            CYAN
        }))
        .style(Style::default().bg(PANEL));
    let inner = block.inner(area);
    frame.render_widget(block, area);

    if inner.width == 0 || inner.height == 0 {
        return None;
    }
    let window = state.editor.window(inner.height, inner.width);
    let lines = window
        .lines
        .into_iter()
        .map(|line| Line::styled(line, Style::default().fg(TEXT)))
        .collect::<Vec<_>>();
    frame.render_widget(Paragraph::new(Text::from(lines)), inner);
    Some(Position::new(
        inner.x.saturating_add(window.cursor_x),
        inner.y.saturating_add(window.cursor_y),
    ))
}

fn render_footer(frame: &mut Frame<'_>, area: Rect, state: &AppState) {
    let help = if state.active_turn.is_some() {
        " Ctrl+Q queue  Ctrl+S steer  Ctrl+C cancel  F2 models  F1 help "
    } else {
        " Alt+Enter newline  Enter send  PgUp/PgDn scroll  F2 models  F1 help "
    };
    let status_width = area.width.saturating_sub(help.len() as u16);
    let status_area = Rect::new(area.x, area.y, status_width, 1);
    let help_area = Rect::new(
        area.x.saturating_add(status_width),
        area.y,
        area.width.saturating_sub(status_width),
        1,
    );
    frame.render_widget(
        Paragraph::new(format!(" {}", state.status_message))
            .style(Style::default().fg(MUTED).bg(PANEL)),
        status_area,
    );
    frame.render_widget(
        Paragraph::new(help).style(Style::default().fg(TEXT).bg(PANEL)),
        help_area,
    );
}

fn render_help(frame: &mut Frame<'_>, area: Rect) {
    let popup = centered(area, 72, 22);
    frame.render_widget(Clear, popup);
    let lines = vec![
        Line::styled("Compose", Style::default().fg(CYAN).bold()),
        Line::raw("  Enter / Ctrl+Enter   send, or queue while busy"),
        Line::raw("  Alt+Enter / Shift+Enter / Ctrl+J   newline"),
        Line::default(),
        Line::styled("Active turn", Style::default().fg(CYAN).bold()),
        Line::raw("  Ctrl+Q   queue draft     Ctrl+S   steer now"),
        Line::raw("  Ctrl+C   interrupt       Ctrl+C again   exit"),
        Line::default(),
        Line::styled("Navigate", Style::default().fg(CYAN).bold()),
        Line::raw("  PageUp/PageDown   transcript     Ctrl+L   latest"),
        Line::raw("  F2   models       Alt+Up/Down   queue selection"),
        Line::raw("  Alt+Delete   remove queued message"),
        Line::raw("  Mouse drag   select and auto-copy rendered text"),
        Line::default(),
        Line::styled("Sessions", Style::default().fg(CYAN).bold()),
        Line::raw("  /resume   picker    /resume ID   resume    /new   fresh"),
        Line::raw("  /reload   refresh backend metadata and models"),
        Line::default(),
        Line::styled("F1 or Esc closes this help.", Style::default().fg(MUTED)),
    ];
    let block = Block::default()
        .title(" Help ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(CYAN))
        .style(Style::default().bg(PANEL));
    frame.render_widget(Paragraph::new(lines).block(block), popup);
}

fn render_approval(frame: &mut Frame<'_>, area: Rect, approval: &ApprovalRequest) {
    let popup = centered(area, 76, 12);
    frame.render_widget(Clear, popup);
    let controls = " y accept once · a accept for session · n decline ";
    let text = Text::from(vec![
        Line::styled(&approval.detail, Style::default().fg(TEXT)),
        Line::default(),
        Line::styled(controls, Style::default().fg(YELLOW).bold()),
    ]);
    let block = Block::default()
        .title(format!(" {} ", approval.title))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(YELLOW))
        .style(Style::default().bg(PANEL));
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
            lines.push(Line::from(vec![
                Span::styled(
                    marker,
                    Style::default().fg(if selected { CYAN } else { MUTED }),
                ),
                Span::styled(
                    &session.title,
                    Style::default()
                        .fg(if selected { TEXT } else { BLUE })
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
            ]));
        }
    }
    lines.push(Line::default());
    lines.push(Line::styled(
        "↑/↓ select · Enter resume · Esc cancel",
        Style::default().fg(MUTED),
    ));
    let block = Block::default()
        .title(" Resume session ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(CYAN))
        .style(Style::default().bg(PANEL));
    frame.render_widget(Paragraph::new(lines).block(block), popup);
}

fn relative_time(timestamp: i64) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
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
        let current = if state.selected_model.as_deref() == Some(model.id.as_str()) {
            "  current"
        } else if model.is_default {
            "  default"
        } else {
            ""
        };
        lines.push(Line::from(vec![
            Span::styled(
                marker,
                Style::default().fg(if selected { CYAN } else { MUTED }),
            ),
            Span::styled(
                &model.display_name,
                Style::default()
                    .fg(if selected { TEXT } else { BLUE })
                    .add_modifier(if selected {
                        Modifier::BOLD
                    } else {
                        Modifier::empty()
                    }),
            ),
            Span::styled(current, Style::default().fg(MUTED)),
        ]));
        if selected && !model.description.is_empty() {
            lines.push(Line::styled(
                format!("    {}", model.description),
                Style::default().fg(MUTED),
            ));
        }
    }
    if filtered.is_empty() {
        lines.push(Line::styled(
            "  No matching models",
            Style::default().fg(RED),
        ));
    }
    lines.push(Line::default());
    lines.push(Line::styled(
        "Type to filter · ↑/↓ select · Enter apply · Esc cancel",
        Style::default().fg(MUTED),
    ));

    let block = Block::default()
        .title(" Models ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(CYAN))
        .style(Style::default().bg(PANEL));
    frame.render_widget(Paragraph::new(lines).block(block), popup);
}

fn transcript_line(line: ProjectedLine) -> Line<'static> {
    let color = match line.tone {
        LineTone::Muted => MUTED,
        LineTone::User => CYAN,
        LineTone::Assistant => BLUE,
        LineTone::Steering => MAGENTA,
        LineTone::Reasoning => MUTED,
        LineTone::Tool => YELLOW,
        LineTone::DiffAdd => GREEN,
        LineTone::DiffRemove => RED,
        LineTone::DiffHeader => CYAN,
        LineTone::Warning => YELLOW,
        LineTone::Error => RED,
        LineTone::Body => TEXT,
        LineTone::Code => Color::Rgb(180, 205, 225),
    };
    let mut style = Style::default().fg(color);
    if line.bold {
        style = style.add_modifier(Modifier::BOLD);
    }
    Line::styled(line.text, style)
}

fn status_color(state: &AppState) -> Color {
    match &state.connection {
        crate::state::ConnectionState::Starting => YELLOW,
        crate::state::ConnectionState::Ready { .. } => {
            if state.active_turn.is_some() {
                CYAN
            } else {
                GREEN
            }
        }
        crate::state::ConnectionState::Failed(_)
        | crate::state::ConnectionState::Disconnected(_) => RED,
    }
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
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(rendered.contains("FLOCK"));
        assert!(rendered.contains("Transcript"));
        assert!(rendered.contains("Prompt"));
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
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(rendered.contains("Active turn"));
        assert!(rendered.contains("Ctrl+S"));
        assert!(rendered.contains("F1 or Esc"));
    }
}
