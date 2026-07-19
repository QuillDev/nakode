use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Position, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, BorderType, Borders, Clear, List, ListItem, Paragraph, Wrap},
};

use crate::{
    backend::{ApprovalRequest, TodoStatus},
    commands,
    selection::{ScreenPoint, ScreenSnapshot},
    state::AppState,
    transcript::{LineTone, ProjectedLine},
};

// Nako Agent shares the opaque pink-on-black visual language used across Quill's apps.
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
    let todo_height = todo_panel_height(state);
    let regions = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Min(4),
            Constraint::Length(todo_height),
            Constraint::Length(queue_height),
            Constraint::Length(5),
            Constraint::Length(1),
        ])
        .split(area);

    render_header(frame, regions[0]);
    render_transcript(frame, regions[1], state);
    if todo_height > 0 {
        render_todos(frame, regions[2], state);
    }
    if queue_height > 0 {
        render_queue(frame, regions[3], state);
    }
    let cursor = render_composer(frame, regions[4], state);
    render_footer(frame, regions[5], state);

    let has_modal = state.questions.front().is_some()
        || state.approvals.front().is_some()
        || state.show_help
        || state.session_picker.is_some()
        || state.provider_picker.is_some()
        || state.agent_picker.is_some()
        || state.model_picker.is_some()
        || state.subagent_modal.is_some();
    if !has_modal {
        render_command_completions(frame, regions[4], state);
    }

    if let Some(question) = state.questions.front() {
        render_question(frame, area, question);
    } else if let Some(approval) = state.approvals.front() {
        render_approval(frame, area, approval);
    } else if state.show_help {
        render_help(frame, area);
    } else if state.session_picker.is_some() {
        render_session_picker(frame, area, state);
    } else if state.provider_picker.is_some() {
        render_provider_picker(frame, area, state);
    } else if state.agent_picker.is_some() {
        render_agent_picker(frame, area, state);
    } else if state.model_picker.is_some() {
        render_model_picker(frame, area, state);
    } else if state.subagent_modal.is_some() {
        render_subagent_modal(frame, area, state);
    } else if let Some(position) = cursor {
        frame.set_cursor_position(position);
    }

    let selectable_regions = if state.questions.front().is_some() {
        vec![bordered_inner(centered(area, 76, 16))]
    } else if state.approvals.front().is_some() {
        vec![bordered_inner(centered(area, 76, 12))]
    } else if state.show_help {
        vec![bordered_inner(centered(area, 76, 26))]
    } else if state.session_picker.is_some() {
        vec![bordered_inner(centered(area, 78, 18))]
    } else if state.provider_picker.is_some() {
        vec![bordered_inner(provider_picker_popup(area, state))]
    } else if state.agent_picker.is_some() {
        vec![bordered_inner(centered(area, 82, 24))]
    } else if state.model_picker.is_some() {
        vec![bordered_inner(centered(area, 72, 18))]
    } else if state.subagent_modal.is_some() {
        vec![bordered_inner(subagent_modal_popup(area))]
    } else {
        let mut selectable = vec![bordered_inner(regions[1]), bordered_inner(regions[4])];
        if todo_height > 0 {
            selectable.push(bordered_inner(regions[2]));
        }
        if queue_height > 0 {
            selectable.push(bordered_inner(regions[3]));
        }
        selectable
    };
    capture_and_highlight_selection(frame, state, area, selectable_regions);
}

fn provider_picker_popup(area: Rect, state: &AppState) -> Rect {
    if state
        .provider_picker
        .as_ref()
        .is_some_and(|picker| picker.showing_details)
    {
        centered(area, 72, 32)
    } else {
        centered(area, 68, 14)
    }
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
        " NAKO AGENT ",
        Style::default().bg(ACCENT).fg(BACKGROUND).bold(),
    ));
    frame.render_widget(
        Paragraph::new(line).style(Style::default().bg(SURFACE)),
        area,
    );
}

fn render_transcript(frame: &mut Frame<'_>, area: Rect, state: &mut AppState) {
    let hit_regions = render_transcript_view(
        frame,
        area,
        &mut state.transcript,
        &mut state.scroll_from_bottom,
        Line::default(),
    );
    state.set_subagent_hit_regions(hit_regions);
}

fn render_transcript_view(
    frame: &mut Frame<'_>,
    area: Rect,
    transcript: &mut crate::transcript::Transcript,
    scroll_from_bottom: &mut usize,
    title: Line<'static>,
) -> Vec<(String, ScreenPoint, ScreenPoint)> {
    let block = panel_block(title);
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let width = usize::from(inner.width.max(1));
    let height = usize::from(inner.height);
    let max_scroll = transcript.max_scroll(width, height);
    *scroll_from_bottom = (*scroll_from_bottom).min(max_scroll);
    let visible = transcript.visible(width, height, *scroll_from_bottom);

    let hit_regions = visible
        .lines
        .iter()
        .enumerate()
        .filter_map(|(offset, line)| {
            let run_id = line.source_key.as_deref()?.strip_prefix("subagent:")?;
            let row = inner
                .y
                .saturating_add(u16::try_from(offset).unwrap_or(u16::MAX));
            Some((
                run_id.to_owned(),
                ScreenPoint::new(inner.x, row),
                ScreenPoint::new(inner.right(), row.saturating_add(1)),
            ))
        })
        .collect();

    let lines = visible
        .lines
        .into_iter()
        .map(transcript_line)
        .collect::<Vec<_>>();
    frame.render_widget(Paragraph::new(Text::from(lines)), inner);
    hit_regions
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

fn todo_panel_height(state: &AppState) -> u16 {
    let has_in_progress_task = state
        .todo_phases
        .iter()
        .flat_map(|phase| &phase.tasks)
        .any(|task| task.status == TodoStatus::InProgress);
    if !has_in_progress_task {
        return 0;
    }
    let content_lines = state
        .todo_phases
        .iter()
        .map(|phase| phase.tasks.len().saturating_add(1))
        .sum::<usize>();
    u16::try_from(content_lines)
        .unwrap_or(u16::MAX)
        .saturating_add(2)
        .min(8)
}

fn render_todos(frame: &mut Frame<'_>, area: Rect, state: &AppState) {
    let total = state
        .todo_phases
        .iter()
        .map(|phase| phase.tasks.len())
        .sum::<usize>();
    let completed = state
        .todo_phases
        .iter()
        .flat_map(|phase| &phase.tasks)
        .filter(|task| task.status == TodoStatus::Completed)
        .count();
    let available_lines = usize::from(area.height.saturating_sub(2));
    let mut lines = Vec::with_capacity(available_lines);
    for phase in &state.todo_phases {
        lines.push(Line::styled(
            format!(" {}", phase.name),
            Style::default().fg(ACCENT_BRIGHT).bold(),
        ));
        for task in &phase.tasks {
            let (marker, color) = match task.status {
                TodoStatus::Pending => ("○", MUTED),
                TodoStatus::InProgress => ("◉", WARNING),
                TodoStatus::Completed => ("✓", SUCCESS),
                TodoStatus::Abandoned => ("−", MUTED),
            };
            lines.push(Line::from(vec![
                Span::styled(format!("  {marker} "), Style::default().fg(color)),
                Span::styled(task.content.clone(), Style::default().fg(color)),
            ]));
        }
    }
    if lines.len() > available_lines {
        let hidden = lines
            .len()
            .saturating_sub(available_lines)
            .saturating_add(1);
        lines.truncate(available_lines.saturating_sub(1));
        lines.push(Line::styled(
            format!("  … {hidden} more"),
            Style::default().fg(MUTED),
        ));
    }
    let title = format!(" Todos · {completed}/{total} ");
    frame.render_widget(Paragraph::new(lines).block(panel_block(title)), area);
}

fn truncate_objective(objective: &str, width: usize) -> String {
    let objective = objective.lines().next().unwrap_or_default().trim();
    let characters = objective.chars().count();
    if characters <= width {
        return objective.to_owned();
    }
    if width <= 1 {
        return "…".chars().take(width).collect();
    }
    let mut truncated = objective.chars().take(width - 1).collect::<String>();
    truncated.push('…');
    truncated
}

fn subagent_modal_popup(area: Rect) -> Rect {
    centered(area, 92, area.height.saturating_sub(4))
}

fn render_subagent_modal(frame: &mut Frame<'_>, area: Rect, state: &mut AppState) {
    let Some((agent, objective)) = state.selected_subagent_summary() else {
        state.close_subagent_modal();
        return;
    };
    let popup = subagent_modal_popup(area);
    frame.render_widget(Clear, popup);
    let title = Line::from(vec![
        Span::styled(
            format!(" {agent} "),
            Style::default().fg(ACCENT_BRIGHT).bold(),
        ),
        Span::styled(
            format!("· {} · Esc close ", truncate_objective(&objective, 52)),
            Style::default().fg(MUTED),
        ),
    ]);
    if let Some((transcript, scroll)) = state.selected_subagent_transcript_mut() {
        let _ = render_transcript_view(frame, popup, transcript, scroll, title);
    }
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
    let mut lines = Vec::new();
    for group in ["General", "Compose", "Active turn", "Navigate"] {
        let entries = crate::controls::help_entries()
            .filter(|entry| entry.group == group)
            .collect::<Vec<_>>();
        if entries.is_empty() {
            continue;
        }
        if !lines.is_empty() {
            lines.push(Line::default());
        }
        lines.push(Line::styled(
            group,
            Style::default().fg(ACCENT_BRIGHT).bold(),
        ));
        lines.extend(
            entries
                .into_iter()
                .map(|entry| Line::raw(format!("  {:<22} {}", entry.keys, entry.description))),
        );
    }
    lines.push(Line::default());
    lines.push(Line::styled(
        "Slash commands",
        Style::default().fg(ACCENT_BRIGHT).bold(),
    ));
    lines.extend(crate::controls::slash_controls().iter().map(|control| {
        Line::raw(format!(
            "  {:<22} {}",
            control.invocation, control.description
        ))
    }));
    lines.push(Line::default());
    lines.push(Line::styled(
        "Esc, F1, or Ctrl+? closes this help.",
        Style::default().fg(MUTED),
    ));
    let requested_height = u16::try_from(lines.len())
        .unwrap_or(u16::MAX)
        .saturating_add(2);
    let popup = centered(area, 76, requested_height.min(area.height));
    frame.render_widget(Clear, popup);
    let block = overlay_block(" Help ", ACCENT);
    frame.render_widget(Paragraph::new(lines).block(block), popup);
}

fn render_provider_picker(frame: &mut Frame<'_>, area: Rect, state: &mut AppState) {
    state.set_oauth_link_hit_region(None);
    let picker = state.provider_picker.as_ref().expect("picker checked");
    if picker.showing_details {
        let picker = picker.clone();
        render_provider_details(frame, area, state, &picker);
        return;
    }
    let popup = centered(area, 68, 14);
    frame.render_widget(Clear, popup);
    let mut lines = vec![
        Line::styled(
            "↑/↓ select · Enter details · Esc close",
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
            let state_label = if provider.credential.is_none() {
                "setup required"
            } else if provider.enabled {
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
                        Style::default().fg(if provider.enabled {
                            SUCCESS
                        } else if provider.credential.is_none() {
                            WARNING
                        } else {
                            MUTED
                        }),
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

fn render_agent_picker(frame: &mut Frame<'_>, area: Rect, state: &AppState) {
    let popup = centered(area, 82, 24);
    frame.render_widget(Clear, popup);
    let picker = state.agent_picker.as_ref().expect("picker checked");
    let lines = if let Some(editor) = &picker.editor {
        agent_editor_lines(editor)
    } else {
        agent_list_lines(picker)
    };
    frame.render_widget(
        Paragraph::new(lines).block(overlay_block(" Agents ", ACCENT)),
        popup,
    );
}

fn agent_editor_lines(editor: &crate::state::AgentEditor) -> Vec<Line<'static>> {
    let mut lines = vec![
        Line::styled(
            "Tab/↑/↓ field · type or paste · Ctrl+S save · Esc cancel",
            Style::default().fg(MUTED),
        ),
        Line::default(),
    ];
    let values = [
        editor.slug.as_str(),
        editor.description.as_str(),
        editor.system_prompt.as_str(),
        editor.first_message.as_str(),
        editor.model.as_str(),
        editor.fallback_models.as_str(),
    ];
    for (field, value) in crate::state::AgentEditorField::ALL.into_iter().zip(values) {
        let selected = field == editor.field;
        lines.push(
            Line::from(vec![
                Span::styled(
                    format!("{:<15}", field.label()),
                    Style::default().fg(if selected { ACCENT_BRIGHT } else { MUTED }),
                ),
                Span::styled(
                    truncate_objective(value, 58),
                    Style::default().fg(TEXT).add_modifier(if selected {
                        Modifier::BOLD
                    } else {
                        Modifier::empty()
                    }),
                ),
            ])
            .style(Style::default().bg(if selected {
                SURFACE_RAISED
            } else {
                SURFACE
            })),
        );
        lines.push(Line::default());
    }
    lines.push(Line::styled(
        "Models use provider/model; separate fallbacks with commas.",
        Style::default().fg(MUTED),
    ));
    lines
}

fn agent_list_lines(picker: &crate::state::AgentPicker) -> Vec<Line<'static>> {
    let mut lines = vec![
        Line::styled(
            "↑/↓ select · Enter edit · n new · d delete · Esc close",
            Style::default().fg(MUTED),
        ),
        Line::default(),
    ];
    if picker.agents.is_empty() {
        lines.push(Line::styled(
            "No agent archetypes configured. Press n to create one.",
            Style::default().fg(MUTED),
        ));
    }
    for (index, agent) in picker.agents.iter().enumerate() {
        let selected = index == picker.selected;
        lines.push(agent_list_row(agent, selected));
        let models = std::iter::once(agent.model.as_deref().unwrap_or("inherit parent model"))
            .chain(agent.fallback_models.iter().map(String::as_str))
            .collect::<Vec<_>>()
            .join(" → ");
        lines.push(Line::styled(
            format!("    {models}"),
            Style::default().fg(if selected { ACCENT_DEEP } else { MUTED }),
        ));
    }
    lines
}

fn agent_list_row(agent: &crate::agent::AgentDefinition, selected: bool) -> Line<'static> {
    Line::from(vec![
        Span::styled(
            if selected { "› " } else { "  " },
            Style::default().fg(if selected { ACCENT } else { MUTED }),
        ),
        Span::styled(
            format!("{:<18}", agent.slug),
            Style::default()
                .fg(if selected { TEXT } else { MUTED })
                .add_modifier(if selected {
                    Modifier::BOLD
                } else {
                    Modifier::empty()
                }),
        ),
        Span::styled(
            truncate_objective(&agent.description, 38),
            Style::default().fg(MUTED),
        ),
    ])
    .style(Style::default().bg(if selected { SURFACE_RAISED } else { SURFACE }))
}

fn render_provider_details(
    frame: &mut Frame<'_>,
    area: Rect,
    state: &mut AppState,
    picker: &crate::state::ProviderPicker,
) {
    let popup = centered(area, 72, 32);
    frame.render_widget(Clear, popup);
    let Some(provider) = picker.providers.get(picker.selected) else {
        return;
    };
    let enabled = if provider.enabled {
        "enabled"
    } else {
        "disabled"
    };
    let state_color = if provider.enabled { SUCCESS } else { MUTED };
    let connection = state
        .provider_connection(&provider.provider)
        .map_or("not running", crate::state::ConnectionState::label);
    let mut lines = vec![
        Line::from(vec![
            Span::styled("State      ", Style::default().fg(MUTED)),
            Span::styled(enabled, Style::default().fg(state_color).bold()),
        ]),
        Line::from(vec![
            Span::styled("Connection ", Style::default().fg(MUTED)),
            Span::styled(connection, Style::default().fg(TEXT)),
        ]),
        Line::from(vec![
            Span::styled("Slug       ", Style::default().fg(MUTED)),
            Span::styled(&provider.provider, Style::default().fg(TEXT)),
        ]),
        Line::from(vec![
            Span::styled("Credential ", Style::default().fg(MUTED)),
            Span::styled(
                provider
                    .credential
                    .as_ref()
                    .map_or("not configured", |credential| credential.kind.as_str()),
                Style::default().fg(if provider.credential.is_some() {
                    SUCCESS
                } else {
                    WARNING
                }),
            ),
        ]),
        Line::default(),
        Line::styled("Capabilities", Style::default().fg(ACCENT_BRIGHT).bold()),
    ];
    if let Some(capabilities) = state.provider_capabilities(&provider.provider) {
        for (name, support) in capability_rows(capabilities) {
            let supported = support.is_supported();
            lines.push(Line::from(vec![
                Span::styled(format!("  {name:<22}"), Style::default().fg(MUTED)),
                Span::styled(
                    if supported {
                        "supported"
                    } else {
                        "unsupported"
                    },
                    Style::default().fg(if supported { SUCCESS } else { MUTED }),
                ),
            ]));
        }
    } else {
        lines.push(Line::styled(
            "  Unavailable until this provider is started.",
            Style::default().fg(MUTED),
        ));
    }
    let authentication_url_line = if let Some(authentication) = &picker.authentication {
        let first_line = lines.len();
        append_provider_authentication(&mut lines, authentication);
        matches!(
            authentication,
            crate::state::ProviderAuthentication::Challenge { .. }
        )
        .then_some(first_line + 1)
    } else {
        None
    };
    append_provider_actions(&mut lines, provider.credential.is_some());
    let block = overlay_block(format!(" {} ", provider.display_name), ACCENT);
    frame.render_widget(
        Paragraph::new(lines)
            .block(block)
            .wrap(Wrap { trim: false }),
        popup,
    );
    register_oauth_link(
        state,
        popup,
        authentication_url_line,
        picker.authentication.as_ref(),
    );
}

fn append_provider_actions(lines: &mut Vec<Line<'_>>, has_credential: bool) {
    lines.push(Line::default());
    if has_credential {
        lines.push(Line::styled(
            "[l] Log out and clear credentials",
            Style::default().fg(DANGER),
        ));
    }
    lines.push(Line::styled(
        if has_credential {
            "Enter or Space enable/disable · Esc providers"
        } else {
            "Enter or Space set up credentials · Esc providers"
        },
        Style::default().fg(MUTED),
    ));
}

fn register_oauth_link(
    state: &mut AppState,
    popup: Rect,
    line: Option<usize>,
    authentication: Option<&crate::state::ProviderAuthentication>,
) {
    let (
        Some(line),
        Some(crate::state::ProviderAuthentication::Challenge {
            verification_url, ..
        }),
    ) = (line, authentication)
    else {
        return;
    };
    let row = popup
        .y
        .saturating_add(1)
        .saturating_add(u16::try_from(line).unwrap_or(u16::MAX));
    state.set_oauth_link_hit_region(Some((
        verification_url.clone(),
        ScreenPoint::new(popup.x.saturating_add(1), row),
        ScreenPoint::new(popup.right().saturating_sub(1), row.saturating_add(1)),
    )));
}

fn append_provider_authentication<'a>(
    lines: &mut Vec<Line<'a>>,
    authentication: &'a crate::state::ProviderAuthentication,
) {
    lines.push(Line::default());
    match authentication {
        crate::state::ProviderAuthentication::Starting => lines.push(Line::styled(
            "Starting provider authentication…",
            Style::default().fg(WARNING),
        )),
        crate::state::ProviderAuthentication::Challenge {
            verification_url,
            user_code,
        } => {
            lines.push(Line::styled(
                "[o] Open in browser ↗  ·  [c] Copy URL",
                Style::default()
                    .fg(ACCENT_BRIGHT)
                    .add_modifier(Modifier::BOLD | Modifier::UNDERLINED),
            ));
            lines.push(Line::from(vec![
                Span::styled("URL ", Style::default().fg(MUTED)),
                Span::styled(verification_url, Style::default().fg(TEXT)),
            ]));
            if user_code.is_empty() {
                lines.push(Line::styled(
                    "Complete sign-in in your browser; this screen will update automatically.",
                    Style::default().fg(MUTED),
                ));
            } else {
                lines.push(Line::from(vec![
                    Span::styled("Code ", Style::default().fg(MUTED)),
                    Span::styled(user_code, Style::default().fg(TEXT).bold()),
                ]));
            }
        }
    }
}

fn capability_rows(
    capabilities: &crate::backend::BackendCapabilities,
) -> [(&'static str, crate::backend::CapabilitySupport); 10] {
    [
        ("Resume", capabilities.resume),
        ("Steering", capabilities.steering),
        ("Interruption", capabilities.interruption),
        ("Model catalog", capabilities.model_catalog),
        ("Models need session", capabilities.models_require_session),
        ("Session model config", capabilities.session_model_config),
        ("Approvals", capabilities.approvals),
        ("Native tools", capabilities.native_tools),
        ("MCP", capabilities.mcp),
        ("Close session", capabilities.close_session),
    ]
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

fn render_question(frame: &mut Frame<'_>, area: Rect, prompt: &crate::state::QuestionPrompt) {
    let description_count = prompt
        .request
        .options
        .iter()
        .filter(|option| option.description.is_some())
        .count();
    let height = u16::try_from(prompt.request.options.len() + description_count)
        .unwrap_or(8)
        .saturating_add(8)
        .min(20);
    let popup = centered(area, 76, height);
    frame.render_widget(Clear, popup);
    let mut lines = vec![
        Line::styled(&prompt.request.question, Style::default().fg(TEXT)),
        Line::default(),
    ];
    for (index, option) in prompt.request.options.iter().enumerate() {
        let selected = index == prompt.selected;
        let checked = prompt.selections.get(index).copied().unwrap_or(false);
        let marker = if prompt.request.multi {
            if checked {
                "✓"
            } else if selected {
                "›"
            } else {
                " "
            }
        } else if selected {
            "›"
        } else {
            " "
        };
        let style = if selected {
            Style::default().fg(ACCENT_BRIGHT).bold()
        } else {
            Style::default().fg(TEXT)
        };
        lines.push(Line::styled(
            format!(
                "{marker} {}. {}{}",
                index + 1,
                option.label,
                if prompt.request.recommended == Some(index) {
                    " (Recommended)"
                } else {
                    ""
                }
            ),
            style,
        ));
        if let Some(description) = &option.description {
            lines.push(Line::styled(
                format!("     ↳ {description}"),
                Style::default().fg(MUTED),
            ));
        }
    }
    lines.push(Line::default());
    lines.push(Line::styled(
        if prompt.request.multi {
            " ↑/↓ select · Space toggle · Enter confirm "
        } else {
            " ↑/↓ select · Enter choose · 1-8 quick select "
        },
        Style::default().fg(ACCENT),
    ));
    frame.render_widget(
        Paragraph::new(Text::from(lines))
            .block(overlay_block(format!(" {} ", prompt.request.title), ACCENT))
            .wrap(Wrap { trim: false }),
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

    let title = match picker.scope {
        crate::state::ModelSelectionScope::Default => " Default Model ",
        crate::state::ModelSelectionScope::Session => " Switch Session Model ",
    };
    let block = overlay_block(title, ACCENT);
    frame.render_widget(Paragraph::new(lines).block(block), popup);
}

fn transcript_line(line: ProjectedLine) -> Line<'static> {
    let color = match line.tone {
        LineTone::Muted | LineTone::Reasoning => MUTED,
        LineTone::User => ACCENT_BRIGHT,
        LineTone::Steering => ACCENT_DEEP,
        LineTone::Tool | LineTone::Warning => WARNING,
        LineTone::DiffAdd | LineTone::SubagentComplete => SUCCESS,
        LineTone::Error | LineTone::DiffRemove => DANGER,
        LineTone::SubagentPending => ACCENT,
        LineTone::Assistant | LineTone::Body | LineTone::Code | LineTone::DiffHeader => TEXT,
    };
    let mut style = Style::default().fg(color);
    if line.bold {
        style = style.add_modifier(Modifier::BOLD);
    }
    let text = if line.tone == LineTone::SubagentPending {
        line.text.replacen('⠋', subagent_spinner(), 1)
    } else {
        line.text
    };
    Line::styled(text, style)
}

fn subagent_spinner() -> &'static str {
    const FRAMES: [&str; 10] = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];
    let tick = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis() / 100);
    let frame = usize::try_from(tick % FRAMES.len() as u128).unwrap_or(0);
    FRAMES[frame]
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
        backend::{
            BackendCapabilities, BackendEvent, BackendIdentity, CODEX_PROVIDER, CapabilitySupport,
            DEVIN_PROVIDER, TodoItem, TodoPhase, TodoStatus,
        },
        session::ProviderRecord,
        state::{AgentRequest, AppState, Effect},
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
            .expect("render Nako Agent view");

        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(ratatui::buffer::Cell::symbol)
            .collect::<String>();
        assert!(rendered.contains("NAKO AGENT"));
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
    fn active_todos_render_as_a_compact_persistent_panel() {
        let backend = TestBackend::new(100, 28);
        let mut terminal = Terminal::new(backend).expect("create test terminal");
        let mut state = AppState::new("/tmp/project", None, 100);
        state.todo_phases = vec![TodoPhase {
            name: "Implementation".to_owned(),
            tasks: vec![
                TodoItem {
                    content: "Project todo events".to_owned(),
                    status: TodoStatus::Completed,
                },
                TodoItem {
                    content: "Render the active plan".to_owned(),
                    status: TodoStatus::InProgress,
                },
            ],
        }];

        terminal
            .draw(|frame| super::draw(frame, &mut state))
            .expect("render todo panel");
        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(ratatui::buffer::Cell::symbol)
            .collect::<String>();

        assert!(rendered.contains("Todos · 1/2"));
        assert!(rendered.contains("Implementation"));
        assert!(rendered.contains("Project todo events"));
        assert!(rendered.contains("Render the active plan"));
    }

    #[test]
    fn inactive_todos_do_not_render_as_a_persistent_panel() {
        let backend = TestBackend::new(100, 28);
        let mut terminal = Terminal::new(backend).expect("create test terminal");
        let mut state = AppState::new("/tmp/project", None, 100);
        state.todo_phases = vec![TodoPhase {
            name: "Finished work".to_owned(),
            tasks: vec![
                TodoItem {
                    content: "Completed task".to_owned(),
                    status: TodoStatus::Completed,
                },
                TodoItem {
                    content: "Pending task".to_owned(),
                    status: TodoStatus::Pending,
                },
            ],
        }];

        terminal
            .draw(|frame| super::draw(frame, &mut state))
            .expect("render without inactive todo panel");
        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(ratatui::buffer::Cell::symbol)
            .collect::<String>();

        assert!(!rendered.contains("Todos"));
        assert!(!rendered.contains("Finished work"));
        assert!(!rendered.contains("Completed task"));
        assert!(!rendered.contains("Pending task"));
    }

    #[test]
    fn subagent_renders_inline_with_pending_status_and_truncated_objective() {
        let backend = TestBackend::new(100, 28);
        let mut terminal = Terminal::new(backend).expect("create test terminal");
        let mut state = AppState::new("/tmp/project", None, 100);
        state.invoke_agent(&AgentRequest {
            id: 1,
            agent: "explorer".to_owned(),
            task: "Map the authentication flow and identify every relevant boundary".to_owned(),
        });

        terminal
            .draw(|frame| super::draw(frame, &mut state))
            .expect("render subagent");
        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(ratatui::buffer::Cell::symbol)
            .collect::<String>();

        assert!(!rendered.contains("Subagents · click to inspect"));
        assert!(rendered.contains("pending"));
        assert!(rendered.contains("Map the authentication flow"));
        assert!(!rendered.contains("Starting provider"));
    }

    #[test]
    fn completed_subagent_remains_available_inline() {
        let backend = TestBackend::new(100, 28);
        let mut terminal = Terminal::new(backend).expect("create test terminal");
        let mut state = AppState::new("/tmp/project", None, 100);
        let effects = state.invoke_agent(&AgentRequest {
            id: 1,
            agent: "explorer".to_owned(),
            task: "Inspect persistence boundaries".to_owned(),
        });
        let Effect::SpawnSubagent { run_id, .. } = &effects[0] else {
            panic!("expected subagent launch");
        };
        state.handle_subagent_backend(
            run_id,
            BackendEvent::TurnCompleted {
                turn_id: "child-turn".to_owned(),
                outcome: crate::backend::TurnOutcome::Completed,
                error: None,
            },
        );

        terminal
            .draw(|frame| super::draw(frame, &mut state))
            .expect("render completed subagent");
        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(ratatui::buffer::Cell::symbol)
            .collect::<String>();

        assert!(rendered.contains("completed"));
        assert!(rendered.contains("Inspect persistence boundaries"));
        assert!(!rendered.contains("pending"));
    }

    #[test]
    fn clicking_a_subagent_opens_its_reused_transcript_view() {
        let backend = TestBackend::new(100, 28);
        let mut terminal = Terminal::new(backend).expect("create test terminal");
        let mut state = AppState::new("/tmp/project", None, 100);
        state.invoke_agent(&AgentRequest {
            id: 1,
            agent: "explorer".to_owned(),
            task: "Map authentication".to_owned(),
        });
        terminal
            .draw(|frame| super::draw(frame, &mut state))
            .expect("render inline subagent");
        let objective_row = terminal
            .backend()
            .buffer()
            .content()
            .chunks(100)
            .position(|row| {
                row.iter()
                    .map(ratatui::buffer::Cell::symbol)
                    .collect::<String>()
                    .contains("Map authentication")
            })
            .expect("inline objective row");
        assert!(state.open_subagent_at(crate::selection::ScreenPoint::new(
            2,
            u16::try_from(objective_row).expect("test row fits in terminal")
        )));

        terminal
            .draw(|frame| super::draw(frame, &mut state))
            .expect("render subagent transcript modal");
        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(ratatui::buffer::Cell::symbol)
            .collect::<String>();

        assert!(rendered.contains("explorer"));
        assert!(rendered.contains("Map authentication"));
        assert!(rendered.contains("PARENT"));
        assert!(rendered.contains("Delegated task"));
    }

    #[test]
    fn provider_menu_shows_state_and_live_capability_details() {
        let backend = TestBackend::new(100, 30);
        let mut terminal = Terminal::new(backend).expect("create test terminal");
        let mut state = AppState::new("/tmp/project", None, 100);
        state.handle_backend(BackendEvent::Ready(BackendIdentity {
            provider: CODEX_PROVIDER.to_owned(),
            display_name: "Codex".to_owned(),
            version: None,
            capabilities: BackendCapabilities {
                resume: CapabilitySupport::Supported,
                ..BackendCapabilities::default()
            },
        }));
        state.editor.set_text("/providers");
        let _ = state.submit_editor();
        state.install_providers(vec![
            ProviderRecord {
                provider: CODEX_PROVIDER.to_owned(),
                display_name: "Codex".to_owned(),
                enabled: true,
                credential: Some(crate::session::ProviderCredentialRecord {
                    provider: CODEX_PROVIDER.to_owned(),
                    kind: "chatgpt_device_code".to_owned(),
                    metadata: serde_json::json!({}),
                    updated_at: 1,
                }),
            },
            ProviderRecord {
                provider: DEVIN_PROVIDER.to_owned(),
                display_name: "Devin".to_owned(),
                enabled: false,
                credential: None,
            },
        ]);

        terminal
            .draw(|frame| super::draw(frame, &mut state))
            .expect("render provider list");
        let list = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(ratatui::buffer::Cell::symbol)
            .collect::<String>();
        assert!(list.contains("Codex  enabled"));
        assert!(list.contains("Devin  setup required"));

        state.open_provider_details();
        terminal
            .draw(|frame| super::draw(frame, &mut state))
            .expect("render provider details");
        let details = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(ratatui::buffer::Cell::symbol)
            .collect::<String>();
        assert!(details.contains("Capabilities"));
        assert!(details.contains("Resume"));
        assert!(details.contains("supported"));
        assert!(details.contains("[l] Log out and clear credentials"));
    }

    #[test]
    fn provider_authentication_shows_full_url_and_click_target() {
        let backend = TestBackend::new(100, 30);
        let mut terminal = Terminal::new(backend).expect("create test terminal");
        let mut state = AppState::new("/tmp/project", None, 100);
        state.editor.set_text("/providers");
        let _ = state.submit_editor();
        state.install_providers(vec![ProviderRecord {
            provider: CODEX_PROVIDER.to_owned(),
            display_name: "Codex".to_owned(),
            enabled: false,
            credential: None,
        }]);
        state.open_provider_details();

        state
            .provider_picker
            .as_mut()
            .expect("provider picker")
            .authentication = Some(crate::state::ProviderAuthentication::Challenge {
            verification_url: "https://app.example.test/auth/cli/continue".to_owned(),
            user_code: String::new(),
        });
        terminal
            .draw(|frame| super::draw(frame, &mut state))
            .expect("render provider authentication");
        let authentication_row = terminal
            .backend()
            .buffer()
            .content()
            .chunks(100)
            .position(|row| {
                row.iter()
                    .map(ratatui::buffer::Cell::symbol)
                    .collect::<String>()
                    .contains("[o] Open in browser ↗")
            })
            .expect("authentication URL row");
        let rendered_authentication = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(ratatui::buffer::Cell::symbol)
            .collect::<String>();
        assert!(rendered_authentication.contains("https://app.example.test/auth/cli/continue"));
        assert!(rendered_authentication.contains("[c] Copy URL"));
        assert_eq!(
            state
                .oauth_url_at(crate::selection::ScreenPoint::new(
                    16,
                    u16::try_from(authentication_row).expect("test row fits")
                ))
                .as_deref(),
            Some("https://app.example.test/auth/cli/continue")
        );
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
    fn agent_menu_shows_archetypes_and_all_editable_configuration_fields() {
        let backend = TestBackend::new(100, 30);
        let mut terminal = Terminal::new(backend).expect("create test terminal");
        let mut state = AppState::new("/tmp/project", None, 100);
        state.open_agent_picker();

        terminal
            .draw(|frame| super::draw(frame, &mut state))
            .expect("render agent list");
        let list = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(ratatui::buffer::Cell::symbol)
            .collect::<String>();
        assert!(list.contains("explorer"));

        state.edit_selected_agent();
        terminal
            .draw(|frame| super::draw(frame, &mut state))
            .expect("render agent editor");
        let editor = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(ratatui::buffer::Cell::symbol)
            .collect::<String>();
        assert!(editor.contains("System prompt"));
        assert!(editor.contains("First message"));
        assert!(editor.contains("Fallbacks"));
        assert!(editor.contains("Ctrl+S save"));
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
