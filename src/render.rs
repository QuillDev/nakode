use ratatui::{
    Frame,
    layout::{Alignment, Constraint, Direction, Layout, Position, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, BorderType, Borders, Clear, List, ListItem, Paragraph, Wrap},
};
use unicode_width::UnicodeWidthStr;

use nakode_protocol::{InteractionView, ModelOptions, TodoStatusView};

use crate::{
    commands,
    selection::{ScreenPoint, ScreenSnapshot},
    transcript::{
        IMAGE_PREVIEW_MARKER, IMAGE_PREVIEW_ROWS, LineTone, MarkdownModifier, MarkdownSpan,
        MarkdownTone, ProjectedLine, is_tool_toggle_marker,
    },
    tui_state::{
        AgentBrowserStatus, AgentEditor, AgentEditorField, AgentModelOption, AgentPicker,
        MemoryBackend, ModelPickerStage, ModelSelectionScope, ProviderAuthentication,
        ProviderPicker, QuestionPrompt, SettingsState, SettingsView, TuiState, WebBackend,
        connection_label, provider_capability_rows, provider_dashboard_url,
        terminal_image_mode_label,
    },
};

use crate::terminal_image::TerminalImageRenderer;

// Nakode shares the opaque pink-on-black visual language used across Quill's apps.
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
const SPINNER_FRAMES: [&str; 10] = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];

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

#[derive(Default)]
struct RenderOutput {
    screen_snapshot: Option<ScreenSnapshot>,
    scroll_from_bottom: usize,
    selected_subagent_scroll: Option<usize>,
    subagent_hit_regions: Vec<(String, ScreenPoint, ScreenPoint)>,
    tool_toggle_hit_regions: Vec<(String, ScreenPoint, ScreenPoint)>,
    oauth_link_hit_region: Option<(String, ScreenPoint, ScreenPoint)>,
    api_key_input_hit_region: Option<(ScreenPoint, ScreenPoint)>,
}

pub fn draw(frame: &mut Frame<'_>, state: &mut TuiState) {
    draw_with_images(frame, state, None);
}

pub fn draw_with_images(
    frame: &mut Frame<'_>,
    state: &mut TuiState,
    image_renderer: Option<&mut TerminalImageRenderer>,
) {
    draw_tui_with_images(frame, state, image_renderer);
}

fn draw_tui_with_images(
    frame: &mut Frame<'_>,
    state: &mut TuiState,
    image_renderer: Option<&mut TerminalImageRenderer>,
) {
    let mut output = RenderOutput {
        scroll_from_bottom: state.client.scroll_from_bottom,
        ..RenderOutput::default()
    };
    draw_immutable(frame, state, image_renderer, &mut output);
    state.client.scroll_from_bottom = output.scroll_from_bottom;
    if let Some(scroll) = output.selected_subagent_scroll {
        state.set_selected_subagent_scroll(scroll);
    }
    if let Some(snapshot) = output.screen_snapshot {
        state.set_screen_snapshot(snapshot);
    }
    state.set_subagent_hit_regions(output.subagent_hit_regions);
    state.set_tool_toggle_hit_regions(output.tool_toggle_hit_regions);
    state.set_oauth_link_hit_region(output.oauth_link_hit_region);
    state.set_api_key_input_hit_region(output.api_key_input_hit_region);
}

fn draw_immutable(
    frame: &mut Frame<'_>,
    state: &TuiState,
    mut image_renderer: Option<&mut TerminalImageRenderer>,
    output: &mut RenderOutput,
) {
    let area = frame.area();
    if let Some(renderer) = image_renderer.as_deref_mut() {
        renderer.begin_frame(area.as_size());
    }
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

    render_header(frame, regions[0], state);
    render_transcript(frame, regions[1], state, image_renderer, output);
    if todo_height > 0 {
        render_todos(frame, regions[2], state);
    }
    if queue_height > 0 {
        render_queue(frame, regions[3], state);
    }
    let cursor = render_composer(frame, regions[4], state);
    render_prompt_metadata(frame, regions[5], state);

    let has_modal = state.questions.front().is_some()
        || state.approvals.front().is_some()
        || state.client.show_help
        || state.client.session_picker.is_some()
        || state.client.provider_picker.is_some()
        || state.client.agent_picker.is_some()
        || state.client.settings.is_some()
        || state.client.model_picker.is_some()
        || state.client.subagent_modal.is_some();
    if !has_modal {
        render_command_completions(frame, regions[4], state);
    }

    if let Some(question) = state.questions.front() {
        render_question(frame, area, question);
    } else if let Some(approval) = state.approvals.front() {
        render_approval(frame, area, approval);
    } else if state.client.show_help {
        render_help(frame, area);
    } else if state.client.session_picker.is_some() {
        render_session_picker(frame, area, state);
    } else if state.client.provider_picker.is_some() {
        render_provider_picker(frame, area, state, output);
    } else if state.client.settings.is_some() {
        render_settings(frame, area, state);
    } else if state.client.agent_picker.is_some() {
        render_agent_picker(frame, area, state);
    } else if state.client.model_picker.is_some() {
        render_model_picker(frame, area, state);
    } else if state.client.subagent_modal.is_some() {
        render_subagent_modal(frame, area, state, output);
    } else if let Some(position) = cursor {
        frame.set_cursor_position(position);
    }

    let selectable_regions = if state.questions.front().is_some() {
        vec![bordered_inner(centered(area, 76, 16))]
    } else if state.approvals.front().is_some() {
        vec![bordered_inner(centered(area, 76, 12))]
    } else if state.client.show_help {
        vec![bordered_inner(centered(area, 76, 26))]
    } else if state.client.session_picker.is_some() {
        vec![bordered_inner(centered(area, 78, 18))]
    } else if state.client.provider_picker.is_some() {
        vec![bordered_inner(provider_picker_popup(area, state))]
    } else if state.client.settings.is_some() {
        vec![bordered_inner(centered(area, 76, 22))]
    } else if state.client.agent_picker.is_some() {
        vec![bordered_inner(centered(area, 82, 24))]
    } else if state.client.model_picker.is_some() {
        vec![bordered_inner(centered(area, 72, 18))]
    } else if state.client.subagent_modal.is_some() {
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
    capture_and_highlight_selection(frame, state, area, selectable_regions, output);
}

fn provider_picker_popup(area: Rect, state: &TuiState) -> Rect {
    if state
        .client
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
    state: &TuiState,
    area: Rect,
    selectable_regions: Vec<Rect>,
    output: &mut RenderOutput,
) {
    let selection = state
        .client
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
    output.screen_snapshot = Some(snapshot);
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

fn render_header(frame: &mut Frame<'_>, area: Rect, state: &TuiState) {
    frame.render_widget(
        Paragraph::new(Line::default()).style(Style::default().bg(SURFACE)),
        area,
    );

    let brand_width = area.width.min(8);
    let brand_area = Rect::new(area.x, area.y, brand_width, area.height);
    frame.render_widget(
        Paragraph::new(Line::from(Span::styled(
            " NAKODE ",
            Style::default().bg(ACCENT).fg(BACKGROUND).bold(),
        ))),
        brand_area,
    );

    let (model, model_style) = state.selected_model_display_name().map_or_else(
        || ("No model selected".to_owned(), Style::default().fg(MUTED)),
        |model| (model, Style::default().fg(ACCENT_BRIGHT).bold()),
    );
    let mut spans = vec![
        Span::styled("MODEL ", Style::default().fg(MUTED)),
        Span::styled(model, model_style),
    ];
    if state.selected_model_uses_fast_mode() {
        spans.push(Span::styled(" ⚡", Style::default().fg(ACCENT).bold()));
    }
    spans.push(Span::raw(" "));
    let line = Line::from(spans);
    let model_area = Rect::new(
        area.x.saturating_add(brand_width),
        area.y,
        area.width.saturating_sub(brand_width),
        area.height,
    );
    frame.render_widget(
        Paragraph::new(line)
            .alignment(Alignment::Right)
            .style(Style::default().bg(SURFACE)),
        model_area,
    );
}

struct TranscriptHitRegions {
    subagents: Vec<(String, ScreenPoint, ScreenPoint)>,
    tool_toggles: Vec<(String, ScreenPoint, ScreenPoint)>,
}

fn render_transcript(
    frame: &mut Frame<'_>,
    area: Rect,
    state: &TuiState,
    image_renderer: Option<&mut TerminalImageRenderer>,
    output: &mut RenderOutput,
) {
    let mut transcript = state.transcript.clone();
    let hit_regions = render_transcript_view(
        frame,
        area,
        &mut transcript,
        &mut output.scroll_from_bottom,
        Line::default(),
        image_renderer,
    );
    output.subagent_hit_regions = hit_regions.subagents;
    output.tool_toggle_hit_regions = hit_regions.tool_toggles;
}

fn render_transcript_view(
    frame: &mut Frame<'_>,
    area: Rect,
    transcript: &mut crate::transcript::Transcript,
    scroll_from_bottom: &mut usize,
    title: Line<'static>,
    mut image_renderer: Option<&mut TerminalImageRenderer>,
) -> TranscriptHitRegions {
    let block = panel_block(title);
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let width = usize::from(inner.width.max(1));
    let height = usize::from(inner.height);
    let max_scroll = transcript.max_scroll(width, height);
    *scroll_from_bottom = (*scroll_from_bottom).min(max_scroll);
    let visible = transcript.visible(width, height, *scroll_from_bottom);

    let subagents = visible
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
    let tool_toggles = visible
        .lines
        .iter()
        .enumerate()
        .filter_map(|(offset, line)| {
            if !is_tool_toggle_marker(&line.text) {
                return None;
            }
            let key = line.source_key.clone()?;
            let row = inner
                .y
                .saturating_add(u16::try_from(offset).unwrap_or(u16::MAX));
            Some((
                key,
                ScreenPoint::new(inner.x, row),
                ScreenPoint::new(inner.right(), row.saturating_add(1)),
            ))
        })
        .collect();

    let image_placements = visible
        .lines
        .iter()
        .enumerate()
        .filter_map(|(offset, line)| {
            let index = line.text.strip_prefix(IMAGE_PREVIEW_MARKER)?.parse().ok()?;
            let key = line.source_key.clone()?;
            (offset.saturating_add(IMAGE_PREVIEW_ROWS) <= visible.lines.len())
                .then_some((key, index, offset))
        })
        .collect::<Vec<_>>();
    let lines = visible
        .lines
        .into_iter()
        .map(|line| {
            if line.text.starts_with(IMAGE_PREVIEW_MARKER) {
                Line::default()
            } else {
                transcript_line(line)
            }
        })
        .collect::<Vec<_>>();
    frame.render_widget(Paragraph::new(Text::from(lines)), inner);
    if let Some(renderer) = image_renderer.as_mut() {
        for (key, index, offset) in image_placements {
            let Some(image) = transcript.image(&key, index) else {
                continue;
            };
            let row = inner
                .y
                .saturating_add(u16::try_from(offset).unwrap_or(u16::MAX));
            let preview = Rect::new(
                inner.x.saturating_add(2),
                row,
                inner.width.saturating_sub(4),
                u16::try_from(IMAGE_PREVIEW_ROWS).unwrap_or(u16::MAX),
            );
            renderer.render(frame, preview, image);
        }
    }
    TranscriptHitRegions {
        subagents,
        tool_toggles,
    }
}

fn render_queue(frame: &mut Frame<'_>, area: Rect, state: &TuiState) {
    let items = state
        .queue
        .iter()
        .enumerate()
        .map(|(index, prompt)| {
            let selected = state.client.queue_selection == Some(index);
            let marker = if selected { "›" } else { " " };
            let summary = prompt.summary.lines().next().unwrap_or_default();
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

fn todo_panel_height(state: &TuiState) -> u16 {
    let has_in_progress_task = state
        .todo_phases
        .iter()
        .flat_map(|phase| &phase.tasks)
        .any(|task| task.status == TodoStatusView::InProgress);
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

fn render_todos(frame: &mut Frame<'_>, area: Rect, state: &TuiState) {
    let total = state
        .todo_phases
        .iter()
        .map(|phase| phase.tasks.len())
        .sum::<usize>();
    let completed = state
        .todo_phases
        .iter()
        .flat_map(|phase| &phase.tasks)
        .filter(|task| task.status == TodoStatusView::Completed)
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
                TodoStatusView::Pending => ("○", MUTED),
                TodoStatusView::InProgress => ("◉", WARNING),
                TodoStatusView::Completed => ("✓", SUCCESS),
                TodoStatusView::Abandoned => ("−", MUTED),
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

fn render_subagent_modal(
    frame: &mut Frame<'_>,
    area: Rect,
    state: &TuiState,
    output: &mut RenderOutput,
) {
    let Some((agent, objective)) = state.selected_subagent_summary() else {
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
    let tool_toggles = if let Some(transcript) = state.selected_subagent_transcript() {
        let mut transcript = transcript.clone();
        let mut scroll = state.selected_subagent_scroll();
        let regions =
            render_transcript_view(frame, popup, &mut transcript, &mut scroll, title, None);
        output.selected_subagent_scroll = Some(scroll);
        regions.tool_toggles
    } else {
        Vec::new()
    };
    output.tool_toggle_hit_regions = tool_toggles;
}

fn render_composer(frame: &mut Frame<'_>, area: Rect, state: &TuiState) -> Option<Position> {
    let composer_label = if state.is_shell_mode() {
        " Shell "
    } else {
        " Nako "
    };
    let mut title = if state.is_busy() {
        vec![
            Span::raw(" "),
            Span::styled(spinner_frame(), Style::default().fg(ACCENT_BRIGHT)),
            Span::styled(composer_label, Style::default().fg(TEXT).bold()),
        ]
    } else {
        vec![Span::styled(
            composer_label,
            Style::default().fg(TEXT).bold(),
        )]
    };
    if let Some(usage) = state.context_usage
        && let Some(label) = context_usage_label(usage.estimated_tokens, usage.context_window)
    {
        let color = context_usage_color(usage.estimated_tokens, usage.context_window);
        title.push(Span::styled("· ", Style::default().fg(MUTED)));
        title.push(Span::styled(label, Style::default().fg(color)));
    }
    let block = overlay_block(
        Line::from(title),
        if state.is_busy() || !state.client.editor.is_blank() {
            ACCENT
        } else {
            BORDER
        },
    );
    let inner = block.inner(area);
    frame.render_widget(block, area);

    if inner.width == 0 || inner.height == 0 {
        return None;
    }
    let window = state.client.editor.window(inner.height, inner.width);
    let lines = window
        .lines
        .into_iter()
        .zip(window.prompt_line_starts)
        .map(|(line, first_prompt_line)| styled_composer_line(line, first_prompt_line))
        .collect::<Vec<_>>();
    frame.render_widget(Paragraph::new(Text::from(lines)), inner);
    Some(Position::new(
        inner.x.saturating_add(window.cursor_x),
        inner.y.saturating_add(window.cursor_y),
    ))
}

fn context_usage_label(estimated_tokens: u64, context_window: Option<u64>) -> Option<String> {
    match context_window {
        Some(context_window) => Some(format!(
            "CTX ~{} / {} ",
            compact_token_count(estimated_tokens),
            compact_token_count(context_window)
        )),
        None if estimated_tokens > 0 => {
            Some(format!("CTX ~{} ", compact_token_count(estimated_tokens)))
        }
        None => None,
    }
}

fn context_usage_color(estimated_tokens: u64, context_window: Option<u64>) -> Color {
    let Some(context_window) = context_window.filter(|window| *window > 0) else {
        return MUTED;
    };
    if estimated_tokens >= context_window.saturating_mul(9) / 10 {
        DANGER
    } else if estimated_tokens >= context_window.saturating_mul(3) / 4 {
        WARNING
    } else {
        MUTED
    }
}

fn compact_token_count(tokens: u64) -> String {
    fn scaled(tokens: u64, divisor: u64, suffix: char) -> String {
        let tenths = tokens.saturating_mul(10).saturating_add(divisor / 2) / divisor;
        if tenths.is_multiple_of(10) {
            format!("{}{suffix}", tenths / 10)
        } else {
            format!("{}.{:01}{suffix}", tenths / 10, tenths % 10)
        }
    }

    if tokens >= 1_000_000 {
        scaled(tokens, 1_000_000, 'm')
    } else if tokens >= 1_000 {
        scaled(tokens, 1_000, 'k')
    } else {
        tokens.to_string()
    }
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

fn render_command_completions(frame: &mut Frame<'_>, composer_area: Rect, state: &TuiState) {
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
                format!("{:<12}", completion.replacement()),
                Style::default().fg(ACCENT_BRIGHT).bold(),
            ),
            Span::styled(completion.description(), Style::default().fg(MUTED)),
        ]))
        .style(Style::default().bg(if is_selected { SURFACE_RAISED } else { SURFACE }))
    });
    let block = overlay_block(" Commands and skills · ↑/↓ select · Tab complete ", ACCENT);
    frame.render_widget(List::new(items).block(block), popup);
}

fn render_prompt_metadata(frame: &mut Frame<'_>, area: Rect, state: &TuiState) {
    if state.status_message.starts_with("Reloaded ") {
        let line = Line::from(vec![
            Span::styled(" ✓ ", Style::default().fg(SUCCESS).bold()),
            Span::styled(state.status_message.as_str(), Style::default().fg(SUCCESS)),
        ]);
        frame.render_widget(
            Paragraph::new(line).style(Style::default().bg(SURFACE)),
            area,
        );
        return;
    }

    let model = state
        .selected_model_display_name()
        .unwrap_or_else(|| "Default".to_owned());
    let mut spans = vec![
        Span::styled(" Model: ", Style::default().fg(MUTED)),
        Span::styled(model, Style::default().fg(TEXT)),
    ];
    if state.selected_model_uses_fast_mode() {
        spans.push(Span::styled(" ⚡", Style::default().fg(ACCENT)));
    }
    spans.extend([
        Span::styled(" · Directory: ", Style::default().fg(MUTED)),
        Span::styled(state.workspace.as_str(), Style::default().fg(TEXT)),
    ]);
    let line = Line::from(spans);
    frame.render_widget(
        Paragraph::new(line).style(Style::default().bg(SURFACE)),
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

fn render_settings(frame: &mut Frame<'_>, area: Rect, state: &TuiState) {
    let popup = centered(area, 76, 22);
    frame.render_widget(Clear, popup);
    let settings = state.client.settings.as_ref().expect("settings checked");
    let lines = match settings.view {
        SettingsView::Menu => settings_menu_lines(settings),
        SettingsView::Addons => settings_addon_lines(settings),
        SettingsView::WebBrowsing => settings_web_browsing_lines(settings),
        SettingsView::Vision => settings_vision_lines(settings),
        SettingsView::Memory => settings_memory_lines(settings),
        SettingsView::TerminalImages => settings_terminal_image_lines(settings),
    };
    frame.render_widget(
        Paragraph::new(lines).block(overlay_block(" Settings ", ACCENT)),
        popup,
    );
}

fn settings_menu_lines(settings: &SettingsState) -> Vec<Line<'static>> {
    let mut lines = vec![
        Line::from(vec![
            Span::styled("Search: ", Style::default().fg(MUTED)),
            Span::styled(settings.query.clone(), Style::default().fg(TEXT)),
        ]),
        Line::default(),
    ];
    let sections = settings.filtered_sections();
    for (index, section) in sections.iter().enumerate() {
        let selected = index == settings.selected;
        lines.push(
            Line::from(vec![
                Span::styled(
                    if selected { "› " } else { "  " },
                    Style::default().fg(if selected { ACCENT } else { MUTED }),
                ),
                Span::styled(
                    format!("{:<12}", section.label()),
                    Style::default()
                        .fg(if selected { TEXT } else { MUTED })
                        .add_modifier(if selected {
                            Modifier::BOLD
                        } else {
                            Modifier::empty()
                        }),
                ),
                Span::styled(section.description(), Style::default().fg(MUTED)),
            ])
            .style(Style::default().bg(if selected {
                SURFACE_RAISED
            } else {
                SURFACE
            })),
        );
    }
    if sections.is_empty() {
        lines.push(Line::styled(
            "  No matching settings",
            Style::default().fg(DANGER),
        ));
    }
    lines.extend([
        Line::default(),
        Line::styled(
            "Type to search · ↑/↓ select · Enter open · Esc close",
            Style::default().fg(MUTED),
        ),
    ]);
    lines
}

fn settings_addon_lines(settings: &SettingsState) -> Vec<Line<'static>> {
    let rows = [
        ("Web browsing", settings.web.backend.label().to_owned()),
        (
            "Vision",
            settings
                .vision
                .model
                .clone()
                .unwrap_or_else(|| "Disabled".to_owned()),
        ),
        ("Memory", settings.memory.backend.label().to_owned()),
        (
            "Terminal images",
            terminal_image_mode_label(settings.terminal_images).to_owned(),
        ),
    ];
    let mut lines = vec![
        Line::styled("Add-ons", Style::default().fg(TEXT).bold()),
        Line::default(),
    ];
    for (index, (label, value)) in rows.into_iter().enumerate() {
        lines.push(settings_row(label, &value, settings.selected == index));
        lines.push(Line::default());
    }
    lines.push(Line::styled(
        "Enter open · ↑/↓ select · Esc back",
        Style::default().fg(MUTED),
    ));
    lines
}

fn settings_memory_lines(settings: &SettingsState) -> Vec<Line<'static>> {
    let mut lines = vec![
        Line::styled("Memory", Style::default().fg(TEXT).bold()),
        Line::default(),
        settings_row(
            "Provider",
            settings.memory.backend.label(),
            settings.addon_field == 0,
        ),
        Line::default(),
    ];
    if settings.memory.backend == MemoryBackend::Disabled {
        lines.push(settings_status_row("Status", "Disabled", MUTED));
    } else {
        lines.push(settings_row(
            "Executable",
            &settings.memory.executable,
            settings.addon_field == 1,
        ));
        lines.push(settings_row(
            "Global bank",
            &settings.memory.global_bank,
            settings.addon_field == 2,
        ));
        lines.push(settings_row(
            "Data directory",
            if settings.memory.data_directory.is_empty() {
                "Mnemosyne default"
            } else {
                &settings.memory.data_directory
            },
            settings.addon_field == 3,
        ));
        let (status, color) = if !settings.memory.configured() {
            ("Setup required", WARNING)
        } else if settings.memory.available() {
            ("Available", SUCCESS)
        } else {
            ("Executable not detected", WARNING)
        };
        lines.push(Line::default());
        lines.push(settings_status_row(
            "Bank format",
            "Up to 64 letters, numbers, hyphens, or underscores",
            MUTED,
        ));
        lines.push(settings_status_row(
            "Scopes",
            "Project (managed) + global",
            MUTED,
        ));
        lines.push(settings_status_row("Status", status, color));
        lines.push(settings_status_row(
            "Install",
            "uv tool install 'mnemosyne-memory[mcp]'",
            MUTED,
        ));
    }
    lines.extend([
        Line::default(),
        Line::styled(
            "←/→ provider · ↑/↓ field · type to edit · Esc save",
            Style::default().fg(MUTED),
        ),
    ]);
    lines
}

fn settings_terminal_image_lines(settings: &SettingsState) -> Vec<Line<'static>> {
    vec![
        Line::styled("Terminal images", Style::default().fg(TEXT).bold()),
        Line::default(),
        settings_row(
            "Previews",
            terminal_image_mode_label(settings.terminal_images),
            true,
        ),
        Line::default(),
        settings_status_row("Applies", "Next launch", MUTED),
        settings_status_row("Fallback", "Attachment labels", MUTED),
        Line::default(),
        Line::styled(
            "Enter toggle · ←/→ change · Esc back",
            Style::default().fg(MUTED),
        ),
    ]
}

fn settings_vision_lines(settings: &SettingsState) -> Vec<Line<'static>> {
    let model = settings.vision.model.as_deref().unwrap_or("Disabled");
    vec![
        Line::styled("Vision", Style::default().fg(TEXT).bold()),
        Line::default(),
        settings_row("Model", model, true),
        Line::default(),
        settings_status_row(
            "Status",
            if settings.vision.is_enabled() {
                "Configured"
            } else {
                "Disabled"
            },
            if settings.vision.is_enabled() {
                SUCCESS
            } else {
                MUTED
            },
        ),
        Line::default(),
        Line::styled(
            "Enter select model · Ctrl+U disable · Esc back",
            Style::default().fg(MUTED),
        ),
    ]
}

fn settings_web_browsing_lines(settings: &SettingsState) -> Vec<Line<'static>> {
    let mut lines = vec![
        Line::styled("Web browsing", Style::default().fg(TEXT).bold()),
        Line::default(),
        settings_row(
            "Backend",
            settings.web.backend.label(),
            settings.addon_field == 0,
        ),
        Line::default(),
    ];
    match settings.web.backend {
        WebBackend::Disabled => {
            lines.push(settings_status_row("Status", "Disabled", MUTED));
        }
        WebBackend::AgentBrowser => {
            let (status, color) = match &settings.agent_browser_status {
                AgentBrowserStatus::Checking => ("Checking…".to_owned(), MUTED),
                AgentBrowserStatus::Available(version) => {
                    (format!("Available · {version}"), SUCCESS)
                }
                AgentBrowserStatus::Unavailable => ("Not detected".to_owned(), WARNING),
            };
            lines.push(settings_status_row("Status", &status, color));
            lines.push(settings_status_row(
                "Requirement",
                "agent-browser executable on PATH",
                MUTED,
            ));
        }
        WebBackend::Firecrawl => {
            let masked = if settings.web.firecrawl_api_key.is_empty() {
                "not set".to_owned()
            } else {
                "•".repeat(settings.web.firecrawl_api_key.chars().count().min(32))
            };
            lines.push(settings_row("API key", &masked, settings.addon_field == 1));
            let (status, color) = if settings.web.firecrawl_api_key.is_empty() {
                ("Setup required", WARNING)
            } else {
                ("Configured", SUCCESS)
            };
            lines.push(settings_status_row("Status", status, color));
        }
    }
    lines.extend([
        Line::default(),
        Line::styled(
            "Enter toggle · ↑/↓ field · Esc back",
            Style::default().fg(MUTED),
        ),
    ]);
    lines
}

fn settings_status_row(label: &str, value: &str, color: Color) -> Line<'static> {
    Line::from(vec![
        Span::styled("  ", Style::default()),
        Span::styled(format!("{label:<18}"), Style::default().fg(MUTED)),
        Span::styled(value.to_owned(), Style::default().fg(color)),
    ])
}

fn settings_row(label: &str, value: &str, selected: bool) -> Line<'static> {
    Line::from(vec![
        Span::styled(
            if selected { "› " } else { "  " },
            Style::default().fg(ACCENT),
        ),
        Span::styled(format!("{label:<18}"), Style::default().fg(MUTED)),
        Span::styled(
            value.to_owned(),
            Style::default()
                .fg(if selected { TEXT } else { MUTED })
                .bold(),
        ),
    ])
    .style(Style::default().bg(if selected { SURFACE_RAISED } else { SURFACE }))
}

fn render_provider_picker(
    frame: &mut Frame<'_>,
    area: Rect,
    state: &TuiState,
    output: &mut RenderOutput,
) {
    output.oauth_link_hit_region = None;
    output.api_key_input_hit_region = None;
    let picker = state
        .client
        .provider_picker
        .as_ref()
        .expect("picker checked");
    if picker.showing_details {
        let picker = picker.clone();
        render_provider_details(frame, area, state, &picker, output);
        return;
    }
    let popup = centered(area, 68, 14);
    frame.render_widget(Clear, popup);
    let mut lines = vec![
        Line::styled(
            if state.current_menu_has_parent() {
                "↑/↓ select · Enter details · Esc back"
            } else {
                "↑/↓ select · Enter details · Esc close"
            },
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
            let state_label = if !provider.credential_configured {
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
                        } else if !provider.credential_configured {
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

fn render_agent_picker(frame: &mut Frame<'_>, area: Rect, state: &TuiState) {
    let popup = centered(area, 82, 24);
    frame.render_widget(Clear, popup);
    let picker = state.client.agent_picker.as_ref().expect("picker checked");
    let lines = if let Some(editor) = &picker.editor {
        agent_editor_lines(editor)
    } else {
        agent_list_lines(picker, state.current_menu_has_parent())
    };
    frame.render_widget(
        Paragraph::new(lines).block(overlay_block(" Agents ", ACCENT)),
        popup,
    );
    if let Some(dropdown) = picker
        .editor
        .as_ref()
        .and_then(|editor| editor.model_dropdown.as_ref())
    {
        let dropdown_popup = centered(area, 68, 18);
        render_searchable_dropdown(
            frame,
            dropdown_popup,
            dropdown,
            " Select Agent Model ",
            "No matching models",
            AgentModelOption::search_text,
            |option, selected| {
                Line::from(vec![
                    Span::styled(
                        if selected { "› " } else { "  " },
                        Style::default().fg(if selected { ACCENT } else { MUTED }),
                    ),
                    Span::styled(
                        option.label(),
                        Style::default()
                            .fg(if selected { TEXT } else { MUTED })
                            .add_modifier(if selected {
                                Modifier::BOLD
                            } else {
                                Modifier::empty()
                            }),
                    ),
                    Span::styled(format!("  {}", option.detail()), Style::default().fg(MUTED)),
                ])
            },
        );
    } else if let Some(pending) = picker
        .editor
        .as_ref()
        .and_then(|editor| editor.pending_options.as_ref())
    {
        render_model_options_popup(
            frame,
            centered(area, 72, 18),
            &pending.options,
            pending.selected,
            !pending.reasoning_efforts.is_empty(),
            pending.fast_mode_configurable,
            "Configure how this agent runs its model",
        );
    }
}

fn render_searchable_dropdown<T, S, R>(
    frame: &mut Frame<'_>,
    popup: Rect,
    dropdown: &crate::searchable_dropdown::SearchableDropdown<T>,
    title: &str,
    empty_message: &str,
    search_text: S,
    row: R,
) where
    S: Fn(&T) -> String,
    R: Fn(&T, bool) -> Line<'static>,
{
    frame.render_widget(Clear, popup);
    let filtered = dropdown.filtered_items(search_text);
    let visible_rows = usize::from(popup.height.saturating_sub(6)).max(1);
    let start = scroll_start(dropdown.selected, filtered.len(), visible_rows);
    let mut lines = vec![
        Line::from(vec![
            Span::styled("Search: ", Style::default().fg(ACCENT_BRIGHT).bold()),
            Span::styled(&dropdown.query, Style::default().fg(TEXT)),
        ])
        .style(Style::default().bg(SURFACE_RAISED)),
        Line::default(),
    ];
    if filtered.is_empty() {
        lines.push(Line::styled(
            empty_message.to_owned(),
            Style::default().fg(DANGER),
        ));
    } else {
        lines.extend(
            filtered
                .iter()
                .enumerate()
                .skip(start)
                .take(visible_rows)
                .map(|(index, item)| row(item, index == dropdown.selected)),
        );
    }
    lines.push(Line::default());
    lines.push(Line::styled(
        "Type to search · ↑/↓ select · Enter apply · Esc cancel",
        Style::default().fg(MUTED),
    ));
    frame.render_widget(
        Paragraph::new(lines).block(overlay_block(title.to_owned(), ACCENT)),
        popup,
    );
    let query_width =
        u16::try_from(UnicodeWidthStr::width(dropdown.query.as_str())).unwrap_or(u16::MAX);
    let input_start = popup.x.saturating_add(1).saturating_add(8);
    let input_end = popup.right().saturating_sub(2);
    frame.set_cursor_position(Position::new(
        input_start.saturating_add(query_width).min(input_end),
        popup.y.saturating_add(1),
    ));
}

fn agent_editor_lines(editor: &AgentEditor) -> Vec<Line<'static>> {
    let mut lines = vec![
        Line::styled(
            "Tab/↑/↓ field · type/paste · Enter choose model + options · autosaves · Esc back",
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
    for (field, value) in AgentEditorField::ALL.into_iter().zip(values) {
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
    // What the chosen model was actually given. Both facts are set in the options step behind the
    // Model field, so this is the one line they can be read back from; nothing is said when there is
    // nothing to say, which is also what an archetype with no model of its own has.
    let mut carried = Vec::new();
    if let Some(effort) = editor.reasoning_effort.as_deref() {
        carried.push(format!("effort {effort}"));
    }
    if editor.fast_mode {
        carried.push("⚡ fast mode".to_owned());
    }
    if !carried.is_empty() {
        lines.push(Line::styled(
            format!("Runs at {}", carried.join(" · ")),
            Style::default().fg(ACCENT),
        ));
    }
    lines
}

fn agent_list_lines(picker: &AgentPicker, has_parent: bool) -> Vec<Line<'static>> {
    let mut lines = vec![
        Line::styled(
            if has_parent {
                "↑/↓ select · Enter edit · n new · d delete · Esc back"
            } else {
                "↑/↓ select · Enter edit · n new · d delete · Esc close"
            },
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
        let models = std::iter::once(
            agent
                .model_id
                .as_ref()
                .map_or("inherit parent model", nakode_protocol::ModelId::as_str),
        )
        .chain(
            agent
                .fallback_models
                .iter()
                .map(nakode_protocol::ModelId::as_str),
        )
        .collect::<Vec<_>>()
        .join(" → ");
        let models = if agent.fast_mode {
            format!("{models}  ⚡ fast")
        } else {
            models
        };
        lines.push(Line::styled(
            format!("    {models}"),
            Style::default().fg(if selected { ACCENT_DEEP } else { MUTED }),
        ));
    }
    lines
}

fn agent_list_row(agent: &nakode_protocol::AgentDefinitionView, selected: bool) -> Line<'static> {
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
    state: &TuiState,
    picker: &ProviderPicker,
    output: &mut RenderOutput,
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
    let connection = connection_label(&provider.connection);
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
            Span::styled(provider.id.as_str(), Style::default().fg(TEXT)),
        ]),
        Line::from(vec![
            Span::styled("Credential ", Style::default().fg(MUTED)),
            Span::styled(
                provider
                    .credential_kind
                    .as_deref()
                    .unwrap_or("not configured"),
                Style::default().fg(if provider.credential_configured {
                    SUCCESS
                } else {
                    WARNING
                }),
            ),
        ]),
    ];
    append_provider_capabilities(&mut lines, state, provider);
    let mut api_key_input_line = None;
    let authentication_url_line = if let Some(authentication) = &picker.authentication {
        let first_line = lines.len();
        append_provider_authentication(&mut lines, authentication);
        if matches!(authentication, ProviderAuthentication::ApiKeyInput { .. }) {
            api_key_input_line = Some(first_line + 2);
        }
        matches!(
            authentication,
            ProviderAuthentication::Challenge { .. } | ProviderAuthentication::ApiKeyInput { .. }
        )
        .then_some(first_line + 1)
    } else {
        None
    };
    if !matches!(
        picker.authentication,
        Some(ProviderAuthentication::ApiKeyInput { .. })
    ) {
        append_provider_actions(&mut lines, provider.credential_configured);
    }
    let block = overlay_block(format!(" {} ", provider.display_name), ACCENT);
    frame.render_widget(
        Paragraph::new(lines)
            .block(block)
            .wrap(Wrap { trim: false }),
        popup,
    );
    register_oauth_link(
        output,
        popup,
        authentication_url_line,
        picker.authentication.as_ref(),
        provider_dashboard_url(provider),
    );
    register_api_key_input(output, popup, api_key_input_line);
}

fn append_provider_capabilities(
    lines: &mut Vec<Line<'_>>,
    _state: &TuiState,
    provider: &nakode_protocol::ProviderView,
) {
    if !provider.credential_configured {
        return;
    }
    lines.push(Line::default());
    lines.push(Line::styled(
        "Capabilities",
        Style::default().fg(ACCENT_BRIGHT).bold(),
    ));
    for (name, capability) in provider_capability_rows() {
        let supported = provider.capabilities.supports(capability);
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
}

fn register_api_key_input(output: &mut RenderOutput, popup: Rect, line: Option<usize>) {
    let Some(line) = line else {
        return;
    };
    let row = popup
        .y
        .saturating_add(1)
        .saturating_add(u16::try_from(line).unwrap_or(u16::MAX));
    output.api_key_input_hit_region = Some((
        ScreenPoint::new(popup.x.saturating_add(1), row),
        ScreenPoint::new(popup.right().saturating_sub(1), row.saturating_add(1)),
    ));
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
    output: &mut RenderOutput,
    popup: Rect,
    line: Option<usize>,
    authentication: Option<&ProviderAuthentication>,
    api_key_url: Option<&str>,
) {
    let Some(line) = line else {
        return;
    };
    let url = match authentication {
        Some(ProviderAuthentication::Challenge {
            verification_url, ..
        }) => verification_url.clone(),
        Some(ProviderAuthentication::ApiKeyInput { .. }) => {
            let Some(url) = api_key_url else { return };
            url.to_owned()
        }
        Some(ProviderAuthentication::Starting) | None => return,
    };
    let row = popup
        .y
        .saturating_add(1)
        .saturating_add(u16::try_from(line).unwrap_or(u16::MAX));
    output.oauth_link_hit_region = Some((
        url,
        ScreenPoint::new(popup.x.saturating_add(1), row),
        ScreenPoint::new(popup.right().saturating_sub(1), row.saturating_add(1)),
    ));
}

fn append_provider_authentication<'a>(
    lines: &mut Vec<Line<'a>>,
    authentication: &'a ProviderAuthentication,
) {
    lines.push(Line::default());
    match authentication {
        ProviderAuthentication::Starting => lines.push(Line::styled(
            "Saving or starting provider authentication…",
            Style::default().fg(WARNING),
        )),
        ProviderAuthentication::ApiKeyInput { value, focused } => {
            lines.push(Line::styled(
                "[o] Get API key ↗",
                Style::default()
                    .fg(ACCENT_BRIGHT)
                    .add_modifier(Modifier::BOLD | Modifier::UNDERLINED),
            ));
            let masked = if value.is_empty() {
                if *focused {
                    " █".to_owned()
                } else {
                    " Add API key".to_owned()
                }
            } else {
                format!(" {}█", "•".repeat(value.chars().count().min(48)))
            };
            lines.push(Line::from(vec![
                Span::styled("API key  ", Style::default().fg(MUTED)),
                Span::styled(
                    "[",
                    Style::default().fg(if *focused { ACCENT_BRIGHT } else { MUTED }),
                ),
                Span::styled(masked, Style::default().fg(TEXT).bold()),
                Span::styled(
                    " ]",
                    Style::default().fg(if *focused { ACCENT_BRIGHT } else { MUTED }),
                ),
            ]));
        }
        ProviderAuthentication::Challenge {
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

fn render_approval(frame: &mut Frame<'_>, area: Rect, approval: &InteractionView) {
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

fn render_question(frame: &mut Frame<'_>, area: Rect, prompt: &QuestionPrompt) {
    let description_count = prompt
        .interaction
        .options
        .iter()
        .filter(|option| option.description.is_some())
        .count();
    let height = u16::try_from(prompt.interaction.options.len() + description_count)
        .unwrap_or(8)
        .saturating_add(8)
        .min(20);
    let popup = centered(area, 76, height);
    frame.render_widget(Clear, popup);
    let mut lines = vec![
        Line::styled(&prompt.interaction.detail, Style::default().fg(TEXT)),
        Line::default(),
    ];
    for (index, option) in prompt.interaction.options.iter().enumerate() {
        let selected = index == prompt.selected;
        let checked = prompt.selections.get(index).copied().unwrap_or(false);
        let marker = if prompt.interaction.multiple {
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
                if option.recommended {
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
        if prompt.interaction.multiple {
            " ↑/↓ select · Space toggle · Enter confirm "
        } else {
            " ↑/↓ select · Enter choose · 1-8 quick select "
        },
        Style::default().fg(ACCENT),
    ));
    frame.render_widget(
        Paragraph::new(Text::from(lines))
            .block(overlay_block(
                format!(" {} ", prompt.interaction.title),
                ACCENT,
            ))
            .wrap(Wrap { trim: false }),
        popup,
    );
}

fn render_session_picker(frame: &mut Frame<'_>, area: Rect, state: &TuiState) {
    let popup = centered(area, 78, 18);
    frame.render_widget(Clear, popup);
    let picker = state
        .client
        .session_picker
        .as_ref()
        .expect("picker checked");
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
            let short_id = session.id.as_str().get(..8).unwrap_or(session.id.as_str());
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
                        format!("  {short_id}  {}", relative_time(session.updated_at_ms)),
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

fn relative_time(timestamp_ms: i64) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .try_into()
        .unwrap_or(i64::MAX);
    let age = now.saturating_sub(timestamp_ms.saturating_div(1_000));
    match age {
        0..=59 => "now".to_owned(),
        60..=3_599 => format!("{}m ago", age / 60),
        3_600..=86_399 => format!("{}h ago", age / 3_600),
        _ => format!("{}d ago", age / 86_400),
    }
}

/// The options step: a row per thing the model takes, and nothing for what it does not.
///
/// `effort_row` and `fast_row` are what the CHOSEN model reports, so the same popup draws the
/// session's two rows, a Cursor model's fast-mode-only row, and an agent archetype's own pair. The
/// rows are indexed in the order they are pushed, which is what `option_selected` counts.
fn render_model_options_popup(
    frame: &mut Frame<'_>,
    popup: Rect,
    options: &ModelOptions,
    option_selected: usize,
    effort_row: bool,
    fast_row: bool,
    description: &str,
) {
    frame.render_widget(Clear, popup);
    // No level set means the model's own default — say that, rather than naming a level nobody chose.
    let effort = options.reasoning_effort.as_deref().unwrap_or("default");
    let fast = if options.fast_mode { "⚡ on" } else { "off" };
    let option_line = |index: usize, label: &str, value: &str| {
        let selected = option_selected == index;
        Line::from(vec![
            Span::styled(
                if selected { "› " } else { "  " },
                Style::default().fg(if selected { ACCENT } else { MUTED }),
            ),
            Span::styled(
                format!("{label}: "),
                Style::default()
                    .fg(if selected { TEXT } else { MUTED })
                    .add_modifier(if selected {
                        Modifier::BOLD
                    } else {
                        Modifier::empty()
                    }),
            ),
            Span::styled(
                value.to_owned(),
                Style::default().fg(if selected { ACCENT } else { MUTED }),
            ),
        ])
    };
    let mut lines = vec![
        Line::styled(description.to_owned(), Style::default().fg(MUTED)),
        Line::default(),
    ];
    let mut row = 0;
    if effort_row {
        lines.push(option_line(row, "Reasoning effort", effort));
        row = row.saturating_add(1);
    }
    if fast_row {
        lines.push(option_line(row, "Fast mode", fast));
    }
    lines.extend([
        Line::default(),
        Line::styled(
            "↑/↓ select · ←/→ change · Enter apply · Esc back",
            Style::default().fg(MUTED),
        ),
    ]);
    frame.render_widget(
        Paragraph::new(lines)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(" Model Options "),
            )
            .style(Style::default().bg(SURFACE)),
        popup,
    );
}

#[allow(clippy::too_many_lines)]
fn render_model_picker(frame: &mut Frame<'_>, area: Rect, state: &TuiState) {
    let popup = centered(area, 72, 18);
    frame.render_widget(Clear, popup);
    let picker = state.client.model_picker.as_ref().expect("picker checked");
    if picker.stage == ModelPickerStage::Options {
        render_model_options_popup(
            frame,
            popup,
            &picker.options,
            picker.option_selected,
            !picker.options_fast_only,
            true,
            match (picker.scope, picker.options_fast_only) {
                (ModelSelectionScope::Session, true) => "Configure this session's Cursor model",
                (ModelSelectionScope::Session, false) => "Configure this session's OpenAI model",
                (_, true) => "Configure the Cursor model default",
                (_, false) => "Configure the OpenAI model default",
            },
        );
        return;
    }
    let filtered = state.filtered_models();
    // The popup's own height, less the two border rows, the filter line and the blank under it, and the
    // blank and footer beneath the list. What is left is the list's window.
    let visible_rows = usize::from(popup.height.saturating_sub(6)).max(1);
    let start = scroll_start(picker.selected, filtered.len(), visible_rows);
    let mut lines = vec![Line::from(vec![
        Span::styled("Filter: ", Style::default().fg(MUTED)),
        Span::styled(&picker.filter, Style::default().fg(TEXT)),
        // Where the window sits in the catalogue, on the line that is already there. A terminal list has
        // no scrollbar to show that it moved, and the filter narrows what is being counted, so the pair
        // is the only thing that says both "there is more" and "your search cut it down".
        Span::styled(
            if filtered.len() > visible_rows {
                format!(
                    "   {} of {}",
                    picker.selected.saturating_add(1),
                    filtered.len()
                )
            } else {
                String::new()
            },
            Style::default().fg(MUTED),
        ),
    ])];
    lines.push(Line::default());
    for (index, model) in filtered.iter().enumerate().skip(start).take(visible_rows) {
        let selected = index == picker.selected;
        let marker = if selected { "› " } else { "  " };
        let display = &model.display_name;
        let fast = state.model_uses_fast_mode(model);
        let current = if state.selected_model.as_ref() == Some(&model.id) {
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
                    display,
                    Style::default()
                        .fg(if selected { TEXT } else { MUTED })
                        .add_modifier(if selected {
                            Modifier::BOLD
                        } else {
                            Modifier::empty()
                        }),
                ),
                Span::styled(
                    format!("  {}", model.provider_id),
                    Style::default().fg(MUTED),
                ),
                Span::styled(if fast { "  ⚡" } else { "" }, Style::default().fg(ACCENT)),
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
        if state.current_menu_has_parent() {
            "Type to filter · ↑/↓ select · Enter apply · Esc back"
        } else {
            "Type to filter · ↑/↓ select · Enter apply · Esc cancel"
        },
        Style::default().fg(MUTED),
    ));

    let title = match picker.scope {
        ModelSelectionScope::Default => " Default Model ",
        ModelSelectionScope::Session => " Switch Session Model ",
        ModelSelectionScope::Vision => " Vision Model ",
    };
    let block = overlay_block(title, ACCENT);
    frame.render_widget(Paragraph::new(lines).block(block), popup);
}

fn transcript_line(line: ProjectedLine) -> Line<'static> {
    let color = match line.tone {
        LineTone::Muted | LineTone::Reasoning => MUTED,
        LineTone::User | LineTone::AgentPending => ACCENT_BRIGHT,
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
    if line.spans.is_empty() {
        let animated_tool = line.tone == LineTone::Tool && line.text.starts_with('⠋');
        let text = if animated_tool
            || matches!(
                line.tone,
                LineTone::AgentPending | LineTone::SubagentPending
            ) {
            line.text.replacen('⠋', spinner_frame(), 1)
        } else {
            line.text
        };
        Line::styled(text, style)
    } else {
        Line::from(
            line.spans
                .into_iter()
                .map(|span| markdown_span(span, style))
                .collect::<Vec<_>>(),
        )
    }
}

fn markdown_span(span: MarkdownSpan, mut style: Style) -> Span<'static> {
    if let Some(tone) = span.style.tone {
        let color = match tone {
            MarkdownTone::Accent | MarkdownTone::Link => ACCENT_BRIGHT,
            MarkdownTone::Muted => MUTED,
            MarkdownTone::Success => SUCCESS,
            MarkdownTone::Warning => WARNING,
            MarkdownTone::Rgb(red, green, blue) => Color::Rgb(red, green, blue),
        };
        style = style.fg(color);
    }
    if span.style.modifiers.contains(MarkdownModifier::Bold) {
        style = style.add_modifier(Modifier::BOLD);
    }
    if span.style.modifiers.contains(MarkdownModifier::Italic) {
        style = style.add_modifier(Modifier::ITALIC);
    }
    if span.style.modifiers.contains(MarkdownModifier::Underlined) {
        style = style.add_modifier(Modifier::UNDERLINED);
    }
    if span.style.modifiers.contains(MarkdownModifier::CrossedOut) {
        style = style.add_modifier(Modifier::CROSSED_OUT);
    }
    if span.style.code {
        style = style.bg(SURFACE_RAISED);
    }
    Span::styled(span.text, style)
}

fn spinner_frame() -> &'static str {
    let tick = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis() / 100);
    let frame = usize::try_from(tick % SPINNER_FRAMES.len() as u128).unwrap_or(0);
    SPINNER_FRAMES[frame]
}

/// First row of the window a list of `length` rows shows inside `visible_rows`, given the selection.
///
/// A `Paragraph` draws the lines it is handed and silently loses the rest to the bottom of its block,
/// so a list longer than its popup is not scrolled — it is CUT, taking every row past the first
/// screenful and the footer under them with it. Every overlay that draws a selectable list therefore
/// has to window the list itself, and this is the one place that decides how.
///
/// The window is a pure function of the selection because that is all these overlays keep: the
/// selected index is the scroll position, so nothing can drift out of step with what is drawn. It
/// stays put until the selection would leave the bottom of it, which is what makes holding ↓ walk the
/// list a row at a time instead of paging it, and it is clamped so the last screenful is a full one.
fn scroll_start(selected: usize, length: usize, visible_rows: usize) -> usize {
    selected
        .saturating_sub(visible_rows.saturating_sub(1))
        .min(length.saturating_sub(visible_rows))
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
    use std::collections::BTreeSet;

    use nakode_protocol::{
        AgentBrowserView, BootstrapView, ConnectionView, ContextUsageView, MemorySettingsView,
        ModelConfigurationView, ModelId, ModelOptions, ModelView, ProviderAuthenticationView,
        ProviderCapabilities, ProviderCapability, ProviderId, ProviderView, RunId, RunStatus,
        RunView, SessionActivity, SessionId, SessionView, SettingsView as ProtocolSettingsView,
        TerminalImageModeView, TodoItemView, TodoPhaseView, TodoStatusView, TranscriptEntryKind,
        TranscriptEntryStatus, TranscriptEntryView, TranscriptPage, VisionSettingsView,
        WebSettingsView, WorkspaceId,
    };
    use ratatui::{Terminal, backend::TestBackend};

    use crate::{
        transcript::{
            LineTone, MarkdownModifier, MarkdownSpan, MarkdownStyle, MarkdownTone, ProjectedLine,
        },
        tui_state::{
            AgentEditorField, ModelPicker, ModelPickerStage, ModelSelectionScope, SettingsView,
            TuiState,
        },
    };

    fn bootstrap() -> BootstrapView {
        BootstrapView {
            workspace_id: WorkspaceId::from("workspace"),
            workspace_path: "/tmp/project".to_owned(),
            providers: Vec::new(),
            models: Vec::new(),
            agents: Vec::new(),
            skills: Vec::new(),
            settings: ProtocolSettingsView {
                web: WebSettingsView {
                    backend: "disabled".to_owned(),
                    credential_configured: false,
                    agent_browser: AgentBrowserView::Unavailable,
                },
                memory: MemorySettingsView {
                    backend: "disabled".to_owned(),
                    executable: String::new(),
                    global_bank: String::new(),
                    data_directory: String::new(),
                    configured: false,
                    available: false,
                },
                vision: VisionSettingsView { model_id: None },
                terminal_images: TerminalImageModeView::Auto,
            },
            sessions: Vec::new(),
            active_session: Some(session()),
            session_bridges: Vec::new(),
        }
    }

    fn session() -> SessionView {
        SessionView {
            id: SessionId::from("session"),
            revision: 1,
            workspace_id: WorkspaceId::from("workspace"),
            title: "Session".to_owned(),
            status_message: String::new(),
            diagnostic_count: 0,
            activity: SessionActivity::Idle,
            selected_provider_id: None,
            selected_model_id: None,
            selected_model_options: nakode_protocol::ModelOptions::default(),
            active_agent_session: None,
            active_turn: None,
            last_turn: None,
            next_turn_configuration_pending: false,
            next_turn_transition: None,
            context_usage: None,
            transcript: empty_transcript(),
            recoverable_prompt: None,
            queue: Vec::new(),
            interactions: Vec::new(),
            todos: Vec::new(),
            runs: Vec::new(),
            runs_has_earlier: false,
            notices: Vec::new(),
            external_tool_calls: Vec::new(),
            created_at_ms: 0,
            updated_at_ms: 0,
        }
    }

    fn empty_transcript() -> TranscriptPage {
        TranscriptPage {
            entries: Vec::new(),
            has_earlier: false,
            stream_active: false,
            stream_label: "Nakode".to_owned(),
        }
    }

    fn state() -> TuiState {
        TuiState::from_bootstrap(&bootstrap(), 2_000)
    }

    fn model(id: &str, display_name: &str, fast_mode: bool) -> ModelView {
        let (provider, model_slug) = id.split_once('/').expect("qualified model");
        ModelView {
            id: ModelId::from(id),
            provider_id: ProviderId::from(provider),
            model_slug: model_slug.to_owned(),
            display_name: display_name.to_owned(),
            is_default: true,
            reasoning_effort: None,
            fast_mode,
            configuration: ModelConfigurationView::default(),
        }
    }

    fn provider(
        id: &str,
        display_name: &str,
        enabled: bool,
        credential_configured: bool,
    ) -> ProviderView {
        ProviderView {
            id: ProviderId::from(id),
            display_name: display_name.to_owned(),
            enabled,
            credential_configured,
            credential_kind: credential_configured.then(|| "test".to_owned()),
            connection: if enabled {
                ConnectionView::Ready
            } else {
                ConnectionView::Disabled
            },
            capabilities: ProviderCapabilities::default(),
            authentication: None,
        }
    }

    fn run(id: &str, objective: &str, status: RunStatus) -> RunView {
        RunView {
            id: RunId::from(id),
            parent_run_id: None,
            agent_slug: "explorer".to_owned(),
            archetype_purpose: "Explore the repository".to_owned(),
            provider_id: ProviderId::from("openai-codex"),
            model_id: None,
            reasoning_effort: None,
            fast_mode: false,
            started_at_ms: 0,
            ended_at_ms: None,
            duration_ms: None,
            termination_kind: None,
            termination_detail: None,
            objective_mismatch_handoff: None,
            policy: nakode_protocol::RunPolicyView::default(),
            tool_denials: Vec::new(),
            tool_denials_retained_total: 0,
            native_session_id: None,
            usage: nakode_protocol::TokenUsageView {
                input_tokens: 0,
                output_tokens: 0,
                cached_input_tokens: 0,
                cache_write_tokens: 0,
            },
            objective: objective.to_owned(),
            objective_start_byte: 0,
            objective_total_bytes: u64::try_from(objective.len()).unwrap_or(u64::MAX),
            status,
            latest_activity: String::new(),
            latest_activity_start_byte: 0,
            latest_activity_total_bytes: 0,
            outcome: None,
            outcome_start_byte: 0,
            outcome_total_bytes: 0,
            result: None,
            result_start_byte: 0,
            result_total_bytes: 0,
            transcript: TranscriptPage {
                entries: vec![TranscriptEntryView {
                    id: "parent-entry".into(),
                    kind: TranscriptEntryKind::User,
                    title: "PARENT".to_owned(),
                    body: format!("Delegated task\n{objective}"),
                    body_start_byte: 0,
                    body_total_bytes: u64::try_from("Delegated task\n".len() + objective.len())
                        .unwrap_or(u64::MAX),
                    status: TranscriptEntryStatus::Complete,
                    artifacts: Vec::new(),
                    provider_id: None,
                    model_id: None,
                    owner_turn_id: None,
                    resolved_reasoning_effort: None,
                    resolved_fast_mode: None,
                    tool_audit_json: None,
                    created_at_ms: None,
                }],
                has_earlier: false,
                stream_active: matches!(status, RunStatus::Starting | RunStatus::Working),
                stream_label: "explorer".to_owned(),
            },
        }
    }

    fn state_with_run(objective: &str, status: RunStatus) -> TuiState {
        let mut view = bootstrap();
        view.active_session
            .as_mut()
            .expect("active session")
            .runs
            .push(run("run-1", objective, status));
        TuiState::from_bootstrap(&view, 2_000)
    }

    #[test]
    fn memory_settings_render_provider_status_and_install_guidance() {
        let mut view = bootstrap();
        view.settings.memory = MemorySettingsView {
            backend: "mnemosyne".to_owned(),
            executable: "/missing/mnemosyne".to_owned(),
            global_bank: "my-global-memory".to_owned(),
            data_directory: String::new(),
            configured: true,
            available: false,
        };
        let mut state = TuiState::from_bootstrap(&view, 100);
        state.open_settings();
        let settings = state.client.settings.as_mut().expect("settings");
        settings.view = SettingsView::Memory;
        let rendered = super::settings_memory_lines(settings)
            .into_iter()
            .flat_map(|line| {
                line.spans
                    .into_iter()
                    .map(|span| span.content.into_owned())
                    .chain(std::iter::once("\n".to_owned()))
            })
            .collect::<String>();
        assert!(rendered.contains("Mnemosyne"));
        assert!(rendered.contains("Global bank"));
        assert!(rendered.contains("my-global-memory"));
        assert!(!rendered.contains("Project bank"));
        assert!(rendered.contains("Project (managed) + global"));
        assert!(rendered.contains("Executable not detected"));
        assert!(rendered.contains("mnemosyne-memory[mcp]"));
    }

    #[test]
    fn main_view_renders_into_a_test_backend() {
        let backend = TestBackend::new(100, 28);
        let mut terminal = Terminal::new(backend).expect("create test terminal");
        let mut view = bootstrap();
        view.models = vec![model("openai-codex/fixture-model", "Fixture Model", false)];
        view.active_session
            .as_mut()
            .expect("active session")
            .selected_model_id = Some(ModelId::from("openai-codex/fixture-model"));
        let mut state = TuiState::from_bootstrap(&view, 100);

        terminal
            .draw(|frame| super::draw(frame, &mut state))
            .expect("render Nakode view");

        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(ratatui::buffer::Cell::symbol)
            .collect::<String>();
        assert!(rendered.contains("NAKODE"));
        assert!(rendered.contains("Nako"));
        assert!(!rendered.contains("Prompt"));
        assert!(!rendered.contains("Ready."));
        assert!(!rendered.contains("Transcript"));
        assert!(rendered.contains("Model: Fixture Model"));
        assert!(rendered.contains("Directory: /tmp/project"));
        assert!(!rendered.contains("queue 0"));
        assert!(!rendered.contains("F1 help"));

        let buffer = terminal.backend().buffer();
        assert_eq!(buffer[(0, 0)].bg, super::ACCENT);
        assert_eq!(buffer[(0, 0)].fg, super::BACKGROUND);
        assert_eq!(buffer[(0, 1)].symbol(), "╭");
        assert_eq!(buffer[(0, 1)].fg, super::BORDER);
    }

    #[test]
    fn composer_identifies_shell_mode_for_a_leading_bang() {
        let backend = TestBackend::new(80, 28);
        let mut terminal = Terminal::new(backend).expect("create test terminal");
        let mut state = state();
        state.client.editor.set_text("!printf hello");

        terminal
            .draw(|frame| super::draw(frame, &mut state))
            .expect("render shell composer");

        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(ratatui::buffer::Cell::symbol)
            .collect::<String>();
        assert!(rendered.contains("Shell"));
        assert!(rendered.contains("!printf hello"));
    }

    #[test]
    fn addons_settings_render_terminal_image_choice() {
        let backend = TestBackend::new(100, 28);
        let mut terminal = Terminal::new(backend).expect("create test terminal");
        let mut state = state();
        state.open_settings();
        state.client.settings.as_mut().expect("settings").view = SettingsView::Addons;

        terminal
            .draw(|frame| super::draw(frame, &mut state))
            .expect("render add-ons settings");

        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(ratatui::buffer::Cell::symbol)
            .collect::<String>();
        assert!(rendered.contains("Web browsing"));
        assert!(rendered.contains("Vision"));
        assert!(rendered.contains("Terminal images"));
        assert!(rendered.contains("Automatic"));
    }

    #[test]
    fn successful_reload_replaces_prompt_metadata_with_a_visible_confirmation() {
        let backend = TestBackend::new(80, 1);
        let mut terminal = Terminal::new(backend).expect("create test terminal");
        let mut state = state();
        state.set_status("Reloaded 2 skills and 3 agents.");

        terminal
            .draw(|frame| super::render_prompt_metadata(frame, frame.area(), &state))
            .expect("render reload confirmation");

        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(ratatui::buffer::Cell::symbol)
            .collect::<String>();
        assert!(rendered.contains("✓ Reloaded 2 skills and 3 agents."));
        assert_eq!(terminal.backend().buffer()[(1, 0)].fg, super::SUCCESS);
    }

    #[test]
    fn composer_title_animates_while_nako_is_processing() {
        let backend = TestBackend::new(40, 5);
        let mut terminal = Terminal::new(backend).expect("create test terminal");
        let mut state = state();
        state.activity = SessionActivity::RunningTurn;

        terminal
            .draw(|frame| {
                super::render_composer(frame, frame.area(), &state);
            })
            .expect("render busy composer");

        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(ratatui::buffer::Cell::symbol)
            .collect::<String>();
        assert!(rendered.contains("Nako"));
        assert!(
            super::SPINNER_FRAMES
                .iter()
                .any(|frame| rendered.contains(frame))
        );
    }

    #[test]
    fn composer_title_shows_estimated_context_usage() {
        let backend = TestBackend::new(50, 5);
        let mut terminal = Terminal::new(backend).expect("create test terminal");
        let mut state = state();
        state.context_usage = Some(ContextUsageView {
            estimated_tokens: 12_345,
            context_window: Some(258_400),
            compacting: false,
        });

        terminal
            .draw(|frame| {
                super::render_composer(frame, frame.area(), &state);
            })
            .expect("render composer context usage");

        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(ratatui::buffer::Cell::symbol)
            .collect::<String>();
        assert!(rendered.contains("Nako · CTX ~12.3k / 258.4k"));
    }

    #[test]
    fn context_usage_color_warns_near_the_window_limit() {
        assert_eq!(
            super::context_usage_color(74_999, Some(100_000)),
            super::MUTED
        );
        assert_eq!(
            super::context_usage_color(75_000, Some(100_000)),
            super::WARNING
        );
        assert_eq!(
            super::context_usage_color(90_000, Some(100_000)),
            super::DANGER
        );
    }

    #[test]
    fn header_reports_when_no_model_is_selected() {
        let backend = TestBackend::new(40, 1);
        let mut terminal = Terminal::new(backend).expect("create test terminal");
        let state = state();

        terminal
            .draw(|frame| super::render_header(frame, frame.area(), &state))
            .expect("render Nakode header");

        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(ratatui::buffer::Cell::symbol)
            .collect::<String>();
        assert!(rendered.contains("MODEL No model selected"));
    }

    #[test]
    fn header_normalizes_model_name_and_marks_fast_mode() {
        let backend = TestBackend::new(50, 1);
        let mut terminal = Terminal::new(backend).expect("create test terminal");
        let mut view = bootstrap();
        view.models = vec![model("openai-codex/gpt-5.6-sol", "GPT 5.6 Sol", true)];
        view.active_session
            .as_mut()
            .expect("active session")
            .selected_model_id = Some(ModelId::from("openai-codex/gpt-5.6-sol"));
        let state = TuiState::from_bootstrap(&view, 100);

        terminal
            .draw(|frame| super::render_header(frame, frame.area(), &state))
            .expect("render Nakode header");

        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(ratatui::buffer::Cell::symbol)
            .collect::<String>();
        assert!(rendered.contains("MODEL GPT 5.6 Sol ⚡"));
    }

    #[test]
    fn switch_options_popup_exposes_cursor_fast_mode_for_this_session() {
        let backend = TestBackend::new(100, 28);
        let mut terminal = Terminal::new(backend).expect("create test terminal");
        let mut state = state();
        state.client.model_picker = Some(ModelPicker {
            filter: String::new(),
            selected: 0,
            scope: ModelSelectionScope::Session,
            stage: ModelPickerStage::Options,
            option_selected: 0,
            options: ModelOptions {
                reasoning_effort: None,
                fast_mode: false,
            },
            options_fast_only: true,
        });

        terminal
            .draw(|frame| super::draw(frame, &mut state))
            .expect("render switch options");
        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(ratatui::buffer::Cell::symbol)
            .collect::<String>();
        assert!(rendered.contains("Model Options"));
        assert!(rendered.contains("Configure this session's Cursor model"));
        assert!(rendered.contains("Fast mode"));
    }

    #[test]
    fn markdown_spans_map_to_terminal_colors_and_modifiers() {
        let mut markdown_style = MarkdownStyle {
            tone: Some(MarkdownTone::Rgb(12, 34, 56)),
            code: true,
            ..MarkdownStyle::default()
        };
        for modifier in [
            MarkdownModifier::Bold,
            MarkdownModifier::Italic,
            MarkdownModifier::Underlined,
            MarkdownModifier::CrossedOut,
        ] {
            markdown_style.modifiers.insert(modifier);
        }
        let line = super::transcript_line(ProjectedLine {
            text: "formatted".to_owned(),
            spans: vec![MarkdownSpan {
                text: "formatted".to_owned(),
                style: markdown_style,
            }],
            tone: LineTone::Body,
            bold: false,
            source_key: None,
        });

        let span = &line.spans[0];
        assert_eq!(span.style.fg, Some(ratatui::style::Color::Rgb(12, 34, 56)));
        assert_eq!(span.style.bg, Some(super::SURFACE_RAISED));
        assert!(
            span.style
                .add_modifier
                .contains(ratatui::style::Modifier::BOLD)
        );
        assert!(
            span.style
                .add_modifier
                .contains(ratatui::style::Modifier::ITALIC)
        );
        assert!(
            span.style
                .add_modifier
                .contains(ratatui::style::Modifier::UNDERLINED)
        );
        assert!(
            span.style
                .add_modifier
                .contains(ratatui::style::Modifier::CROSSED_OUT)
        );
    }

    #[test]
    fn active_todos_render_as_a_compact_persistent_panel() {
        let backend = TestBackend::new(100, 28);
        let mut terminal = Terminal::new(backend).expect("create test terminal");
        let mut state = state();
        state.todo_phases = vec![TodoPhaseView {
            name: "Implementation".to_owned(),
            tasks: vec![
                TodoItemView {
                    content: "Project todo events".to_owned(),
                    status: TodoStatusView::Completed,
                },
                TodoItemView {
                    content: "Render the active plan".to_owned(),
                    status: TodoStatusView::InProgress,
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
        let mut state = state();
        state.todo_phases = vec![TodoPhaseView {
            name: "Finished work".to_owned(),
            tasks: vec![
                TodoItemView {
                    content: "Completed task".to_owned(),
                    status: TodoStatusView::Completed,
                },
                TodoItemView {
                    content: "Pending task".to_owned(),
                    status: TodoStatusView::Pending,
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
        let mut state = state_with_run(
            "Map the authentication flow and identify every relevant boundary",
            RunStatus::Starting,
        );

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
        let mut state = state_with_run("Inspect persistence boundaries", RunStatus::Completed);

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
        let mut state = state_with_run("Map authentication", RunStatus::Starting);
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
        assert!(rendered.contains("Parent"));
        assert!(rendered.contains("Delegated task"));
    }

    #[test]
    fn provider_menu_shows_state_and_live_capability_details() {
        let backend = TestBackend::new(100, 30);
        let mut terminal = Terminal::new(backend).expect("create test terminal");
        let mut view = bootstrap();
        let mut codex = provider("openai-codex", "Codex", true, true);
        codex.capabilities = ProviderCapabilities {
            supported: BTreeSet::from([ProviderCapability::Resume]),
        };
        view.providers = vec![codex, provider("devin-acp", "Devin", false, false)];
        let mut state = TuiState::from_bootstrap(&view, 100);
        state.open_provider_picker();

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
    fn cursor_api_key_input_is_masked_in_provider_details() {
        let backend = TestBackend::new(100, 34);
        let mut terminal = Terminal::new(backend).expect("create test terminal");
        let mut view = bootstrap();
        let mut cursor = provider("cursor-acp", "Cursor", false, false);
        cursor.authentication = Some(ProviderAuthenticationView::ApiKeyRequired {
            dashboard_url: "https://cursor.example.test/api-keys".to_owned(),
            credential_kind: "api_key".to_owned(),
        });
        view.providers = vec![cursor];
        let mut state = TuiState::from_bootstrap(&view, 100);
        state.open_provider_picker();
        state.open_provider_details();

        terminal
            .draw(|frame| super::draw(frame, &mut state))
            .expect("render unfocused Cursor API key input");
        let input_row = terminal
            .backend()
            .buffer()
            .content()
            .chunks(100)
            .position(|row| {
                row.iter()
                    .map(ratatui::buffer::Cell::symbol)
                    .collect::<String>()
                    .contains("Add API key")
            })
            .expect("API key input row");
        assert!(!state.provider_api_key_input_active());
        assert!(
            state.focus_provider_api_key_at(crate::selection::ScreenPoint::new(
                50,
                u16::try_from(input_row).expect("input row fits")
            ))
        );
        state.provider_api_key_insert_str("cursor-super-secret");

        terminal
            .draw(|frame| super::draw(frame, &mut state))
            .expect("render Cursor API key input");
        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(ratatui::buffer::Cell::symbol)
            .collect::<String>();
        assert!(rendered.contains("Get API key"));
        assert!(!rendered.contains("Paste or type"));
        assert!(!rendered.contains("Enter save"));
        assert!(!rendered.contains("Capabilities"));
        assert!(!rendered.contains("Unavailable until"));
        assert!(rendered.contains('•'));
        assert!(!rendered.contains("cursor-super-secret"));
    }

    #[test]
    fn provider_authentication_shows_full_url_and_click_target() {
        let backend = TestBackend::new(100, 30);
        let mut terminal = Terminal::new(backend).expect("create test terminal");
        let mut view = bootstrap();
        let mut codex = provider("openai-codex", "Codex", false, false);
        codex.authentication = Some(ProviderAuthenticationView::Challenge {
            verification_url: "https://app.example.test/auth/cli/continue".to_owned(),
            user_code: String::new(),
        });
        view.providers = vec![codex];
        let mut state = TuiState::from_bootstrap(&view, 100);
        state.open_provider_picker();
        state.open_provider_details();
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
        let mut state = state();
        state.client.show_help = true;

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
    fn agent_menu_starts_empty_and_shows_all_editable_configuration_fields() {
        let backend = TestBackend::new(100, 30);
        let mut terminal = Terminal::new(backend).expect("create test terminal");
        let mut view = bootstrap();
        let mut cursor = model("cursor-acp/composer-2.5", "Composer 2.5", false);
        cursor.configuration.fast_mode_configurable = true;
        view.models = vec![
            model("openai-codex/gpt-5.6-sol", "GPT 5.6 Sol", false),
            cursor,
        ];
        let mut state = TuiState::from_bootstrap(&view, 100);
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
        assert!(list.contains("No agent archetypes configured"));

        state.create_agent();
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
        assert!(editor.contains("Enter choose model"));
        assert!(editor.contains("autosaves"));

        state
            .client
            .agent_picker
            .as_mut()
            .and_then(|picker| picker.editor.as_mut())
            .expect("agent editor")
            .field = AgentEditorField::Model;
        state.open_agent_model_dropdown();
        terminal
            .draw(|frame| super::draw(frame, &mut state))
            .expect("render agent model dropdown");
        let dropdown = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(ratatui::buffer::Cell::symbol)
            .collect::<String>();
        assert!(dropdown.contains("Select Agent Model"));
        assert!(dropdown.contains("Inherit parent model"));
        assert!(dropdown.contains("GPT 5.6 Sol"));
        assert!(
            terminal.backend().cursor_visible(),
            "the dropdown should focus its search input even when a model row is selected"
        );
        let cursor = terminal.backend().cursor_position();
        assert_eq!(cursor.y, 7);
        assert_eq!(cursor.x, 25);

        let editor = state
            .client
            .agent_picker
            .as_mut()
            .and_then(|picker| picker.editor.as_mut())
            .expect("agent editor");
        editor.model = "cursor-acp/composer-2.5".to_owned();
        editor.model_dropdown = None;
        editor.pending_options = Some(crate::tui_state::AgentPendingOptions {
            reasoning_efforts: Vec::new(),
            fast_mode_configurable: true,
            options: ModelOptions {
                reasoning_effort: None,
                fast_mode: false,
            },
            selected: 0,
        });
        terminal
            .draw(|frame| super::draw(frame, &mut state))
            .expect("render agent model options");
        let options = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(ratatui::buffer::Cell::symbol)
            .collect::<String>();
        assert!(options.contains("Model Options"));
        assert!(options.contains("Configure how this agent runs its model"));
        assert!(options.contains("Fast mode"));
        // A fast-mode-only model draws NO effort row: the levels on offer are the model's own, and
        // this one reports none.
        assert!(!options.contains("Reasoning effort"));
    }

    /// An archetype's level is offered beside its model, from that model's own list, and an archetype
    /// with none set reads as "default" rather than as a level nobody chose — which is what every
    /// definition written before the field existed means.
    #[test]
    fn an_agent_model_that_takes_levels_offers_them_beside_fast_mode() {
        let backend = TestBackend::new(100, 30);
        let mut terminal = Terminal::new(backend).expect("create test terminal");
        let mut view = bootstrap();
        let mut codex = model("openai-codex/gpt-5.6-sol", "GPT 5.6 Sol", false);
        codex.configuration.reasoning_efforts = vec!["low".to_owned(), "high".to_owned()];
        codex.configuration.fast_mode_configurable = true;
        view.models = vec![codex];
        let mut state = TuiState::from_bootstrap(&view, 100);
        state.open_agent_picker();
        state.create_agent();
        let editor = state
            .client
            .agent_picker
            .as_mut()
            .and_then(|picker| picker.editor.as_mut())
            .expect("agent editor");
        editor.model = "openai-codex/gpt-5.6-sol".to_owned();
        editor.pending_options = Some(crate::tui_state::AgentPendingOptions {
            reasoning_efforts: vec!["low".to_owned(), "high".to_owned()],
            fast_mode_configurable: true,
            options: ModelOptions {
                reasoning_effort: None,
                fast_mode: false,
            },
            selected: 0,
        });

        terminal
            .draw(|frame| super::draw(frame, &mut state))
            .expect("render agent effort options");
        let levels = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(ratatui::buffer::Cell::symbol)
            .collect::<String>();
        assert!(levels.contains("Reasoning effort: default"));
        assert!(levels.contains("Fast mode"));

        // Set, it names itself — and the level list is the model's own, never a table of ours.
        state.adjust_agent_model_options(1);
        terminal
            .draw(|frame| super::draw(frame, &mut state))
            .expect("render chosen agent effort");
        let chosen = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(ratatui::buffer::Cell::symbol)
            .collect::<String>();
        assert!(chosen.contains("Reasoning effort: low"));
    }

    /// A catalogue longer than the popup must scroll to whatever is selected rather than drawing
    /// only its first screenful: every row past the visible window is otherwise unreachable, and the
    /// footer that says how to reach it is pushed off the popup too.
    #[test]
    fn model_picker_scrolls_a_catalogue_taller_than_its_popup() {
        let backend = TestBackend::new(100, 40);
        let mut terminal = Terminal::new(backend).expect("create test terminal");
        let mut view = bootstrap();
        view.models = (0..40)
            .map(|index| {
                let mut entry = model(
                    &format!("openai-codex/model-{index:02}"),
                    &format!("Catalogue Model {index:02}"),
                    false,
                );
                entry.is_default = index == 0;
                entry
            })
            .collect();
        let mut state = TuiState::from_bootstrap(&view, 100);
        state.open_model_picker(ModelSelectionScope::Default);
        for _ in 0..37 {
            state.picker_move(1);
        }

        terminal
            .draw(|frame| super::draw(frame, &mut state))
            .expect("render model picker");
        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(ratatui::buffer::Cell::symbol)
            .collect::<String>();

        assert!(
            rendered.contains("Catalogue Model 37"),
            "the selected row must be inside the drawn window"
        );
        assert!(
            !rendered.contains("Catalogue Model 00"),
            "the window must have scrolled away from the top of the catalogue"
        );
        assert!(
            rendered.contains("Enter apply"),
            "the footer must survive a catalogue longer than the popup"
        );
        assert!(
            rendered.contains("38 of 40"),
            "the filter line must say where in the catalogue the window sits"
        );

        // Searching narrows the list to something that fits, and the window must go back to its top
        // rather than staying scrolled past the one remaining match.
        state.picker_insert('3');
        state.picker_insert('7');
        terminal
            .draw(|frame| super::draw(frame, &mut state))
            .expect("render filtered model picker");
        let filtered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(ratatui::buffer::Cell::symbol)
            .collect::<String>();
        assert!(filtered.contains("Catalogue Model 37"));
        assert!(
            !filtered.contains("of 40"),
            "a list that fits needs no position counter"
        );
    }

    /// The window is what the popup can draw, and the selection is the only thing that moves it — so
    /// every screenful must be reachable and the last one must be full rather than a part-empty tail.
    #[test]
    fn scroll_start_keeps_the_selection_inside_a_full_window() {
        // Short lists never scroll, however the selection moves inside them.
        assert_eq!(super::scroll_start(0, 3, 10), 0);
        assert_eq!(super::scroll_start(2, 3, 10), 0);
        // It holds still until the selection would leave the bottom of the window.
        assert_eq!(super::scroll_start(9, 40, 10), 0);
        assert_eq!(super::scroll_start(10, 40, 10), 1);
        // And stops with a full window rather than scrolling past the end of the list.
        assert_eq!(super::scroll_start(39, 40, 10), 30);
        // An empty list has nowhere to start, and a popup with no room for a row must not panic.
        assert_eq!(super::scroll_start(0, 0, 10), 0);
        assert_eq!(super::scroll_start(5, 40, 0), 5);
    }

    #[test]
    fn slash_input_renders_command_completions() {
        let backend = TestBackend::new(100, 28);
        let mut terminal = Terminal::new(backend).expect("create test terminal");
        let mut state = state();
        state.client.editor.set_text("/");

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
