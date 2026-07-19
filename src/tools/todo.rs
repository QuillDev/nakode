use std::collections::HashSet;

use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use super::{Tool, ToolContext, ToolFuture, ToolResult, required_string};
use crate::{
    backend::{BackendEvent, TodoItem, TodoPhase, TodoStatus},
    runtime::ToolDefinition,
};

pub struct TodoTool;

impl Tool for TodoTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "todo",
            description: "Maintain a phased session task list. Tasks are referenced by exact content, never generated ids. Use init for the full plan, start/done/drop with task, append with phase and items, rm with task or phase, and view to recover exact wording.",
            parameters: json!({
                "type": "object",
                "properties": {
                    "op": {"type": "string", "enum": ["init", "start", "done", "rm", "drop", "append", "view"]},
                    "list": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "phase": {"type": "string"},
                                "items": {"type": "array", "minItems": 1, "items": {"type": "string"}}
                            },
                            "required": ["phase", "items"],
                            "additionalProperties": false
                        }
                    },
                    "task": {"type": "string", "description": "Exact task content"},
                    "phase": {"type": "string", "description": "Exact phase name"},
                    "items": {"type": "array", "items": {"type": "string"}}
                },
                "required": ["op"],
                "additionalProperties": false
            }),
        }
    }

    fn summarize(&self, arguments: &Value) -> String {
        arguments
            .get("op")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned()
    }

    fn execute<'a>(
        &'a self,
        context: ToolContext<'a>,
        arguments: Value,
        cancellation: &'a CancellationToken,
    ) -> ToolFuture<'a> {
        Box::pin(async move {
            if cancellation.is_cancelled() {
                return ToolResult::failure("todo operation interrupted");
            }
            match apply_operation(&mut context.session.todos, &arguments) {
                Ok(output) => {
                    let _ = context
                        .backend_events
                        .send(BackendEvent::TodoUpdated {
                            phases: context.session.todos.clone(),
                        })
                        .await;
                    ToolResult::success(output)
                }
                Err(error) => ToolResult::failure(error),
            }
        })
    }
}

fn apply_operation(phases: &mut Vec<TodoPhase>, arguments: &Value) -> Result<String, String> {
    match required_string(arguments, "op")? {
        "view" => {}
        "init" => initialize(phases, arguments)?,
        "append" => append(phases, arguments)?,
        "start" => set_task_status(
            phases,
            required_string(arguments, "task")?,
            TodoStatus::InProgress,
        )?,
        "done" => set_target_status(phases, arguments, TodoStatus::Completed)?,
        "drop" => set_target_status(phases, arguments, TodoStatus::Abandoned)?,
        "rm" => remove_target(phases, arguments)?,
        operation => return Err(format!("unsupported todo operation {operation}")),
    }
    if !matches!(required_string(arguments, "op")?, "view" | "start") {
        promote_next_task(phases);
    }
    Ok(render_phases(phases))
}

fn initialize(phases: &mut Vec<TodoPhase>, arguments: &Value) -> Result<(), String> {
    let replacement = if let Some(list) = arguments.get("list").and_then(Value::as_array) {
        if list.is_empty() {
            return Err("todo init list must not be empty".to_owned());
        }
        list.iter()
            .map(|entry| phase_from_value(entry, "phase"))
            .collect::<Result<Vec<_>, _>>()?
    } else {
        vec![TodoPhase {
            name: "Todos".to_owned(),
            tasks: tasks_from_value(arguments, "items")?,
        }]
    };
    validate_unique(&replacement)?;
    *phases = replacement;
    Ok(())
}

fn append(phases: &mut Vec<TodoPhase>, arguments: &Value) -> Result<(), String> {
    let phase_name = required_string(arguments, "phase")?;
    let additions = tasks_from_value(arguments, "items")?;
    if let Some(phase) = phases.iter_mut().find(|phase| phase.name == phase_name) {
        phase.tasks.extend(additions);
    } else {
        phases.push(TodoPhase {
            name: phase_name.to_owned(),
            tasks: additions,
        });
    }
    validate_unique(phases)
}

fn set_target_status(
    phases: &mut [TodoPhase],
    arguments: &Value,
    status: TodoStatus,
) -> Result<(), String> {
    if let Some(task) = arguments.get("task").and_then(Value::as_str) {
        return set_task_status(phases, task, status);
    }
    let phase_name = required_string(arguments, "phase")?;
    let phase = phases
        .iter_mut()
        .find(|phase| phase.name == phase_name)
        .ok_or_else(|| format!("todo phase not found: {phase_name}"))?;
    for task in &mut phase.tasks {
        task.status = status;
    }
    Ok(())
}

fn set_task_status(
    phases: &mut [TodoPhase],
    content: &str,
    status: TodoStatus,
) -> Result<(), String> {
    let task = phases
        .iter_mut()
        .flat_map(|phase| &mut phase.tasks)
        .find(|task| task.content == content)
        .ok_or_else(|| format!("todo task not found: {content}"))?;
    task.status = status;
    Ok(())
}

fn remove_target(phases: &mut Vec<TodoPhase>, arguments: &Value) -> Result<(), String> {
    if let Some(content) = arguments.get("task").and_then(Value::as_str) {
        for phase in phases.iter_mut() {
            if let Some(index) = phase.tasks.iter().position(|task| task.content == content) {
                phase.tasks.remove(index);
                phases.retain(|phase| !phase.tasks.is_empty());
                return Ok(());
            }
        }
        return Err(format!("todo task not found: {content}"));
    }
    if let Some(name) = arguments.get("phase").and_then(Value::as_str) {
        let before = phases.len();
        phases.retain(|phase| phase.name != name);
        return (phases.len() < before)
            .then_some(())
            .ok_or_else(|| format!("todo phase not found: {name}"));
    }
    phases.clear();
    Ok(())
}

fn phase_from_value(value: &Value, field: &str) -> Result<TodoPhase, String> {
    Ok(TodoPhase {
        name: required_string(value, field)?.trim().to_owned(),
        tasks: tasks_from_value(value, "items")?,
    })
}

fn tasks_from_value(value: &Value, field: &str) -> Result<Vec<TodoItem>, String> {
    value
        .get(field)
        .and_then(Value::as_array)
        .filter(|items| !items.is_empty())
        .ok_or_else(|| format!("todo requires a non-empty {field} array"))?
        .iter()
        .map(|item| {
            let content = item
                .as_str()
                .map(str::trim)
                .filter(|content| !content.is_empty())
                .ok_or_else(|| "todo items must be non-empty strings".to_owned())?;
            Ok(TodoItem {
                content: content.to_owned(),
                status: TodoStatus::Pending,
            })
        })
        .collect()
}

fn validate_unique(phases: &[TodoPhase]) -> Result<(), String> {
    let mut phase_names = HashSet::new();
    let mut task_contents = HashSet::new();
    for phase in phases {
        if !phase_names.insert(&phase.name) {
            return Err(format!("duplicate todo phase: {}", phase.name));
        }
        for task in &phase.tasks {
            if !task_contents.insert(&task.content) {
                return Err(format!("duplicate todo task: {}", task.content));
            }
        }
    }
    Ok(())
}

fn promote_next_task(phases: &mut [TodoPhase]) {
    if phases
        .iter()
        .flat_map(|phase| &phase.tasks)
        .any(|task| task.status == TodoStatus::InProgress)
    {
        return;
    }
    if let Some(task) = phases
        .iter_mut()
        .flat_map(|phase| &mut phase.tasks)
        .find(|task| task.status == TodoStatus::Pending)
    {
        task.status = TodoStatus::InProgress;
    }
}

fn render_phases(phases: &[TodoPhase]) -> String {
    if phases.is_empty() {
        return "todo list is empty".to_owned();
    }
    let mut lines = Vec::new();
    for (index, phase) in phases.iter().enumerate() {
        lines.push(format!("{}. {}", index + 1, phase.name));
        lines.extend(phase.tasks.iter().map(|task| {
            let marker = match task.status {
                TodoStatus::Pending => "[ ]",
                TodoStatus::InProgress => "[~]",
                TodoStatus::Completed => "[x]",
                TodoStatus::Abandoned => "[-]",
            };
            format!("  {marker} {}", task.content)
        }));
    }
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::apply_operation;
    use crate::backend::TodoStatus;

    #[test]
    fn phased_todos_are_addressed_by_content_and_auto_advance() {
        let mut phases = Vec::new();
        apply_operation(
            &mut phases,
            &json!({"op": "init", "list": [
                {"phase": "Build", "items": ["Implement reader", "Implement writer"]},
                {"phase": "Verify", "items": ["Run tests"]}
            ]}),
        )
        .expect("initialize todos");
        assert_eq!(phases[0].tasks[0].status, TodoStatus::InProgress);

        apply_operation(
            &mut phases,
            &json!({"op": "done", "task": "Implement reader"}),
        )
        .expect("complete todo");
        assert_eq!(phases[0].tasks[1].status, TodoStatus::InProgress);
        assert_eq!(phases[1].tasks[0].status, TodoStatus::Pending);
    }
}
