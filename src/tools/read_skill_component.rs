use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use super::{Tool, ToolConcurrency, ToolContext, ToolFuture, ToolResult, required_string};
use crate::runtime::ToolDefinition;

pub struct ReadSkillComponentTool;

impl Tool for ReadSkillComponentTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "read_skill_component",
            description: "Load one exact Markdown component advertised by a prior read_skill result. Use the skill name and component_name from that result; this tool never accepts an arbitrary filesystem path.",
            parameters: json!({
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "minLength": 1,
                        "description": "Exact installed skill name used with read_skill."
                    },
                    "component_name": {
                        "type": "string",
                        "minLength": 1,
                        "description": "Exact component_name returned in read_skill.components."
                    }
                },
                "required": ["name", "component_name"],
                "additionalProperties": false
            }),
        }
    }

    fn summarize(&self, arguments: &Value) -> String {
        let skill = arguments.get("name").and_then(Value::as_str);
        let component = arguments.get("component_name").and_then(Value::as_str);
        match (skill, component) {
            (Some(skill), Some(component)) => format!("read {skill}/{component} component"),
            _ => "read skill component".to_owned(),
        }
    }

    fn concurrency(&self) -> ToolConcurrency {
        ToolConcurrency::ReadOnly
    }

    fn execute<'a>(
        &'a self,
        context: ToolContext<'a>,
        arguments: Value,
        _cancellation: &'a CancellationToken,
    ) -> ToolFuture<'a> {
        Box::pin(async move {
            let result = (|| {
                let name = required_string(&arguments, "name")?;
                let component_name = required_string(&arguments, "component_name")?;
                let skill = context.session.skill_catalogue.find(name).ok_or_else(|| {
                    format!("skill {name:?} is disabled or unavailable for the current profile")
                })?;
                let component = skill.component(component_name).ok_or_else(|| {
                    format!(
                        "component {component_name:?} was not advertised by skill {name:?}; call read_skill and use an exact component_name from components"
                    )
                })?;
                if component.owner_skill() != name
                    && context
                        .session
                        .skill_catalogue
                        .find(component.owner_skill())
                        .is_none()
                {
                    return Err(format!(
                        "component {component_name:?} belongs to disabled or unavailable skill {:?}",
                        component.owner_skill()
                    ));
                }
                Ok::<_, String>((
                    json!({
                        "name": name,
                        "component_name": component.component_name,
                        "file_path": component.file_path,
                        "component_content": component.contents,
                    })
                    .to_string(),
                    skill.stable_id().to_owned(),
                ))
            })();
            result.map_or_else(ToolResult::failure, |(payload, identity)| {
                ToolResult::success(payload).with_invocation_identity(identity)
            })
        })
    }
}
