use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use super::{Tool, ToolConcurrency, ToolContext, ToolFuture, ToolResult, required_string};
use crate::{
    runtime::ToolDefinition,
    skill::{SkillCatalog, skill_is_advertised},
};

pub struct ReadSkillTool;

impl Tool for ReadSkillTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "read_skill",
            description: "Load one installed Nakode skill by exact catalogue name as structured JSON containing skill_instructions, skill_content, and the auto-discovered components catalogue. Component bodies are loaded separately with read_skill_component, so agents never search package directories or receive unused reference material.",
            parameters: json!({
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "minLength": 1,
                        "description": "Exact installed skill name from the Nakode skill catalogue."
                    }
                },
                "required": ["name"],
                "additionalProperties": false
            }),
        }
    }

    fn summarize(&self, arguments: &Value) -> String {
        arguments.get("name").and_then(Value::as_str).map_or_else(
            || "read installed skill".to_owned(),
            |name| format!("read {name} skill"),
        )
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
                if !skill_is_advertised(&context.session.instructions, name) {
                    return Err(format!(
                        "skill {name:?} is disabled or was not available when this session started"
                    ));
                }
                let catalogue = SkillCatalog::load(context.workspace)
                    .map_err(|error| format!("could not load installed skills: {error}"))?;
                let skill = catalogue.find(name).ok_or_else(|| {
                    format!("skill {name:?} is not installed in the current Nakode workspace")
                })?;
                Ok::<_, String>((skill.instructions.clone(), skill.stable_id().to_owned()))
            })();
            result.map_or_else(ToolResult::failure, |(instructions, identity)| {
                ToolResult::success(instructions).with_invocation_identity(identity)
            })
        })
    }
}
