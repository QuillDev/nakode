use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{Tool, ToolContext, ToolFuture, ToolResult};
use crate::{
    backend::{QuestionOption, QuestionRequest},
    runtime::ToolDefinition,
};

pub struct AskTool;

impl Tool for AskTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "ask",
            description: "Ask one or more related questions only after repository context and reasonable defaults cannot resolve a material choice. Use short option labels, put tradeoffs in descriptions, group related questions, and mark the recommended default.",
            parameters: json!({
                "type": "object",
                "properties": {
                    "questions": {
                        "type": "array",
                        "minItems": 1,
                        "items": {
                            "type": "object",
                            "properties": {
                                "id": {"type": "string", "description": "Stable question id"},
                                "question": {"type": "string"},
                                "header": {"type": "string", "description": "Optional short display heading"},
                                "options": {
                                    "type": "array",
                                    "minItems": 2,
                                    "maxItems": 5,
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "label": {"type": "string"},
                                            "description": {"type": "string"},
                                            "preview": {"type": "string"}
                                        },
                                        "required": ["label"],
                                        "additionalProperties": false
                                    }
                                },
                                "multi": {"type": "boolean"},
                                "recommended": {"type": "number", "minimum": 0}
                            },
                            "required": ["id", "question", "options"],
                            "additionalProperties": false
                        }
                    }
                },
                "required": ["questions"],
                "additionalProperties": false
            }),
        }
    }

    fn summarize(&self, arguments: &Value) -> String {
        arguments["questions"]
            .as_array()
            .and_then(|questions| questions.first())
            .and_then(|question| question["question"].as_str())
            .unwrap_or_default()
            .chars()
            .take(100)
            .collect()
    }

    fn execute<'a>(
        &'a self,
        context: ToolContext<'a>,
        arguments: Value,
        cancellation: &'a CancellationToken,
    ) -> ToolFuture<'a> {
        Box::pin(async move {
            let questions = match parse_questions(&arguments) {
                Ok(questions) => questions,
                Err(error) => return ToolResult::failure(error),
            };
            let mut results = Vec::new();
            for question in questions {
                let logical_id = question.logical_id.clone();
                let question_text = question.request.question.clone();
                match context
                    .questions
                    .ask(question.request, context.backend_events, cancellation)
                    .await
                {
                    Ok(answer) => {
                        let selected_options = serde_json::from_str::<Vec<String>>(&answer)
                            .unwrap_or_else(|_| vec![answer]);
                        results.push(json!({
                            "id": logical_id,
                            "question": question_text,
                            "selectedOptions": selected_options
                        }));
                    }
                    Err(error) => return ToolResult::failure(error),
                }
            }
            ToolResult::success(
                serde_json::to_string_pretty(&results).unwrap_or_else(|_| "[]".to_owned()),
            )
        })
    }
}

struct ParsedQuestion {
    logical_id: String,
    request: QuestionRequest,
}

fn parse_questions(arguments: &Value) -> Result<Vec<ParsedQuestion>, String> {
    arguments
        .get("questions")
        .and_then(Value::as_array)
        .filter(|questions| !questions.is_empty())
        .ok_or_else(|| "ask requires a non-empty questions array".to_owned())?
        .iter()
        .map(parse_question)
        .collect()
}

fn parse_question(value: &Value) -> Result<ParsedQuestion, String> {
    let logical_id = non_empty_string(value, "id")?.to_owned();
    let question = non_empty_string(value, "question")?.to_owned();
    let title = value
        .get("header")
        .and_then(Value::as_str)
        .filter(|header| !header.trim().is_empty())
        .unwrap_or("Question")
        .to_owned();
    let options = value
        .get("options")
        .and_then(Value::as_array)
        .filter(|options| (2..=5).contains(&options.len()))
        .ok_or_else(|| "each ask question requires between 2 and 5 options".to_owned())?
        .iter()
        .map(|option| {
            Ok(QuestionOption {
                label: non_empty_string(option, "label")?.to_owned(),
                description: option
                    .get("description")
                    .and_then(Value::as_str)
                    .filter(|description| !description.trim().is_empty())
                    .map(str::to_owned),
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    let recommended = value
        .get("recommended")
        .and_then(Value::as_u64)
        .map(usize::try_from)
        .transpose()
        .map_err(|error| error.to_string())?;
    if recommended.is_some_and(|index| index >= options.len()) {
        return Err(format!(
            "recommended option is out of range for question {logical_id}"
        ));
    }
    Ok(ParsedQuestion {
        logical_id,
        request: QuestionRequest {
            id: Uuid::now_v7().to_string(),
            title,
            question,
            options,
            multi: value.get("multi").and_then(Value::as_bool).unwrap_or(false),
            recommended,
        },
    })
}

fn non_empty_string<'a>(value: &'a Value, field: &str) -> Result<&'a str, String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|text| !text.trim().is_empty())
        .ok_or_else(|| format!("ask question requires non-empty string field {field}"))
}
