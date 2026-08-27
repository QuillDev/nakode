use std::{
    io::{BufRead, Write},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use rquickjs::{Context, Function, Promise, Runtime};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const MAX_SOURCE_BYTES: usize = 32 * 1024;
pub const MAX_TOOL_CALLS: usize = 64;
pub const MAX_FRAME_BYTES: usize = 1024 * 1024;
pub const MEMORY_LIMIT_BYTES: usize = 32 * 1024 * 1024;
pub const MAX_STACK_BYTES: usize = 1024 * 1024;
pub const EXECUTION_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug, Deserialize)]
pub struct WorkerRequest {
    pub source: String,
    pub tools: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct HostResponse {
    value: Value,
    #[serde(default)]
    failed: bool,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum WorkerMessage<'a> {
    Invoke { name: &'a str, arguments: Value },
    Complete { value: Value },
    Failed { message: &'a str },
}

/// Runs the confined Code Mode worker over standard input and output.
///
/// # Errors
///
/// Returns an error when worker setup or protocol I/O fails.
pub fn run() -> Result<(), String> {
    run_protocol(
        std::io::BufReader::new(std::io::stdin()),
        std::io::BufWriter::new(std::io::stdout()),
    )
}

/// Runs one worker request over the supplied framed protocol streams.
///
/// # Errors
///
/// Returns an error when the request, JavaScript execution, or protocol I/O fails.
pub fn run_protocol(
    mut input: impl BufRead + Send + 'static,
    mut output: impl Write + Send + 'static,
) -> Result<(), String> {
    let request_line =
        read_frame(&mut input)?.ok_or_else(|| "codemode worker request is missing".to_owned())?;
    let request: WorkerRequest = serde_json::from_str(&request_line)
        .map_err(|error| format!("invalid codemode worker request: {error}"))?;
    if request.source.len() > MAX_SOURCE_BYTES {
        return write_message(
            &mut output,
            &WorkerMessage::Failed {
                message: "codemode source exceeds the 32 KiB limit",
            },
        );
    }
    let input = Arc::new(Mutex::new(input));
    let output = Arc::new(Mutex::new(output));
    let result = execute(&request, &input, &output);
    let mut output = output
        .lock()
        .map_err(|_| "codemode worker output lock was poisoned".to_owned())?;
    match result {
        Ok(value) => write_message(&mut *output, &WorkerMessage::Complete { value }),
        Err(message) => write_message(&mut *output, &WorkerMessage::Failed { message: &message }),
    }
}

#[allow(clippy::too_many_lines)]
fn execute<R, W>(
    request: &WorkerRequest,
    input: &Arc<Mutex<R>>,
    output: &Arc<Mutex<W>>,
) -> Result<Value, String>
where
    R: BufRead + Send + 'static,
    W: Write + Send + 'static,
{
    let runtime =
        Runtime::new().map_err(|error| format!("could not create QuickJS runtime: {error}"))?;
    runtime.set_memory_limit(MEMORY_LIMIT_BYTES);
    runtime.set_max_stack_size(MAX_STACK_BYTES);
    let deadline = Instant::now() + EXECUTION_TIMEOUT;
    runtime.set_interrupt_handler(Some(Box::new(move || Instant::now() >= deadline)));
    let context = Context::full(&runtime)
        .map_err(|error| format!("could not create QuickJS context: {error}"))?;

    context.with(|ctx| {
        let host_input = Arc::clone(input);
        let host_output = Arc::clone(output);
        let call_count = Arc::new(AtomicUsize::new(0));
        let host_call_count = Arc::clone(&call_count);
        let host_call = Function::new(
            ctx.clone(),
            move |name: String, arguments_json: String| -> String {
                if host_call_count.fetch_add(1, Ordering::Relaxed) >= MAX_TOOL_CALLS {
                    return failed_envelope("codemode exceeded the 64-call limit");
                }
                let arguments = serde_json::from_str::<Value>(&arguments_json)
                    .unwrap_or_else(|_| Value::Object(serde_json::Map::new()));
                let request = WorkerMessage::Invoke {
                    name: &name,
                    arguments,
                };
                let sent = host_output
                    .lock()
                    .map_err(|_| "codemode worker output lock was poisoned".to_owned())
                    .and_then(|mut output| write_message(&mut *output, &request));
                if let Err(error) = sent {
                    return failed_envelope(&error);
                }
                let response = host_input
                    .lock()
                    .map_err(|_| "codemode worker input lock was poisoned".to_owned())
                    .and_then(|mut input| {
                        read_frame(&mut *input)?.ok_or_else(|| {
                            "codemode host closed before returning a tool result".to_owned()
                        })
                    })
                    .and_then(|line| {
                        serde_json::from_str::<HostResponse>(&line)
                            .map_err(|error| format!("invalid codemode host response: {error}"))
                    });
                match response {
                    Ok(response) => serde_json::to_string(&response).unwrap_or_else(|error| {
                        failed_envelope(&format!("could not encode host response: {error}"))
                    }),
                    Err(error) => failed_envelope(&error),
                }
            },
        )
        .map_err(|error| format!("could not create codemode host function: {error}"))?;
        ctx.globals()
            .set("__nakode_call", host_call)
            .map_err(|error| format!("could not install codemode host function: {error}"))?;

        let names = serde_json::to_string(&request.tools)
            .map_err(|error| format!("could not encode codemode catalogue: {error}"))?;
        let program = format!(
            r#"
"use strict";
const __toolNames = {names};
const tools = Object.freeze(Object.fromEntries(__toolNames.map((name) => [name, (args = {{}}) => {{
  const response = JSON.parse(__nakode_call(name, JSON.stringify(args)));
  if (response.failed) throw new Error(typeof response.value === "string" ? response.value : JSON.stringify(response.value));
  return response.value;
}}])));
globalThis.fetch = undefined;
globalThis.XMLHttpRequest = undefined;
globalThis.WebSocket = undefined;
globalThis.eval = undefined;
globalThis.Function = undefined;
globalThis.Date = undefined;
Math.random = () => {{ throw new Error("Math.random is unavailable in codemode"); }};
(async () => {{
{source}
}})().then(
  (value) => JSON.stringify({{ ok: true, value: value === undefined ? null : value }}),
  (error) => JSON.stringify({{ ok: false, error: String(error && error.stack ? error.stack : error) }})
)
"#,
            source = request.source,
        );
        let promise: Promise = ctx
            .eval(program)
            .map_err(|error| format_js_error(&ctx, &error))?;
        let encoded: String = promise
            .finish()
            .map_err(|error| format_js_error(&ctx, &error))?;
        let envelope: Value = serde_json::from_str(&encoded)
            .map_err(|error| format!("codemode returned invalid result JSON: {error}"))?;
        if envelope.get("ok").and_then(Value::as_bool) == Some(true) {
            Ok(envelope.get("value").cloned().unwrap_or(Value::Null))
        } else {
            Err(envelope
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("codemode execution failed")
                .to_owned())
        }
    })
}

fn format_js_error(ctx: &rquickjs::Ctx<'_>, error: &rquickjs::Error) -> String {
    if error.is_exception() {
        let caught = ctx.catch();
        if let Some(exception) = caught.as_exception() {
            return exception
                .stack()
                .unwrap_or_else(|| exception.message().unwrap_or_else(|| error.to_string()));
        }
    }
    error.to_string()
}

fn failed_envelope(message: &str) -> String {
    serde_json::to_string(&serde_json::json!({"value": message, "failed": true}))
        .unwrap_or_else(|_| r#"{"value":"codemode host failure","failed":true}"#.to_owned())
}

fn read_frame(input: &mut impl BufRead) -> Result<Option<String>, String> {
    let mut retained = Vec::new();
    let mut read_any = false;
    loop {
        let available = input
            .fill_buf()
            .map_err(|error| format!("codemode protocol read failed: {error}"))?;
        if available.is_empty() {
            if !read_any {
                return Ok(None);
            }
            return Err("codemode protocol frame is unterminated".to_owned());
        }
        read_any = true;
        let newline = available.iter().position(|byte| *byte == b'\n');
        let consumed = newline.map_or(available.len(), |index| index + 1);
        let content = newline.map_or(available, |index| &available[..index]);
        if retained.len().saturating_add(content.len()) > MAX_FRAME_BYTES {
            return Err(format!(
                "codemode protocol frame exceeds {MAX_FRAME_BYTES} bytes"
            ));
        }
        retained.extend_from_slice(content);
        input.consume(consumed);
        if newline.is_some() {
            if retained.last() == Some(&b'\r') {
                retained.pop();
            }
            return String::from_utf8(retained)
                .map(Some)
                .map_err(|error| format!("codemode protocol frame is not UTF-8: {error}"));
        }
    }
}

fn write_message(output: &mut impl Write, message: &WorkerMessage<'_>) -> Result<(), String> {
    serde_json::to_writer(&mut *output, message)
        .map_err(|error| format!("codemode protocol encode failed: {error}"))?;
    output
        .write_all(b"\n")
        .and_then(|()| output.flush())
        .map_err(|error| format!("codemode protocol write failed: {error}"))
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn confined_program_composes_host_tools_and_returns_json() {
        let request = serde_json::json!({
            "source": "const first = await tools.read({ path: 'a' }); const second = await tools.read({ path: 'b' }); return { joined: first + second };",
            "tools": ["read"]
        });
        let input = format!(
            "{}\n{}\n{}\n",
            request,
            serde_json::json!({"value": "A", "failed": false}),
            serde_json::json!({"value": "B", "failed": false})
        );
        let output = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&output);
        run_protocol(Cursor::new(input.into_bytes()), SharedWriter(output)).expect("worker run");
        let output = String::from_utf8(captured.lock().expect("output").clone()).expect("utf8");
        let messages = output
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).expect("message"))
            .collect::<Vec<_>>();
        assert_eq!(messages[0]["type"], "invoke");
        assert_eq!(messages[0]["arguments"]["path"], "a");
        assert_eq!(messages[1]["arguments"]["path"], "b");
        assert_eq!(messages[2]["value"]["joined"], "AB");
    }

    #[test]
    fn failed_host_call_is_catchable_in_program() {
        let request = serde_json::json!({
            "source": "try { await tools.read({ path: 'missing' }); } catch (error) { return String(error.message); }",
            "tools": ["read"]
        });
        let input = format!(
            "{}\n{}\n",
            request,
            serde_json::json!({"value": "not found", "failed": true})
        );
        let output = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&output);
        run_protocol(Cursor::new(input.into_bytes()), SharedWriter(output)).expect("worker run");
        let output = String::from_utf8(captured.lock().expect("output").clone()).expect("utf8");
        let complete: Value = serde_json::from_str(output.lines().last().expect("complete"))
            .expect("complete message");
        assert!(
            complete["value"]
                .as_str()
                .is_some_and(|value| value.contains("not found"))
        );
    }

    #[test]
    fn worker_has_no_ambient_host_authority() {
        let request = serde_json::json!({
            "source": "return { fetch: typeof fetch, process: typeof process, require: typeof require, deno: typeof Deno, bun: typeof Bun, timer: typeof setTimeout, eval: typeof eval, function: typeof Function };",
            "tools": []
        });
        let input = format!("{request}\n");
        let output = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&output);
        run_protocol(Cursor::new(input.into_bytes()), SharedWriter(output)).expect("worker run");
        let output = String::from_utf8(captured.lock().expect("output").clone()).expect("utf8");
        let complete: Value = serde_json::from_str(output.lines().last().expect("complete"))
            .expect("complete message");
        let authority = complete["value"].as_object().expect("authority result");
        assert!(authority.values().all(|value| value == "undefined"));
    }

    #[test]
    fn worker_enforces_the_total_host_call_limit() {
        let request = serde_json::json!({
            "source": "try { for (let index = 0; index < 65; index += 1) await tools.read({ index }); } catch (error) { return String(error.message); }",
            "tools": ["read"]
        });
        let mut input = format!("{request}\n");
        for _ in 0..MAX_TOOL_CALLS {
            input.push_str("{\"value\":null,\"failed\":false}\n");
        }
        let output = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&output);
        run_protocol(Cursor::new(input.into_bytes()), SharedWriter(output)).expect("worker run");
        let output = String::from_utf8(captured.lock().expect("output").clone()).expect("utf8");
        let messages = output
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).expect("message"))
            .collect::<Vec<_>>();
        assert_eq!(
            messages
                .iter()
                .filter(|message| message["type"] == "invoke")
                .count(),
            MAX_TOOL_CALLS
        );
        assert!(
            messages
                .last()
                .and_then(|message| message["value"].as_str())
                .is_some_and(|message| message.contains("64-call limit"))
        );
    }

    struct SharedWriter(Arc<Mutex<Vec<u8>>>);

    impl Write for SharedWriter {
        fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
            self.0.lock().expect("writer").write(buffer)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
}
