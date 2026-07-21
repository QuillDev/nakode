use std::{collections::HashMap, ffi::OsString, process::Stdio, time::Duration};

use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, ChildStdout, Command},
    sync::Mutex,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    Tool, ToolContext, ToolFuture, ToolResult, optional_u64, required_string, truncate_output,
};
use crate::runtime::ToolDefinition;

const DONE_PREFIX: &str = "__NAKODE_EVAL_DONE__";

#[derive(Default)]
pub struct EvalTool {
    kernels: Mutex<HashMap<(String, EvalLanguage), EvalKernel>>,
}

impl Tool for EvalTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "eval",
            description: "Run one cell in a persistent language kernel. State survives later eval calls for the same session and language; reset starts that kernel over.",
            parameters: json!({
                "type": "object",
                "properties": {
                    "language": {"type": "string", "enum": ["py", "js", "rb", "jl"], "description": "py, js, rb, or jl"},
                    "title": {"type": "string", "description": "Short transcript label"},
                    "timeout": {"type": "number", "minimum": 0, "maximum": 600, "description": "Seconds; 0 disables the deadline"},
                    "reset": {"type": "boolean", "description": "Restart only this language kernel before running"},
                    "code": {"type": "string"}
                },
                "required": ["language", "code"],
                "additionalProperties": false
            }),
        }
    }

    fn summarize(&self, arguments: &Value) -> String {
        arguments
            .get("title")
            .or_else(|| arguments.get("language"))
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
            match self
                .evaluate(
                    context.session.id.clone(),
                    context.workspace,
                    &arguments,
                    cancellation,
                )
                .await
            {
                Ok(result) => result,
                Err(error) => ToolResult::failure(error),
            }
        })
    }
}

impl EvalTool {
    async fn evaluate(
        &self,
        session_id: String,
        workspace: &std::path::Path,
        arguments: &Value,
        cancellation: &CancellationToken,
    ) -> Result<ToolResult, String> {
        let language = EvalLanguage::parse(required_string(arguments, "language")?)?;
        let code = arguments
            .get("code")
            .and_then(Value::as_str)
            .ok_or_else(|| "eval requires string argument code".to_owned())?;
        let seconds = optional_u64(arguments, "timeout", 60)?.min(600);
        let timeout = (seconds > 0).then(|| Duration::from_secs(seconds));
        let key = (session_id, language);
        let reset = arguments
            .get("reset")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let mut kernel = {
            let mut kernels = self.kernels.lock().await;
            let previous = kernels.remove(&key);
            if reset && let Some(mut previous) = previous {
                previous.stop().await;
                None
            } else {
                previous
            }
        };
        if kernel.is_none() {
            kernel = Some(EvalKernel::spawn(language, workspace)?);
        }
        let mut kernel = kernel.expect("kernel initialized above");
        let result = kernel.execute(code, timeout, cancellation).await;
        if result.is_ok() {
            self.kernels.lock().await.insert(key, kernel);
        } else {
            kernel.stop().await;
        }
        result
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum EvalLanguage {
    Python,
    JavaScript,
    Ruby,
    Julia,
}

impl EvalLanguage {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "py" => Ok(Self::Python),
            "js" => Ok(Self::JavaScript),
            "rb" => Ok(Self::Ruby),
            "jl" => Ok(Self::Julia),
            _ => Err(format!(
                "unsupported eval language {value}; use py, js, rb, or jl"
            )),
        }
    }

    fn command(self) -> (&'static str, Vec<OsString>) {
        match self {
            Self::Python => (
                "python3",
                vec!["-u".into(), "-c".into(), PYTHON_KERNEL.into()],
            ),
            Self::JavaScript => ("node", vec!["-e".into(), JAVASCRIPT_KERNEL.into()]),
            Self::Ruby => ("ruby", vec!["-e".into(), RUBY_KERNEL.into()]),
            Self::Julia => ("julia", vec!["-e".into(), JULIA_KERNEL.into()]),
        }
    }
}

struct EvalKernel {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
}

impl EvalKernel {
    fn spawn(language: EvalLanguage, workspace: &std::path::Path) -> Result<Self, String> {
        let (program, arguments) = language.command();
        let mut child = Command::new(program)
            .args(arguments)
            .current_dir(workspace)
            .kill_on_drop(true)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|error| format!("{program} eval runtime is unavailable: {error}"))?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| "eval kernel stdin was not captured".to_owned())?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| "eval kernel stdout was not captured".to_owned())?;
        Ok(Self {
            child,
            stdin,
            stdout: BufReader::new(stdout),
        })
    }

    async fn execute(
        &mut self,
        code: &str,
        timeout: Option<Duration>,
        cancellation: &CancellationToken,
    ) -> Result<ToolResult, String> {
        let cell_id = Uuid::now_v7().simple().to_string();
        let request = format!("{cell_id}\t{}\n", STANDARD.encode(code));
        self.stdin
            .write_all(request.as_bytes())
            .await
            .map_err(|error| format!("eval kernel input failed: {error}"))?;
        self.stdin
            .flush()
            .await
            .map_err(|error| format!("eval kernel flush failed: {error}"))?;
        let success_marker = format!("{DONE_PREFIX}{cell_id}\tok");
        let error_marker = format!("{DONE_PREFIX}{cell_id}\terror");
        let deadline = timeout.map(|duration| tokio::time::Instant::now() + duration);
        let mut output = Vec::new();
        loop {
            let line = tokio::select! {
                line = read_bounded_line(&mut self.stdout, super::MAX_TOOL_OUTPUT_BYTES) => line.map_err(|error| format!("eval kernel output failed: {error}"))?,
                () = cancellation.cancelled() => return Err("evaluation interrupted".to_owned()),
                () = wait_for_deadline(deadline) => return Err("evaluation timed out".to_owned()),
            };
            let Some(line) = line else {
                let status = self.child.wait().await.map_err(|error| error.to_string())?;
                return Err(format!("eval kernel exited with {status}"));
            };
            if line == success_marker.as_bytes() {
                return Ok(ToolResult::success(truncate_output(output)));
            }
            if line == error_marker.as_bytes() {
                let output = truncate_output(output);
                return Ok(ToolResult::failure(if output.is_empty() {
                    "evaluation failed".to_owned()
                } else {
                    output
                }));
            }
            let remaining = super::MAX_TOOL_OUTPUT_BYTES
                .saturating_add(1)
                .saturating_sub(output.len());
            output.extend_from_slice(&line[..line.len().min(remaining)]);
            if remaining > line.len() {
                output.push(b'\n');
            }
        }
    }

    async fn stop(&mut self) {
        let _ = self.child.start_kill();
        let _ = self.child.wait().await;
    }
}

async fn read_bounded_line(
    reader: &mut (impl AsyncBufRead + Unpin),
    limit: usize,
) -> std::io::Result<Option<Vec<u8>>> {
    let mut retained = Vec::with_capacity(limit.min(8 * 1024));
    let mut read_any = false;
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            return Ok(read_any.then_some(retained));
        }
        read_any = true;
        let newline = available.iter().position(|byte| *byte == b'\n');
        let consumed = newline.map_or(available.len(), |index| index + 1);
        let content = newline.map_or(available, |index| &available[..index]);
        let remaining = limit.saturating_add(1).saturating_sub(retained.len());
        retained.extend_from_slice(&content[..content.len().min(remaining)]);
        reader.consume(consumed);
        if newline.is_some() {
            if retained.last() == Some(&b'\r') {
                retained.pop();
            }
            return Ok(Some(retained));
        }
    }
}

async fn wait_for_deadline(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

const PYTHON_KERNEL: &str = r#"
import base64, sys, traceback
sys.stderr = sys.stdout
scope = {"__name__": "__main__"}
for request in sys.stdin:
    cell_id, encoded = request.rstrip("\n").split("\t", 1)
    status = "ok"
    try:
        exec(compile(base64.b64decode(encoded), "<eval>", "exec"), scope, scope)
    except BaseException:
        status = "error"
        traceback.print_exc()
    print("__NAKODE_EVAL_DONE__" + cell_id + "\t" + status, flush=True)
"#;

const JAVASCRIPT_KERNEL: &str = r#"
const readline = require("node:readline");
const vm = require("node:vm");
const context = vm.createContext({ ...globalThis, console, Buffer, process, require, fetch });
const input = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
(async () => {
  for await (const request of input) {
    const split = request.indexOf("\t");
    const id = request.slice(0, split);
    const code = Buffer.from(request.slice(split + 1), "base64").toString("utf8");
    let status = "ok";
    try {
      const value = vm.runInContext(code, context);
      if (value && typeof value.then === "function") await value;
    } catch (error) { status = "error"; console.error(error?.stack ?? String(error)); }
    console.log("__NAKODE_EVAL_DONE__" + id + "\t" + status);
  }
})();
"#;

const RUBY_KERNEL: &str = r#"
require "base64"
$stdout.sync = true
$stderr.reopen($stdout)
scope = binding
while request = STDIN.gets
  id, encoded = request.chomp.split("\t", 2)
  status = "ok"
  begin
    eval(Base64.decode64(encoded), scope, "<eval>")
  rescue Exception => error
    status = "error"
    warn error.full_message
  end
  puts "__NAKODE_EVAL_DONE__#{id}\t#{status}"
end
"#;

const JULIA_KERNEL: &str = r#"
using Base64
redirect_stderr(stdout)
while !eof(stdin)
    request = readline(stdin)
    id, encoded = split(request, '\t'; limit=2)
    status = "ok"
    try
        Base.include_string(Main, String(base64decode(encoded)), "<eval>")
    catch error
        status = "error"
        showerror(stdout, error, catch_backtrace()); println()
    end
    println("__NAKODE_EVAL_DONE__" * id * "\t" * status); flush(stdout)
end
"#;

#[cfg(test)]
mod tests {
    use super::{EvalLanguage, EvalTool};
    use serde_json::json;
    use tokio_util::sync::CancellationToken;

    #[test]
    fn evaluator_uses_the_omp_language_tokens() {
        assert_eq!(EvalLanguage::parse("py"), Ok(EvalLanguage::Python));
        assert_eq!(EvalLanguage::parse("js"), Ok(EvalLanguage::JavaScript));
        assert_eq!(EvalLanguage::parse("rb"), Ok(EvalLanguage::Ruby));
        assert_eq!(EvalLanguage::parse("jl"), Ok(EvalLanguage::Julia));
        assert!(EvalLanguage::parse("python").is_err());
    }

    #[tokio::test]
    async fn python_kernel_preserves_state_across_cells_and_supports_reset() {
        let tool = EvalTool::default();
        let workspace = tempfile::tempdir().expect("workspace");
        let cancellation = CancellationToken::new();
        let first = tool
            .evaluate(
                "session-1".to_owned(),
                workspace.path(),
                &json!({"language": "py", "code": "answer = 41"}),
                &cancellation,
            )
            .await;
        if first
            .as_ref()
            .is_err_and(|error| error.contains("runtime is unavailable"))
        {
            return;
        }
        assert!(!first.expect("first cell").failed);

        let second = tool
            .evaluate(
                "session-1".to_owned(),
                workspace.path(),
                &json!({"language": "py", "code": "print(answer + 1)"}),
                &cancellation,
            )
            .await
            .expect("second cell");
        assert!(!second.failed);
        assert_eq!(second.output.trim(), "42");

        let reset = tool
            .evaluate(
                "session-1".to_owned(),
                workspace.path(),
                &json!({"language": "py", "reset": true, "code": "print('answer' in globals())"}),
                &cancellation,
            )
            .await
            .expect("reset cell");
        assert_eq!(reset.output.trim(), "False");
    }
}
