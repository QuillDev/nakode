use std::{
    io::Write as _,
    process::{Command, Stdio},
};

use serde_json::Value;

#[test]
fn confined_worker_starts_without_environment_or_server_and_composes_host_calls() {
    let directory = tempfile::tempdir().expect("isolated worker directory");
    let mut child = Command::new(env!("CARGO_BIN_EXE_nakode"))
        .arg("codemode-worker")
        .env_clear()
        .current_dir(directory.path())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start worker without Nakode config or server environment");
    let input = concat!(
        "{\"source\":\"const rows = await tools.search({limit: 3}); return rows.filter(row => row.active).map(row => row.id);\",\"tools\":[\"search\"]}\n",
        "{\"value\":[{\"id\":1,\"active\":false},{\"id\":2,\"active\":true},{\"id\":3,\"active\":true}],\"failed\":false}\n"
    );
    child
        .stdin
        .take()
        .expect("worker stdin")
        .write_all(input.as_bytes())
        .expect("write worker protocol");

    let output = child.wait_with_output().expect("worker output");
    assert!(
        output.status.success(),
        "worker failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let messages = String::from_utf8(output.stdout)
        .expect("UTF-8 worker output")
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).expect("worker message"))
        .collect::<Vec<_>>();
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[0]["type"], "invoke");
    assert_eq!(messages[0]["name"], "search");
    assert_eq!(messages[0]["arguments"]["limit"], 3);
    assert_eq!(messages[1]["type"], "complete");
    assert_eq!(messages[1]["value"], serde_json::json!([2, 3]));
}
