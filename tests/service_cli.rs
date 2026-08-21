//! End-to-end behaviour of the service-facing CLI that unit tests cannot cover:
//! what the executable prints, on which stream, and what it starts.

#![cfg(unix)]

use std::{error::Error, path::Path, process::Command};

type TestResult = Result<(), Box<dyn Error>>;

#[test]
fn bare_invocation_prints_help_and_starts_nothing() -> TestResult {
    let temp = tempfile::tempdir()?;
    let control = temp.path().join("control");
    let output = nakode(temp.path(), &control).output()?;

    assert!(output.status.success(), "`nakode` must exit successfully");
    let help = String::from_utf8(output.stdout)?;
    assert!(
        help.contains("--tui"),
        "help must document the client:\n{help}"
    );
    for command in ["run", "start", "stop", "restart", "status", "logs"] {
        assert!(
            help.contains(command),
            "help must document {command}:\n{help}"
        );
    }
    assert!(
        !control.exists() || sockets_in(&control)? == 0,
        "a bare invocation must not start a service"
    );
    Ok(())
}

#[test]
fn deprecated_endpoint_keeps_its_descriptor_on_standard_output() -> TestResult {
    let temp = tempfile::tempdir()?;
    let control = temp.path().join("control");
    let output = nakode(temp.path(), &control)
        .args(["service", "endpoint"])
        .output()?;
    let stop = nakode(temp.path(), &control).arg("stop").output();

    assert!(
        output.status.success(),
        "`nakode service endpoint` must keep working: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let errors = String::from_utf8(output.stderr)?;
    assert!(
        errors.contains("`nakode service endpoint` is deprecated; use `nakode endpoint`."),
        "the deprecation notice belongs on standard error:\n{errors}"
    );

    // Frontends read the last non-empty line of standard output as the
    // descriptor. Nothing else may reach that stream.
    let out = String::from_utf8(output.stdout)?;
    let descriptor = out
        .lines()
        .rev()
        .find(|line| !line.trim().is_empty())
        .ok_or("no descriptor on standard output")?;
    let descriptor: serde_json::Value = serde_json::from_str(descriptor)?;
    assert_eq!(descriptor["version"], 1);
    assert_eq!(descriptor["transport"], "grpc+unix");
    assert_eq!(
        descriptor["workspace"].as_str(),
        Some(
            temp.path()
                .join(".nakode")
                .canonicalize()?
                .to_str()
                .ok_or("non-UTF-8 installation workspace")?
        )
    );
    assert!(
        Path::new(
            descriptor["endpoint"]
                .as_str()
                .ok_or("descriptor without an endpoint")?
        )
        .is_absolute(),
        "the endpoint must be an absolute socket path"
    );

    assert!(stop?.status.success(), "the started service must stop");
    Ok(())
}

fn nakode(workspace: &Path, control: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_nakode"));
    command
        .env("NAKODE_CONTROL_DIR", control)
        .env("HOME", workspace)
        .current_dir(workspace);
    command
}

fn sockets_in(control: &Path) -> Result<usize, Box<dyn Error>> {
    let workspaces = control.join("w");
    if !workspaces.exists() {
        return Ok(0);
    }
    let mut sockets = 0;
    for entry in std::fs::read_dir(workspaces)? {
        for file in std::fs::read_dir(entry?.path())? {
            if file?.path().extension().is_some_and(|kind| kind == "sock") {
                sockets += 1;
            }
        }
    }
    Ok(sockets)
}
