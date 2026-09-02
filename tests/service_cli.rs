//! End-to-end behaviour of the service-facing CLI that unit tests cannot cover:
//! what the executable prints, on which stream, and what it starts.

#![cfg(unix)]

use std::{
    error::Error,
    path::Path,
    process::{Command, Stdio},
};

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

#[test]
fn remote_enrollment_reruns_preserve_credentials_until_explicit_rotation() -> TestResult {
    let temp = tempfile::tempdir()?;
    let control = temp.path().join("control");
    let first = nakode(temp.path(), &control)
        .args([
            "remote",
            "enable",
            "--bind",
            "127.0.0.1:17342",
            "--endpoint",
            "executor.example:17342",
        ])
        .output()?;
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    let first: serde_json::Value = serde_json::from_slice(&first.stdout)?;
    assert_eq!(first["endpoint"], "https://executor.example:17342");

    let second = nakode(temp.path(), &control)
        .args([
            "remote",
            "enable",
            "--bind",
            "127.0.0.1:17342",
            "--endpoint",
            "executor.example:17342",
        ])
        .output()?;
    assert!(
        second.status.success(),
        "{}",
        String::from_utf8_lossy(&second.stderr)
    );
    let second: serde_json::Value = serde_json::from_slice(&second.stdout)?;
    for stable in ["server_id", "api_key", "ca_certificate_pem"] {
        assert_eq!(first[stable], second[stable], "rerun changed {stable}");
    }

    let rotated = nakode(temp.path(), &control)
        .args([
            "remote",
            "rotate-credentials",
            "--endpoint",
            "executor.example:17342",
        ])
        .output()?;
    assert!(
        rotated.status.success(),
        "{}",
        String::from_utf8_lossy(&rotated.stderr)
    );
    let rotated: serde_json::Value = serde_json::from_slice(&rotated.stdout)?;
    assert_eq!(first["server_id"], rotated["server_id"]);
    assert_ne!(first["api_key"], rotated["api_key"]);
    assert_ne!(first["ca_certificate_pem"], rotated["ca_certificate_pem"]);

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let mode = std::fs::metadata(temp.path().join(".nakode/remote.json"))?
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
    }
    Ok(())
}

#[test]
fn concurrent_remote_rotations_leave_one_complete_printed_descriptor_active() -> TestResult {
    let temp = tempfile::tempdir()?;
    let control = temp.path().join("control");
    let enabled = nakode(temp.path(), &control)
        .args(["remote", "enable", "--endpoint", "executor.example:17342"])
        .output()?;
    assert!(enabled.status.success());

    for _ in 0..4 {
        let first = nakode(temp.path(), &control)
            .args([
                "remote",
                "rotate-credentials",
                "--endpoint",
                "executor.example:17342",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let second = nakode(temp.path(), &control)
            .args([
                "remote",
                "rotate-credentials",
                "--endpoint",
                "executor.example:17342",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let first = first.wait_with_output()?;
        let second = second.wait_with_output()?;
        assert!(first.status.success());
        assert!(second.status.success());
        let first: serde_json::Value = serde_json::from_slice(&first.stdout)?;
        let second: serde_json::Value = serde_json::from_slice(&second.stdout)?;
        let active = nakode(temp.path(), &control)
            .args([
                "remote",
                "descriptor",
                "--endpoint",
                "executor.example:17342",
            ])
            .output()?;
        assert!(active.status.success());
        let active: serde_json::Value = serde_json::from_slice(&active.stdout)?;
        assert!(active == first || active == second);
    }
    Ok(())
}

#[test]
fn remote_enable_rejects_wildcard_listeners_without_explicit_opt_in() -> TestResult {
    for bind in ["0.0.0.0:17342", "[0:0:0:0:0:0:0:0]:17342"] {
        let temp = tempfile::tempdir()?;
        let control = temp.path().join("control");
        let output = nakode(temp.path(), &control)
            .args([
                "remote",
                "enable",
                "--bind",
                bind,
                "--endpoint",
                "executor.example:17342",
            ])
            .output()?;
        assert!(!output.status.success());
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("--allow-public-listen"),
            "unexpected error for {bind}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            !temp.path().join(".nakode/remote.json").exists(),
            "a rejected listener must not enable remote access"
        );
    }
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
