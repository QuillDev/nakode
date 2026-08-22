//! Isolated whole-process proof for deferred update activation and helper recovery.
//!
//! Every path and process in this test belongs to one temporary root. It never discovers or
//! addresses the owner's installation endpoint.

#![cfg(all(unix, feature = "e2e-fixture-provider"))]

use std::{
    error::Error,
    ffi::CString,
    fs::{self, OpenOptions},
    io::Write,
    os::unix::{ffi::OsStrExt, fs::MetadataExt},
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
    time::{Duration, Instant},
};

use nakode_sdk::{ActivationClient, NakodeClient, SessionAttachment, v1 as api};
use serde_json::Value;

const WAIT_LIMIT: Duration = Duration::from_secs(10);
const WAIT_STEP: Duration = Duration::from_millis(50);
const COMMAND_LIMIT: Duration = Duration::from_secs(20);
// Starting the first isolated service includes cold process and provider initialization. Keep that
// distinct from the 20-second activation-endpoint contract exercised after deferred activation.
const COLD_START_LIMIT: Duration = Duration::from_secs(40);
// A manual activation includes cold replacement-service startup and persisted-session restoration.
// CI evidence shows that operation can legitimately outlive the isolated CLI command bound while
// continuing to publish its runtime and sockets, so keep a distinct bounded lifecycle allowance.
const ACTIVATION_LIMIT: Duration = Duration::from_secs(40);

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

struct IsolatedInstallation {
    root: tempfile::TempDir,
    home: PathBuf,
    nakode_home: PathBuf,
    control: PathBuf,
    workspace: PathBuf,
    old_binary: PathBuf,
    installed_binary: PathBuf,
    fixture: PathBuf,
    turn_gate: PathBuf,
}

impl IsolatedInstallation {
    fn new() -> TestResult<Self> {
        let root = tempfile::tempdir()?;
        let home = root.path().join("home");
        let nakode_home = root.path().join("nakode-home");
        let control = root.path().join("control");
        let workspace = root.path().join("workspace");
        let bin = root.path().join("bin");
        for directory in [&home, &nakode_home, &control, &workspace, &bin] {
            fs::create_dir_all(directory)?;
        }

        let installed_binary = bin.join("nakode-installed");
        fs::copy(env!("CARGO_BIN_EXE_nakode"), &installed_binary)?;
        let old_binary = bin.join("nakode-old");
        fs::copy(&installed_binary, &old_binary)?;
        OpenOptions::new()
            .append(true)
            .open(&old_binary)?
            .write_all(b"\nNAKODE-ISOLATED-OLD-BUILD\n")?;

        let fixture = root.path().join("fake_codex.py");
        fs::copy(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fake_codex.py"),
            &fixture,
        )?;
        let turn_gate = root.path().join("turn-gate.fifo");
        make_fifo(&turn_gate)?;

        Ok(Self {
            root,
            home,
            nakode_home,
            control,
            workspace,
            old_binary,
            installed_binary,
            fixture,
            turn_gate,
        })
    }

    fn command(&self, executable: &Path) -> Command {
        let mut command = Command::new(executable);
        command
            .env("HOME", &self.home)
            .env("TMPDIR", self.root.path())
            .env("NAKODE_HOME", &self.nakode_home)
            .env("NAKODE_CONTROL_DIR", &self.control)
            .env("NAKODE_E2E_CODEX_FIXTURE", &self.fixture)
            .env("NAKODE_E2E_GATE_ROOT", self.root.path())
            .env("NAKODE_E2E_TURN_GATE", &self.turn_gate)
            // Distinct fixture processes model distinct provider threads; `thread/resume` accepts
            // the persisted identity when build B reconnects through a fresh bridge process.
            .env("NAKODE_E2E_UNIQUE_CODEX_IDS", "1")
            .env_remove("NAKODE_MODEL")
            .env_remove("NAKODE_RESUME")
            .current_dir(&self.workspace);
        command
    }

    fn output(&self, executable: &Path, arguments: &[&str]) -> TestResult<Output> {
        self.output_with_limit(executable, arguments, COMMAND_LIMIT)
    }

    fn output_with_limit(
        &self,
        executable: &Path,
        arguments: &[&str],
        limit: Duration,
    ) -> TestResult<Output> {
        let mut command = self.command(executable);
        command
            .args(arguments)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        wait_for_child_output(command.spawn()?, &arguments.join(" "), limit)
    }

    fn descriptor(&self, executable: &Path, command: &str) -> TestResult<Value> {
        self.descriptor_with_limit(executable, command, COMMAND_LIMIT)
    }

    fn descriptor_with_limit(
        &self,
        executable: &Path,
        command: &str,
        limit: Duration,
    ) -> TestResult<Value> {
        let output = self.output_with_limit(executable, &[command], limit)?;
        ensure_success(command, &output)?;
        let line = String::from_utf8(output.stdout)?
            .lines()
            .rev()
            .find(|line| !line.trim().is_empty())
            .ok_or_else(|| format!("{command} returned no descriptor"))?
            .to_owned();
        Ok(serde_json::from_str(&line)?)
    }

    fn stop(&self) {
        release_fifo(&self.turn_gate);
        let _ = self
            .output(&self.installed_binary, &["stop"])
            .is_ok_and(|output| output.status.success());
        terminate_isolated_processes(&self.control);
    }
}

impl Drop for IsolatedInstallation {
    fn drop(&mut self) {
        self.stop();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(
    clippy::too_many_lines,
    reason = "the whole-process activation and cleanup ordering is intentionally linear"
)]
async fn deferred_activation_recovers_its_singleton_helper_and_preserves_sessions() -> TestResult {
    let installation = IsolatedInstallation::new()?;
    eprintln!(
        "activation lifecycle: isolated installation ready at {}",
        installation.root.path().display()
    );

    let old_descriptor = installation.descriptor_with_limit(
        &installation.old_binary,
        "endpoint",
        COLD_START_LIMIT,
    )?;
    eprintln!("activation lifecycle: old service endpoint ready");
    let api_socket = descriptor_path(&old_descriptor, "endpoint")?;
    let old_cli_sha = descriptor_string(&old_descriptor, &["cli", "sha256"])?;
    let old_service_sha = descriptor_string(&old_descriptor, &["service", "executable", "sha256"])?;
    assert_eq!(
        old_cli_sha, old_service_sha,
        "old service did not start from build A"
    );

    let old_client = NakodeClient::connect_unix(&api_socket).await?;
    if let Err(error) = wait_for(WAIT_LIMIT, || async {
        old_client
            .get_workspace(installation.workspace.to_string_lossy(), None)
            .await
            .is_ok_and(|state| {
                state
                    .models
                    .iter()
                    .any(|model| model.id.ends_with("/fixture-model"))
            })
    })
    .await
    {
        match old_client
            .get_workspace(installation.workspace.to_string_lossy(), None)
            .await
        {
            Ok(state) => eprintln!("activation lifecycle workspace before timeout: {state:#?}"),
            Err(state_error) => {
                eprintln!("activation lifecycle workspace query failed: {state_error:#}");
            }
        }
        if let Some(runtime_directory) = api_socket.parent() {
            dump_activation_diagnostics(runtime_directory);
        }
        return Err(error);
    }
    eprintln!("activation lifecycle: fixture provider ready");
    let (_, active_session) = old_client
        .open_workspace_session(installation.workspace.to_string_lossy(), None)
        .await?;
    let idle_session = old_client
        .create_session_in_directory(
            old_client.get_session(&active_session).await?.workspace_id,
            Some("Idle session".to_owned()),
            installation.workspace.to_string_lossy(),
        )
        .await?;
    eprintln!("activation lifecycle: logical sessions active={active_session} idle={idle_session}");
    old_client
        .send_prompt(
            &idle_session,
            api::PromptInput {
                text: "idle fixture".to_owned(),
                attachments: Vec::new(),
            },
            None,
        )
        .await?;
    wait_for(WAIT_LIMIT, || async {
        old_client
            .get_session(&idle_session)
            .await
            .is_ok_and(|state| state.active_turn.is_some())
    })
    .await?;
    wait_for(WAIT_LIMIT, || async {
        old_client
            .get_session(&idle_session)
            .await
            .is_ok_and(|state| {
                state.active_turn.is_none()
                    && state.queue.is_empty()
                    && state
                        .transcript
                        .as_ref()
                        .is_some_and(|page| page.entries.len() >= 2)
            })
    })
    .await?;
    eprintln!("activation lifecycle: persisted session is now idle");
    old_client
        .send_prompt(
            &active_session,
            api::PromptInput {
                text: "hello fixture".to_owned(),
                attachments: Vec::new(),
            },
            None,
        )
        .await?;
    wait_for(WAIT_LIMIT, || async {
        old_client
            .get_session(&active_session)
            .await
            .is_ok_and(|state| state.active_turn.is_some())
    })
    .await?;
    eprintln!("activation lifecycle: held owner turn is active");

    let refresh = installation.output_with_limit(
        &installation.installed_binary,
        &["restart-stale"],
        COLD_START_LIMIT,
    )?;
    ensure_success("restart-stale", &refresh)?;
    let refresh_stderr = String::from_utf8(refresh.stderr)?;
    assert!(
        refresh_stderr.contains(
            "installed update pending activation; left live stale service running safely"
        ),
        "stale refresh did not report safe deferral:\n{refresh_stderr}"
    );
    eprintln!("activation lifecycle: stale refresh deferred safely");

    let pending_descriptor =
        installation.descriptor(&installation.installed_binary, "activation-endpoint")?;
    let helper_socket = descriptor_path(&pending_descriptor, "endpoint")?;
    assert_ne!(
        helper_socket, api_socket,
        "pending status must be helper-owned"
    );
    let runtime_directory = helper_socket
        .parent()
        .ok_or("activation endpoint has no runtime directory")?
        .to_path_buf();
    let helper_lock = runtime_directory.join("activation-helper.lock");
    let helper_journal = runtime_directory.join("activation.json");
    let first_helper_pid = wait_for_helper_pid(&helper_lock).await?;
    let first_socket_inode = fs::metadata(&helper_socket)?.ino();

    let activation = ActivationClient::connect_unix(&helper_socket).await?;
    let blocked = wait_for_status(&activation, api::ActivationPhase::Blocked).await?;
    assert!(
        blocked
            .blockers
            .iter()
            .any(|blocker| blocker.session_id == active_session),
        "active logical session was absent from activation blockers"
    );
    assert!(
        helper_journal.is_file(),
        "durable activation journal was not written"
    );
    eprintln!("activation lifecycle: helper reported authoritative blocker");

    // Concurrent discovery must converge on the same durable helper lease.
    let children = (0..4)
        .map(|_| {
            let mut command = installation.command(&installation.installed_binary);
            command
                .arg("activation-endpoint")
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut discovered = Vec::new();
    for child in children {
        let output = child.wait_with_output()?;
        ensure_success("activation-endpoint", &output)?;
        let line = String::from_utf8(output.stdout)?
            .lines()
            .rev()
            .find(|line| !line.trim().is_empty())
            .ok_or("concurrent endpoint discovery returned no descriptor")?
            .to_owned();
        discovered.push(descriptor_path(&serde_json::from_str(&line)?, "endpoint")?);
    }
    assert!(
        discovered.iter().all(|endpoint| endpoint == &helper_socket),
        "concurrent discovery did not converge on one helper endpoint"
    );
    assert_eq!(read_helper_pid(&helper_lock)?, first_helper_pid);
    eprintln!("activation lifecycle: concurrent discovery retained singleton helper");

    // SIGKILL models a helper/process crash. Its lock and socket cannot run Drop cleanup.
    kill_process(first_helper_pid)?;
    wait_for(WAIT_LIMIT, || async { !process_is_alive(first_helper_pid) }).await?;
    eprintln!("activation lifecycle: original helper crashed");
    assert!(
        helper_socket.exists(),
        "crashed helper unexpectedly removed its socket"
    );

    let replacement_descriptor =
        installation.descriptor(&installation.installed_binary, "activation-endpoint")?;
    assert_eq!(
        descriptor_path(&replacement_descriptor, "endpoint")?,
        helper_socket
    );
    let replacement_helper_pid = wait_for_helper_pid(&helper_lock).await?;
    assert_ne!(
        replacement_helper_pid, first_helper_pid,
        "helper crash reused the dead owner"
    );
    wait_for(WAIT_LIMIT, || async {
        fs::metadata(&helper_socket).is_ok_and(|metadata| metadata.ino() != first_socket_inode)
    })
    .await?;
    eprintln!("activation lifecycle: replacement helper reclaimed stale socket");

    // Crash the recovered helper while the blocker is still authoritative. This leaves the durable
    // pending journal as the only activation authority during the later service cutover gap.
    kill_process(replacement_helper_pid)?;
    wait_for(WAIT_LIMIT, || async {
        !process_is_alive(replacement_helper_pid)
    })
    .await?;
    eprintln!("activation lifecycle: replacement helper crashed before cutover");

    release_fifo(&installation.turn_gate);
    wait_for(WAIT_LIMIT, || async {
        old_client
            .get_session(&active_session)
            .await
            .is_ok_and(|state| state.active_turn.is_none() && state.queue.is_empty())
    })
    .await?;
    eprintln!("activation lifecycle: held owner turn completed");

    // Model a helper/process crash in the narrow post-quiescence cutover gap: A has stopped, B has
    // not started, and the durable pending journal is the only activation authority left. A manual
    // check must recover by starting B rather than misreporting the absent service as current.
    let stop = installation.output(&installation.installed_binary, &["stop"])?;
    ensure_success("stop", &stop)?;
    wait_for(WAIT_LIMIT, || async { !api_socket.exists() }).await?;
    eprintln!("activation lifecycle: old service absent in post-quiescence cutover gap");

    let gap_descriptor =
        installation.descriptor(&installation.installed_binary, "activation-endpoint")?;
    assert_eq!(
        descriptor_path(&gap_descriptor, "endpoint")?,
        helper_socket,
        "durable pending activation did not retain the helper endpoint during cutover"
    );
    let cutover_helper_pid = wait_for_helper_pid(&helper_lock).await?;
    assert_ne!(
        cutover_helper_pid, replacement_helper_pid,
        "cutover discovery reused the crashed singleton helper"
    );
    eprintln!("activation lifecycle: cutover discovery recovered singleton helper");

    let replacement_activation = ActivationClient::connect_unix(&helper_socket).await?;
    let Ok(activated) = tokio::time::timeout(
        ACTIVATION_LIMIT,
        replacement_activation.recheck(Some("lifecycle-release".to_owned())),
    )
    .await
    else {
        dump_activation_diagnostics(&runtime_directory);
        return Err(format!(
            "activation recheck did not complete within {}ms",
            ACTIVATION_LIMIT.as_millis()
        )
        .into());
    };
    let activated = activated?;
    assert_eq!(activated.phase, api::ActivationPhase::Activated as i32);
    eprintln!("activation lifecycle: manual check activated build B");

    let current_descriptor = installation.descriptor(&installation.installed_binary, "endpoint")?;
    let current_api = descriptor_path(&current_descriptor, "endpoint")?;
    let current_activation = descriptor_path(&current_descriptor, "activation_endpoint")?;
    assert_eq!(
        current_api, api_socket,
        "service API path changed across handoff"
    );
    assert_eq!(
        current_activation, current_api,
        "activation status did not hand off to service B"
    );
    let current_cli_sha = descriptor_string(&current_descriptor, &["cli", "sha256"])?;
    let current_service_sha =
        descriptor_string(&current_descriptor, &["service", "executable", "sha256"])?;
    assert_eq!(
        current_cli_sha, current_service_sha,
        "installed build B is not active"
    );
    assert_ne!(
        current_service_sha, old_service_sha,
        "test builds were not distinct"
    );
    eprintln!("activation lifecycle: endpoint status handed off to build B");

    let current_client = NakodeClient::connect_unix(&current_api).await?;
    let current_workspace = current_client
        .get_workspace(installation.workspace.to_string_lossy(), None)
        .await?;
    eprintln!(
        "activation lifecycle: build B inventory={:?}",
        current_workspace
            .sessions
            .iter()
            .map(|session| (&session.id, &session.working_directory))
            .collect::<Vec<_>>()
    );
    eprintln!("activation lifecycle: reattaching active session {active_session}");
    assert_eq!(
        current_client
            .open_session_with_attachment(&active_session, SessionAttachment::default())
            .await?,
        active_session,
        "active logical session changed identity while reattaching to build B"
    );
    eprintln!("activation lifecycle: active session reattached");
    let completed = current_client.get_session(&active_session).await?;
    assert_eq!(completed.id, active_session);
    assert!(completed.active_turn.is_none());
    assert!(
        !completed
            .transcript
            .as_ref()
            .is_none_or(|page| page.entries.is_empty()),
        "completed transcript did not survive activation"
    );
    assert_eq!(
        current_client
            .open_session_with_attachment(&idle_session, SessionAttachment::default())
            .await?,
        idle_session,
        "idle logical session changed identity while reattaching to build B"
    );
    assert_eq!(
        current_client.get_session(&idle_session).await?.id,
        idle_session
    );

    let service_activation = ActivationClient::connect_unix(&current_activation).await?;
    assert_eq!(
        service_activation.get_status().await?.phase,
        api::ActivationPhase::Activated as i32
    );
    wait_for(WAIT_LIMIT, || async { !helper_lock.exists() }).await?;
    eprintln!("activation lifecycle: sessions preserved and helper exited");

    installation.stop();
    Ok(())
}

fn wait_for_child_output(
    mut child: std::process::Child,
    label: &str,
    limit: Duration,
) -> TestResult<Output> {
    let started = Instant::now();
    loop {
        if child.try_wait()?.is_some() {
            return Ok(child.wait_with_output()?);
        }
        if started.elapsed() >= limit {
            let _ = child.kill();
            let output = child.wait_with_output()?;
            return Err(format!(
                "isolated command {label:?} exceeded {}ms\nstdout:\n{}\nstderr:\n{}",
                limit.as_millis(),
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }
        std::thread::sleep(WAIT_STEP);
    }
}

fn dump_activation_diagnostics(runtime_directory: &Path) {
    for name in [
        "activation.json",
        "activation-helper.lock",
        "activation.log",
        "service.json",
        "service.log",
    ] {
        let path = runtime_directory.join(name);
        let contents =
            fs::read_to_string(&path).unwrap_or_else(|error| format!("<unavailable: {error}>"));
        eprintln!(
            "activation lifecycle diagnostic {}:\n{}",
            path.display(),
            contents
        );
    }
    for name in ["c.sock", "api.sock", "activation.sock", "activation.lock"] {
        let path = runtime_directory.join(name);
        eprintln!(
            "activation lifecycle diagnostic {} exists={}",
            path.display(),
            path.exists()
        );
    }
}

fn terminate_isolated_processes(root: &Path) {
    let mut pending = vec![root.to_path_buf()];
    let mut pids = Vec::new();
    while let Some(path) = pending.pop() {
        let Ok(entries) = fs::read_dir(path) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
                continue;
            }
            match path.file_name().and_then(|name| name.to_str()) {
                Some("activation-helper.lock") => {
                    if let Ok(pid) = read_helper_pid(&path) {
                        pids.push(pid);
                    }
                }
                Some("service.json") => {
                    if let Ok(record) = fs::read_to_string(&path)
                        && let Ok(record) = serde_json::from_str::<Value>(&record)
                        && let Some(pid) = record.get("pid").and_then(Value::as_u64)
                        && let Ok(pid) = u32::try_from(pid)
                    {
                        pids.push(pid);
                    }
                }
                _ => {}
            }
        }
    }
    pids.sort_unstable();
    pids.dedup();
    for pid in pids {
        if process_is_alive(pid)
            && let Ok(pid) = i32::try_from(pid)
        {
            // SAFETY: records are rooted in this test's private control directory and identify only
            // processes started by this isolated installation.
            let _ = unsafe { libc::kill(pid, libc::SIGKILL) };
        }
    }
}

fn make_fifo(path: &Path) -> TestResult {
    let encoded = CString::new(path.as_os_str().as_bytes())?;
    // SAFETY: `encoded` is a valid NUL-terminated path and mode is a normal permission mask.
    let result = unsafe { libc::mkfifo(encoded.as_ptr(), 0o600) };
    if result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error().into())
    }
}

fn release_fifo(path: &Path) {
    let Ok(encoded) = CString::new(path.as_os_str().as_bytes()) else {
        return;
    };
    // SAFETY: the path pointer is valid for this call. O_NONBLOCK prevents cleanup from hanging if
    // no fixture turn is currently reading the FIFO.
    let descriptor = unsafe { libc::open(encoded.as_ptr(), libc::O_WRONLY | libc::O_NONBLOCK) };
    if descriptor >= 0 {
        let byte = [1_u8];
        // SAFETY: descriptor is open and `byte` points to one readable byte.
        let _ = unsafe { libc::write(descriptor, byte.as_ptr().cast(), byte.len()) };
        // SAFETY: descriptor was returned by `open` above and is closed exactly once.
        let _ = unsafe { libc::close(descriptor) };
    }
}

fn ensure_success(command: &str, output: &Output) -> TestResult {
    if output.status.success() {
        return Ok(());
    }
    Err(format!(
        "{command} failed with {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
    .into())
}

fn descriptor_path(descriptor: &Value, field: &str) -> TestResult<PathBuf> {
    descriptor
        .get(field)
        .and_then(Value::as_str)
        .map(PathBuf::from)
        .ok_or_else(|| format!("descriptor field {field:?} is missing").into())
}

fn descriptor_string(descriptor: &Value, path: &[&str]) -> TestResult<String> {
    let mut value = descriptor;
    for segment in path {
        value = value
            .get(*segment)
            .ok_or_else(|| format!("descriptor path {} is missing", path.join(".")))?;
    }
    value
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| format!("descriptor path {} is not a string", path.join(".")).into())
}

fn read_helper_pid(path: &Path) -> TestResult<u32> {
    let encoded = fs::read_to_string(path)?;
    let (pid, _) = encoded
        .trim()
        .split_once(':')
        .ok_or("helper lease has no instance identity")?;
    Ok(pid.parse()?)
}

async fn wait_for_helper_pid(path: &Path) -> TestResult<u32> {
    let started = Instant::now();
    loop {
        if let Ok(pid) = read_helper_pid(path)
            && process_is_alive(pid)
        {
            return Ok(pid);
        }
        if started.elapsed() >= WAIT_LIMIT {
            return Err(format!("helper lease was not published at {}", path.display()).into());
        }
        tokio::time::sleep(WAIT_STEP).await;
    }
}

fn process_is_alive(pid: u32) -> bool {
    let Ok(pid) = i32::try_from(pid) else {
        return false;
    };
    // SAFETY: signal 0 performs an existence/permission check and does not mutate the process.
    unsafe { libc::kill(pid, 0) == 0 }
}

fn kill_process(pid: u32) -> TestResult {
    let pid = i32::try_from(pid)?;
    // SAFETY: the PID came from this isolated test's helper lease and SIGKILL is intentionally used
    // to exercise crash recovery without running helper destructors.
    if unsafe { libc::kill(pid, libc::SIGKILL) } == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error().into())
    }
}

async fn wait_for<F, Fut>(limit: Duration, mut condition: F) -> TestResult
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let started = Instant::now();
    loop {
        if condition().await {
            return Ok(());
        }
        if started.elapsed() >= limit {
            return Err("timed out waiting for isolated lifecycle condition".into());
        }
        tokio::time::sleep(WAIT_STEP).await;
    }
}

async fn wait_for_status(
    client: &ActivationClient,
    phase: api::ActivationPhase,
) -> TestResult<api::ActivationStatus> {
    let started = Instant::now();
    loop {
        if let Ok(status) = client.get_status().await
            && status.phase == phase as i32
        {
            return Ok(status);
        }
        if started.elapsed() >= WAIT_LIMIT {
            return Err(format!("activation did not reach {phase:?}").into());
        }
        tokio::time::sleep(WAIT_STEP).await;
    }
}
