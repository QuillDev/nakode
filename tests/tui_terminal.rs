#![cfg(unix)]

use std::{
    error::Error,
    ffi::OsString,
    io::{self, Read},
    path::{Path, PathBuf},
    process::Command,
    sync::{Arc, LazyLock, Mutex},
    thread,
    time::{Duration, Instant},
};

use nakode::pty::PtySession;
use portable_pty::PtySize;

static TUI_TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));
const TUI_READY_TIMEOUT: Duration = Duration::from_secs(30);

#[test]
fn tui_from_unregistered_current_directory_reaches_terminal_and_restores_modes()
-> Result<(), Box<dyn Error>> {
    let _guard = TUI_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let temp = tempfile::tempdir()?;
    let home = temp.path().join("nakode-home");
    let _service_cleanup = ServiceCleanup::new(temp.path(), &home);
    let mut session = spawn_tui(temp.path(), &home)?;
    let output_drain = drain_output(&mut session)?;
    if !output_drain.wait_for(b"\x1b[?1049h", TUI_READY_TIMEOUT) {
        let _ = session.kill();
        let _ = session.wait();
        let output = output_drain.finish()?;
        shutdown_service(temp.path(), &home)?;
        return Err(io::Error::other(format!(
            "TUI did not acquire the alternate screen before input:\n{}",
            String::from_utf8_lossy(&output)
        ))
        .into());
    }
    session.writer().write_all(b"NAKODE")?;
    session.writer().flush()?;
    thread::sleep(Duration::from_millis(150));
    // Select the composer text (SGR mouse coordinates are 1-based).
    // Send each event separately so Crossterm's async event stream observes the
    // press before interpreting the drag and release.
    for event in [
        b"\x1b[<0;2;24M".as_slice(),
        b"\x1b[<32;7;24M".as_slice(),
        b"\x1b[<0;7;24m".as_slice(),
    ] {
        session.writer().write_all(event)?;
        session.writer().flush()?;
        thread::sleep(Duration::from_millis(75));
    }
    thread::sleep(Duration::from_millis(500));
    session.writer().write_all(&[0x04])?;
    session.writer().flush()?;

    let exited = wait_for_exit(&mut session)?;
    if !exited {
        let _ = session.kill();
        let _ = session.wait();
    }
    let output = output_drain.finish()?;
    shutdown_service(temp.path(), &home)?;
    assert!(exited, "Nakode did not exit after Ctrl+D");

    let output = String::from_utf8_lossy(&output);
    assert!(
        output.contains("\u{1b}[?1049h"),
        "alternate screen was not entered; output:\n{output}"
    );
    assert!(
        output.contains("\u{1b}[?1049l"),
        "alternate screen was not left"
    );
    assert!(
        output.contains("\u{1b}[>13u") && output.contains("\u{1b}[<1u"),
        "enhanced keyboard reporting was not enabled and restored"
    );
    assert!(
        output.contains("\u{1b}]52;c;TkFLT0RF\u{7}"),
        "mouse selection was not copied with OSC 52; emitted sequences: {:?}; output:\n{}",
        output
            .match_indices("\u{1b}]52;")
            .map(|(index, _)| &output[index
                ..output[index..]
                    .find('\u{7}')
                    .map_or(output.len(), |end| index + end + 1)])
            .collect::<Vec<_>>(),
        output,
    );
    assert!(output.contains("\u{1b}[?25h"), "cursor was not restored");
    assert!(
        output.contains("\u{1b}[?2004l"),
        "bracketed paste was not disabled"
    );
    Ok(())
}

#[test]
fn multiple_tuis_share_one_server_without_owning_its_lifecycle() -> Result<(), Box<dyn Error>> {
    let _guard = TUI_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let temp = tempfile::tempdir()?;
    let home = temp.path().join("nakode-home");
    let _service_cleanup = ServiceCleanup::new(temp.path(), &home);
    let mut first = spawn_tui(temp.path(), &home)?;
    let first_reader = drain_output(&mut first)?;
    if !first_reader.wait_for(b"\x1b[?1049h", TUI_READY_TIMEOUT) {
        let _ = first.kill();
        let _ = first.wait();
        let output = first_reader.finish()?;
        shutdown_service(temp.path(), &home)?;
        return Err(io::Error::other(format!(
            "first TUI did not become ready:\n{}",
            String::from_utf8_lossy(&output)
        ))
        .into());
    }

    let mut second = spawn_tui(temp.path(), &home)?;
    let second_reader = drain_output(&mut second)?;
    if !second_reader.wait_for(b"\x1b[?1049h", TUI_READY_TIMEOUT) {
        let _ = first.kill();
        let _ = second.kill();
        let _ = first.wait();
        let _ = second.wait();
        let first_output = first_reader.finish()?;
        let second_output = second_reader.finish()?;
        shutdown_service(temp.path(), &home)?;
        return Err(io::Error::other(format!(
            "second TUI did not become ready:\nfirst:\n{}\nsecond:\n{}",
            String::from_utf8_lossy(&first_output),
            String::from_utf8_lossy(&second_output)
        ))
        .into());
    }

    if let Some(status) = first.try_wait()? {
        let _ = second.kill();
        let _ = second.wait();
        let first_output = first_reader.finish()?;
        let second_output = second_reader.finish()?;
        return Err(io::Error::other(format!(
            "first TUI exited unexpectedly ({status:?}):\nfirst:\n{}\nsecond:\n{}",
            String::from_utf8_lossy(&first_output),
            String::from_utf8_lossy(&second_output)
        ))
        .into());
    }
    if let Some(status) = second.try_wait()? {
        let _ = first.kill();
        let _ = first.wait();
        let first_output = first_reader.finish()?;
        let second_output = second_reader.finish()?;
        return Err(io::Error::other(format!(
            "second TUI exited unexpectedly ({status:?}):\nfirst:\n{}\nsecond:\n{}",
            String::from_utf8_lossy(&first_output),
            String::from_utf8_lossy(&second_output)
        ))
        .into());
    }

    shutdown_service(temp.path(), &home)?;
    if let Err(error) = assert_stays_alive(&mut first, Duration::from_secs(1), "first TUI")
        .and_then(|()| assert_stays_alive(&mut second, Duration::from_secs(1), "second TUI"))
    {
        let _ = first.kill();
        let _ = second.kill();
        let _ = first.wait();
        let _ = second.wait();
        first_reader.finish()?;
        second_reader.finish()?;
        return Err(error.into());
    }

    first.writer().write_all(&[0x04])?;
    first.writer().flush()?;
    second.writer().write_all(&[0x04])?;
    second.writer().flush()?;
    assert!(wait_for_exit(&mut first)?, "first TUI did not exit");
    assert!(wait_for_exit(&mut second)?, "second TUI did not exit");
    first_reader.finish()?;
    second_reader.finish()?;
    shutdown_service(temp.path(), &home)?;
    Ok(())
}

fn spawn_tui(workspace: &Path, control_directory: &Path) -> Result<PtySession, Box<dyn Error>> {
    let user_home = workspace.join("home");
    let data = workspace.join("data");
    std::fs::create_dir_all(&user_home)?;
    std::fs::create_dir_all(&data)?;
    PtySession::spawn(
        "/usr/bin/env",
        [
            OsString::from("NAKODE_TERMINAL_IMAGES=off"),
            OsString::from(format!("HOME={}", user_home.display())),
            OsString::from(format!("XDG_DATA_HOME={}", data.display())),
            OsString::from(format!(
                "NAKODE_CONTROL_DIR={}",
                control_directory.display()
            )),
            OsString::from(env!("CARGO_BIN_EXE_nakode")),
            OsString::from("--tui"),
        ],
        workspace,
        PtySize {
            rows: 28,
            cols: 100,
            pixel_width: 0,
            pixel_height: 0,
        },
    )
    .map_err(Into::into)
}

fn drain_output(session: &mut PtySession) -> Result<PtyOutputDrain, Box<dyn Error>> {
    let mut reader = session
        .take_reader()
        .ok_or_else(|| io::Error::other("PTY output reader was already taken"))?;
    let output = Arc::new(Mutex::new(Vec::new()));
    let thread_output = Arc::clone(&output);
    let worker = thread::spawn(move || {
        let mut buffer = [0_u8; 4096];
        loop {
            match reader.read(&mut buffer) {
                Ok(0) => break,
                Ok(read) => thread_output
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .extend_from_slice(&buffer[..read]),
                Err(error) if error.raw_os_error() == Some(5) => break,
                Err(error) => return Err(error),
            }
        }
        Ok(())
    });
    Ok(PtyOutputDrain { output, worker })
}

struct PtyOutputDrain {
    output: Arc<Mutex<Vec<u8>>>,
    worker: thread::JoinHandle<io::Result<()>>,
}

impl PtyOutputDrain {
    fn wait_for(&self, needle: &[u8], timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            if self
                .output
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .windows(needle.len())
                .any(|window| window == needle)
            {
                return true;
            }
            if self.worker.is_finished() || Instant::now() >= deadline {
                return false;
            }
            thread::sleep(Duration::from_millis(25));
        }
    }

    fn finish(self) -> io::Result<Vec<u8>> {
        self.worker
            .join()
            .map_err(|_| io::Error::other("PTY reader thread panicked"))??;
        Ok(self
            .output
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone())
    }
}

fn assert_stays_alive(session: &mut PtySession, duration: Duration, label: &str) -> io::Result<()> {
    let deadline = Instant::now() + duration;
    while Instant::now() < deadline {
        if let Some(status) = session.try_wait()? {
            return Err(io::Error::other(format!(
                "{label} exited when the independently owned service stopped: {status:?}"
            )));
        }
        thread::sleep(Duration::from_millis(25));
    }
    Ok(())
}

fn wait_for_exit(session: &mut PtySession) -> io::Result<bool> {
    for _ in 0..200 {
        if session.try_wait()?.is_some() {
            return Ok(true);
        }
        thread::sleep(Duration::from_millis(50));
    }
    Ok(false)
}

struct ServiceCleanup {
    workspace: PathBuf,
    control_directory: PathBuf,
}

impl ServiceCleanup {
    fn new(workspace: &Path, control_directory: &Path) -> Self {
        Self {
            workspace: workspace.to_path_buf(),
            control_directory: control_directory.to_path_buf(),
        }
    }
}

impl Drop for ServiceCleanup {
    fn drop(&mut self) {
        let _ = shutdown_service(&self.workspace, &self.control_directory);
    }
}

fn shutdown_service(workspace: &Path, control_directory: &Path) -> io::Result<()> {
    let status = Command::new(env!("CARGO_BIN_EXE_nakode"))
        .arg("stop")
        .env("NAKODE_CONTROL_DIR", control_directory)
        .current_dir(workspace)
        .status()?;
    if status.success() {
        Ok(())
    } else {
        Err(io::Error::other("could not stop test control service"))
    }
}
