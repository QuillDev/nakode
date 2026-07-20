#![cfg(unix)]

use std::{
    error::Error,
    ffi::OsString,
    io::{self, Read},
    path::Path,
    process::Command,
    thread,
    time::Duration,
};

use nakode::pty::PtySession;
use portable_pty::PtySize;

#[test]
fn tui_exit_restores_terminal_modes() -> Result<(), Box<dyn Error>> {
    let temp = tempfile::tempdir()?;
    let control_directory = temp.path().join("control");
    let mut session = spawn_tui(temp.path(), &control_directory)?;
    let reader_thread = drain_output(&mut session)?;

    thread::sleep(Duration::from_millis(1_500));
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
    let output = reader_thread
        .join()
        .map_err(|_| io::Error::other("PTY reader thread panicked"))??;
    shutdown_service(temp.path(), &control_directory)?;
    assert!(exited, "Nakode did not exit after Ctrl+D");

    let output = String::from_utf8_lossy(&output);
    assert!(
        output.contains("\u{1b}[?1049h"),
        "alternate screen was not entered"
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
        "mouse selection was not copied with OSC 52; emitted sequences: {:?}",
        output
            .match_indices("\u{1b}]52;")
            .map(|(index, _)| &output[index
                ..output[index..]
                    .find('\u{7}')
                    .map_or(output.len(), |end| index + end + 1)])
            .collect::<Vec<_>>()
    );
    assert!(output.contains("\u{1b}[?25h"), "cursor was not restored");
    assert!(
        output.contains("\u{1b}[?2004l"),
        "bracketed paste was not disabled"
    );
    Ok(())
}

#[test]
fn multiple_tuis_share_one_control_service() -> Result<(), Box<dyn Error>> {
    let temp = tempfile::tempdir()?;
    let control_directory = temp.path().join("control");
    let mut first = spawn_tui(temp.path(), &control_directory)?;
    let first_reader = drain_output(&mut first)?;
    thread::sleep(Duration::from_millis(500));
    assert!(first.try_wait()?.is_none(), "first TUI exited unexpectedly");

    let mut second = spawn_tui(temp.path(), &control_directory)?;
    let second_reader = drain_output(&mut second)?;

    thread::sleep(Duration::from_secs(1));
    if let Some(status) = first.try_wait()? {
        let _ = second.kill();
        let _ = second.wait();
        let first_output = first_reader
            .join()
            .map_err(|_| io::Error::other("first PTY reader thread panicked"))??;
        let second_output = second_reader
            .join()
            .map_err(|_| io::Error::other("second PTY reader thread panicked"))??;
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
        let first_output = first_reader
            .join()
            .map_err(|_| io::Error::other("first PTY reader thread panicked"))??;
        let second_output = second_reader
            .join()
            .map_err(|_| io::Error::other("second PTY reader thread panicked"))??;
        return Err(io::Error::other(format!(
            "second TUI exited unexpectedly ({status:?}):\nfirst:\n{}\nsecond:\n{}",
            String::from_utf8_lossy(&first_output),
            String::from_utf8_lossy(&second_output)
        ))
        .into());
    }

    shutdown_service(temp.path(), &control_directory)?;
    thread::sleep(Duration::from_millis(1_500));
    assert!(
        control_directory.join("control.sock").exists(),
        "attached TUIs did not restart the shared service"
    );
    assert!(
        first.try_wait()?.is_none() && second.try_wait()?.is_none(),
        "a TUI exited while the shared service restarted"
    );

    first.writer().write_all(&[0x04])?;
    first.writer().flush()?;
    second.writer().write_all(&[0x04])?;
    second.writer().flush()?;
    assert!(wait_for_exit(&mut first)?, "first TUI did not exit");
    assert!(wait_for_exit(&mut second)?, "second TUI did not exit");
    first_reader
        .join()
        .map_err(|_| io::Error::other("first PTY reader thread panicked"))??;
    second_reader
        .join()
        .map_err(|_| io::Error::other("second PTY reader thread panicked"))??;
    shutdown_service(temp.path(), &control_directory)?;
    Ok(())
}

fn spawn_tui(workspace: &Path, control_directory: &Path) -> Result<PtySession, Box<dyn Error>> {
    let home = workspace.join("home");
    let data = workspace.join("data");
    std::fs::create_dir_all(&home)?;
    std::fs::create_dir_all(&data)?;
    PtySession::spawn(
        "/usr/bin/env",
        [
            OsString::from(format!("HOME={}", home.display())),
            OsString::from(format!("XDG_DATA_HOME={}", data.display())),
            OsString::from(format!(
                "NAKODE_CONTROL_DIR={}",
                control_directory.display()
            )),
            OsString::from(env!("CARGO_BIN_EXE_nakode")),
            OsString::from("--workspace"),
            workspace.as_os_str().to_owned(),
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

fn drain_output(
    session: &mut PtySession,
) -> Result<thread::JoinHandle<io::Result<Vec<u8>>>, Box<dyn Error>> {
    let mut reader = session
        .take_reader()
        .ok_or_else(|| io::Error::other("PTY output reader was already taken"))?;
    Ok(thread::spawn(move || {
        let mut output = Vec::new();
        let mut buffer = [0_u8; 4096];
        loop {
            match reader.read(&mut buffer) {
                Ok(0) => break,
                Ok(read) => output.extend_from_slice(&buffer[..read]),
                Err(error) if error.raw_os_error() == Some(5) => break,
                Err(error) => return Err(error),
            }
        }
        Ok(output)
    }))
}

fn wait_for_exit(session: &mut PtySession) -> io::Result<bool> {
    for _ in 0..100 {
        if session.try_wait()?.is_some() {
            return Ok(true);
        }
        thread::sleep(Duration::from_millis(50));
    }
    Ok(false)
}

fn shutdown_service(workspace: &Path, control_directory: &Path) -> io::Result<()> {
    let status = Command::new(env!("CARGO_BIN_EXE_nakode"))
        .args(["service", "shutdown"])
        .env("NAKODE_CONTROL_DIR", control_directory)
        .current_dir(workspace)
        .status()?;
    if status.success() {
        Ok(())
    } else {
        Err(io::Error::other("could not stop test control service"))
    }
}
