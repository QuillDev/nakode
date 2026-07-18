#![cfg(unix)]

use std::{
    error::Error,
    ffi::OsString,
    fs,
    io::{self, Read},
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    thread,
    time::Duration,
};

use nako_agent::pty::PtySession;
use portable_pty::PtySize;

#[test]
fn tui_exit_restores_terminal_modes() -> Result<(), Box<dyn Error>> {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let fixture = manifest.join("tests/fixtures/fake_codex.py");
    let temp = tempfile::tempdir()?;
    let wrapper = temp.path().join("codex-fixture");
    fs::write(
        &wrapper,
        format!("#!/bin/sh\nexec python3 '{}'\n", fixture.display()),
    )?;
    let mut permissions = fs::metadata(&wrapper)?.permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&wrapper, permissions)?;

    let mut session = PtySession::spawn(
        env!("CARGO_BIN_EXE_nako-agent"),
        [
            OsString::from("--codex"),
            wrapper.into_os_string(),
            OsString::from("--workspace"),
            temp.path().as_os_str().to_owned(),
        ],
        temp.path(),
        PtySize {
            rows: 28,
            cols: 100,
            pixel_width: 0,
            pixel_height: 0,
        },
    )?;
    let mut reader = session
        .take_reader()
        .ok_or_else(|| io::Error::other("PTY output reader was already taken"))?;
    let reader_thread = thread::spawn(move || -> io::Result<Vec<u8>> {
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
    });

    thread::sleep(Duration::from_millis(750));
    session.writer().write_all(b"NAKO")?;
    session.writer().flush()?;
    thread::sleep(Duration::from_millis(150));
    // Select the composer text (SGR mouse coordinates are 1-based).
    // Send each event separately so Crossterm's async event stream observes the
    // press before interpreting the drag and release.
    for event in [
        b"\x1b[<0;2;24M".as_slice(),
        b"\x1b[<32;6;24M".as_slice(),
        b"\x1b[<0;6;24m".as_slice(),
    ] {
        session.writer().write_all(event)?;
        session.writer().flush()?;
        thread::sleep(Duration::from_millis(75));
    }
    thread::sleep(Duration::from_millis(500));
    session.writer().write_all(&[0x04])?;
    session.writer().flush()?;

    let mut exited = false;
    for _ in 0..100 {
        if session.try_wait()?.is_some() {
            exited = true;
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    if !exited {
        let _ = session.kill();
        let _ = session.wait();
    }
    let output = reader_thread
        .join()
        .map_err(|_| io::Error::other("PTY reader thread panicked"))??;
    assert!(exited, "Nako Agent did not exit after Ctrl+D");

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
        output.contains("\u{1b}]52;c;TkFLTw==\u{7}"),
        "mouse selection was not copied with OSC 52"
    );
    assert!(output.contains("\u{1b}[?25h"), "cursor was not restored");
    assert!(
        output.contains("\u{1b}[?2004l"),
        "bracketed paste was not disabled"
    );
    Ok(())
}
