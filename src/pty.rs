//! Dedicated boundary for interactive subprocesses.
//!
//! Codex app-server uses ordinary piped stdio because its transport is JSONL.
//! Interactive shells and terminal tools belong here so PTY resize, I/O, and
//! process cleanup never leak into the renderer or Codex protocol client.

use std::{
    ffi::OsString,
    io::{Read, Write},
    path::Path,
};

use anyhow::Result;
use portable_pty::{Child, CommandBuilder, ExitStatus, MasterPty, PtySize, native_pty_system};

pub struct PtySession {
    master: Box<dyn MasterPty + Send>,
    child: Box<dyn Child + Send + Sync>,
    reader: Option<Box<dyn Read + Send>>,
    writer: Box<dyn Write + Send>,
}

impl PtySession {
    /// Spawn an interactive process in the platform-native PTY.
    ///
    /// This API is synchronous by design. Tokio callers should perform reads
    /// and blocking waits inside `spawn_blocking` tasks, then forward bytes to
    /// the application reducer through bounded channels.
    pub fn spawn(
        program: impl Into<OsString>,
        args: impl IntoIterator<Item = OsString>,
        cwd: &Path,
        size: PtySize,
    ) -> Result<Self> {
        let pair = native_pty_system().openpty(size)?;
        let mut command = CommandBuilder::new(program.into());
        command.args(args);
        command.cwd(cwd);

        let child = pair.slave.spawn_command(command)?;
        drop(pair.slave);
        let reader = pair.master.try_clone_reader()?;
        let writer = pair.master.take_writer()?;

        Ok(Self {
            master: pair.master,
            child,
            reader: Some(reader),
            writer,
        })
    }

    pub fn take_reader(&mut self) -> Option<Box<dyn Read + Send>> {
        self.reader.take()
    }

    pub fn writer(&mut self) -> &mut (dyn Write + Send) {
        &mut *self.writer
    }

    pub fn resize(&self, rows: u16, cols: u16) -> Result<()> {
        self.master.resize(PtySize {
            rows,
            cols,
            pixel_width: 0,
            pixel_height: 0,
        })?;
        Ok(())
    }

    pub fn process_id(&self) -> Option<u32> {
        self.child.process_id()
    }

    pub fn try_wait(&mut self) -> std::io::Result<Option<ExitStatus>> {
        self.child.try_wait()
    }

    pub fn wait(&mut self) -> std::io::Result<ExitStatus> {
        self.child.wait()
    }

    pub fn kill(&mut self) -> std::io::Result<()> {
        self.child.kill()
    }
}

impl Drop for PtySession {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use std::{ffi::OsString, io::Read};

    use portable_pty::PtySize;

    use super::PtySession;

    #[test]
    fn native_pty_spawns_resizes_and_captures_output() {
        let mut session = PtySession::spawn(
            "/bin/sh",
            [OsString::from("-c"), OsString::from("printf pty-ok")],
            &std::env::current_dir().expect("current directory"),
            PtySize::default(),
        )
        .expect("spawn shell in native PTY");
        session.resize(40, 120).expect("resize native PTY");
        let mut reader = session.take_reader().expect("PTY reader");
        let mut output = [0_u8; 6];
        reader.read_exact(&mut output).expect("read PTY output");
        assert_eq!(&output, b"pty-ok");

        let status = session.wait().expect("wait for PTY child");
        assert!(status.success());
    }
}
