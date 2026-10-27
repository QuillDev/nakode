//! A minimal Chrome `DevTools` Protocol client over `--remote-debugging-pipe`.
//!
//! Chromium reads NUL-terminated JSON commands from fd 3 and writes responses and events to fd 4.
//! Both ends of one socket pair are handed to it, so no debugging port is ever opened and no other
//! process can attach to the browser.

use std::{
    collections::HashMap,
    os::unix::{net::UnixStream as StdUnixStream, process::CommandExt as _},
    path::Path,
    process::Stdio,
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _, BufReader},
    net::{UnixStream, unix::OwnedWriteHalf},
    process::{Child, Command},
    sync::oneshot,
};

/// Longest wait for one protocol reply; navigation waits are bounded separately.
const COMMAND_TIMEOUT: Duration = Duration::from_secs(15);
/// A single protocol message larger than this ends the connection (screenshots stay far below).
const MAX_MESSAGE_BYTES: usize = 64 * 1024 * 1024;

type Pending = Arc<Mutex<HashMap<u64, oneshot::Sender<Result<Value, String>>>>>;

/// Receives every protocol event: its method, the target session it came from, and its params.
pub(crate) type EventSink = Box<dyn Fn(&str, Option<&str>, &Value) + Send + Sync>;

pub(crate) struct Connection {
    writer: tokio::sync::Mutex<OwnedWriteHalf>,
    pending: Pending,
    next_id: AtomicU64,
}

impl Connection {
    /// Sends one command, to the browser when `session` is `None`, and waits for its result.
    pub(crate) async fn call(
        &self,
        session: Option<&str>,
        method: &str,
        params: Value,
    ) -> Result<Value, String> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let mut message = json!({"id": id, "method": method, "params": params});
        if let Some(session) = session {
            message["sessionId"] = Value::from(session);
        }
        let (sender, receiver) = oneshot::channel();
        self.pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(id, sender);
        let mut bytes = message.to_string().into_bytes();
        bytes.push(0);
        let written = {
            let mut writer = self.writer.lock().await;
            writer.write_all(&bytes).await
        };
        if let Err(error) = written {
            self.forget(id);
            return Err(format!("the browser connection closed: {error}"));
        }
        match tokio::time::timeout(COMMAND_TIMEOUT, receiver).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err("the browser exited".to_owned()),
            Err(_) => {
                self.forget(id);
                Err(format!("the browser did not answer {method} in time"))
            }
        }
    }

    fn forget(&self, id: u64) {
        self.pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&id);
    }
}

/// Starts the browser and connects to it. The child is killed when it is dropped.
pub(crate) fn launch(
    executable: &Path,
    arguments: &[String],
    events: EventSink,
) -> Result<(Child, Arc<Connection>), String> {
    let (ours, theirs) = StdUnixStream::pair()
        .map_err(|error| format!("could not connect to the browser: {error}"))?;
    let theirs = std::os::fd::IntoRawFd::into_raw_fd(theirs);
    let mut command = Command::new(executable);
    command
        .args(arguments)
        .arg("--remote-debugging-pipe")
        .envs(crate::machine_path::environment())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    // SAFETY: only async-signal-safe `fcntl` and `dup2` calls run between fork and exec. The
    // socket is first copied above fd 4 without close-on-exec, so it survives when it already is
    // fd 3 or 4, then placed on the two descriptors Chromium's pipe transport reads and writes.
    unsafe {
        command.as_std_mut().pre_exec(move || {
            let copy = nix::libc::fcntl(theirs, nix::libc::F_DUPFD, 10);
            if copy < 0 || nix::libc::dup2(copy, 3) < 0 || nix::libc::dup2(copy, 4) < 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let spawned = command.spawn();
    // SAFETY: `theirs` came from `into_raw_fd` above and is closed exactly once, here.
    drop(unsafe { <std::os::fd::OwnedFd as std::os::fd::FromRawFd>::from_raw_fd(theirs) });
    let child = spawned.map_err(|error| {
        format!(
            "could not start the browser {}: {error}",
            executable.display()
        )
    })?;
    ours.set_nonblocking(true)
        .map_err(|error| format!("could not connect to the browser: {error}"))?;
    let stream = UnixStream::from_std(ours)
        .map_err(|error| format!("could not connect to the browser: {error}"))?;
    let (reader, writer) = stream.into_split();
    let pending: Pending = Arc::default();
    tokio::spawn(read_messages(reader, pending.clone(), events));
    Ok((
        child,
        Arc::new(Connection {
            writer: tokio::sync::Mutex::new(writer),
            pending,
            next_id: AtomicU64::new(1),
        }),
    ))
}

async fn read_messages(
    reader: tokio::net::unix::OwnedReadHalf,
    pending: Pending,
    events: EventSink,
) {
    let mut reader = BufReader::new(reader);
    let mut buffer = Vec::new();
    loop {
        buffer.clear();
        match (&mut reader)
            .take(u64::try_from(MAX_MESSAGE_BYTES).unwrap_or(u64::MAX))
            .read_until(0, &mut buffer)
            .await
        {
            Ok(0) | Err(_) => break,
            Ok(_) => {}
        }
        if buffer.last() != Some(&0) {
            break;
        }
        buffer.pop();
        let Ok(message) = serde_json::from_slice::<Value>(&buffer) else {
            continue;
        };
        if let Some(id) = message.get("id").and_then(Value::as_u64) {
            let sender = pending
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(&id);
            if let Some(sender) = sender {
                let result = match message.get("error") {
                    Some(error) => Err(error
                        .get("message")
                        .and_then(Value::as_str)
                        .unwrap_or("the browser refused the command")
                        .to_owned()),
                    None => Ok(message.get("result").cloned().unwrap_or(Value::Null)),
                };
                let _ = sender.send(result);
            }
        } else if let Some(method) = message.get("method").and_then(Value::as_str) {
            let session = message.get("sessionId").and_then(Value::as_str);
            events(
                method,
                session,
                message.get("params").unwrap_or(&Value::Null),
            );
        }
    }
    // Every waiting call learns that the browser is gone.
    pending
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clear();
}
