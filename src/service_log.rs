//! Bounded capture of one workspace service's lifecycle output.
//!
//! A background service has no terminal, so its standard output and standard
//! error are redirected into `service.log` inside the same private per-workspace
//! directory that holds `c.sock` and `api.sock`. Only service lifecycle and
//! error output reaches that file; prompts, transcripts, and provider payloads
//! are never written to standard output by the server, matching the
//! prompt-free posture of `nakode diagnostics`.
//!
//! Growth is bounded by a size cap with one archived generation, so a workspace
//! retains roughly [`MAX_LOG_BYTES`] twice over: the live file, plus one archive
//! holding whatever the cap had grown to when the size was last checked.
//! Rotation copies the
//! live file to `service.log.1` and then truncates the live file in place
//! rather than renaming it: the running service inherited a descriptor for that
//! file and keeps it open for its whole life, so a rename would leave the
//! service appending to an unlinked inode while `nakode logs` watched an empty
//! one. The descriptor is opened for append, so writes resume at offset zero
//! after truncation.

use std::{
    fs::{File, OpenOptions},
    io,
    path::{Path, PathBuf},
    time::Duration,
};

/// Maximum size of the live log file before it is archived and truncated.
pub const MAX_LOG_BYTES: u64 = 5 * 1024 * 1024;

/// How often a running background service re-checks its own log size.
const SIZE_CHECK_INTERVAL: Duration = Duration::from_secs(30);

/// How often `--follow` polls for appended output.
const FOLLOW_INTERVAL: Duration = Duration::from_millis(250);

/// Names the log file a background service was started with.
///
/// The foreground `nakode run` never receives this variable, so it neither
/// redirects nor rotates anything and keeps printing to its terminal.
pub const LOG_PATH_ENVIRONMENT: &str = "NAKODE_SERVICE_LOG";

/// Trailing log content and the offset that follows it.
pub struct Tail {
    pub text: String,
    pub offset: u64,
}

/// Returns the archive path holding the previous generation of `log`.
#[must_use]
pub fn archive_path(log: &Path) -> PathBuf {
    let mut archive = log.as_os_str().to_owned();
    archive.push(".1");
    PathBuf::from(archive)
}

/// Opens the log file for appending, creating it when absent.
///
/// # Errors
/// Returns an error when the file cannot be created or opened.
pub fn open_for_append(log: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.create(true).append(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(log)
}

/// Archives and truncates the log when it exceeds [`MAX_LOG_BYTES`].
///
/// Reports whether rotation happened. A missing log file is not an error.
///
/// # Errors
/// Returns an error when the oversized log cannot be archived or truncated.
pub fn rotate_if_oversized(log: &Path) -> io::Result<bool> {
    let Ok(metadata) = std::fs::metadata(log) else {
        return Ok(false);
    };
    if metadata.len() <= MAX_LOG_BYTES {
        return Ok(false);
    }
    std::fs::copy(log, archive_path(log))?;
    OpenOptions::new().write(true).open(log)?.set_len(0)?;
    Ok(true)
}

/// Keeps a running background service's own log within its size cap.
///
/// Checking only when the service starts would leave one long-lived service run
/// unbounded, so the service re-checks its own log while it runs.
pub async fn supervise_size(log: PathBuf) {
    loop {
        tokio::time::sleep(SIZE_CHECK_INTERVAL).await;
        if let Err(error) = rotate_if_oversized(&log) {
            eprintln!("nakode: could not rotate {}: {error}", log.display());
        }
    }
}

/// Reads the last `lines` lines of the log and the offset that follows them.
///
/// Returns `None` when the log file does not exist yet.
///
/// # Errors
/// Returns an error when an existing log file cannot be read.
pub fn tail(log: &Path, lines: u32) -> io::Result<Option<Tail>> {
    let content = match std::fs::read(log) {
        Ok(content) => content,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let offset = u64::try_from(content.len()).unwrap_or(u64::MAX);
    let text = String::from_utf8_lossy(&content);
    let retained = text
        .lines()
        .rev()
        .take(usize::try_from(lines).unwrap_or(usize::MAX))
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect::<Vec<_>>()
        .join("\n");
    Ok(Some(Tail {
        text: retained,
        offset,
    }))
}

/// Reads log content appended after `offset`, tolerating rotation.
///
/// A log shorter than `offset` was truncated by rotation, so reading restarts
/// from the beginning of the new generation.
///
/// # Errors
/// Returns an error when an existing log file cannot be read.
pub fn read_from(log: &Path, offset: u64) -> io::Result<Option<Tail>> {
    let content = match std::fs::read(log) {
        Ok(content) => content,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let length = u64::try_from(content.len()).unwrap_or(u64::MAX);
    let start = if length < offset {
        0
    } else {
        usize::try_from(offset).unwrap_or(content.len())
    };
    Ok(Some(Tail {
        text: String::from_utf8_lossy(&content[start.min(content.len())..]).into_owned(),
        offset: length,
    }))
}

/// Prints log content appended after `offset` until the process is interrupted.
///
/// # Errors
/// Returns an error when an existing log file cannot be read.
pub async fn follow(log: &Path, mut offset: u64) -> io::Result<()> {
    use std::io::Write;

    loop {
        tokio::time::sleep(FOLLOW_INTERVAL).await;
        let Some(appended) = read_from(log, offset)? else {
            offset = 0;
            continue;
        };
        offset = appended.offset;
        if !appended.text.is_empty() {
            print!("{}", appended.text);
            io::stdout().flush()?;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        MAX_LOG_BYTES, archive_path, open_for_append, read_from, rotate_if_oversized, tail,
    };
    use std::io::Write;

    #[test]
    fn tail_returns_none_before_the_service_writes_anything() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let log = directory.path().join("service.log");

        assert!(
            tail(&log, 10)
                .expect("missing log is not an error")
                .is_none()
        );
    }

    #[test]
    fn tail_retains_only_the_requested_trailing_lines() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let log = directory.path().join("service.log");
        std::fs::write(&log, "first\nsecond\nthird\n").expect("write log");

        let tailed = tail(&log, 2).expect("read log").expect("existing log");
        assert_eq!(tailed.text, "second\nthird");
        assert_eq!(tailed.offset, 19);
    }

    #[test]
    fn appended_output_is_read_from_the_previous_offset() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let log = directory.path().join("service.log");
        std::fs::write(&log, "first\n").expect("write log");

        let first = read_from(&log, 0).expect("read log").expect("existing log");
        assert_eq!(first.text, "first\n");

        std::fs::write(&log, "first\nsecond\n").expect("append log");
        let second = read_from(&log, first.offset)
            .expect("read log")
            .expect("existing log");
        assert_eq!(second.text, "second\n");
    }

    #[test]
    fn rotation_archives_the_log_and_keeps_the_open_descriptor_writing() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let log = directory.path().join("service.log");
        let mut writer = open_for_append(&log).expect("open log");
        writer
            .write_all(&vec![
                b'x';
                usize::try_from(MAX_LOG_BYTES).expect("cap fits usize")
                    + 1
            ])
            .expect("fill log");
        writer.flush().expect("flush log");

        assert!(rotate_if_oversized(&log).expect("rotate oversized log"));
        assert_eq!(
            std::fs::metadata(archive_path(&log))
                .expect("archived log")
                .len(),
            MAX_LOG_BYTES + 1
        );
        assert_eq!(std::fs::metadata(&log).expect("live log").len(), 0);

        writer.write_all(b"after rotation\n").expect("write log");
        writer.flush().expect("flush log");
        assert_eq!(
            std::fs::read_to_string(&log).expect("live log"),
            "after rotation\n",
            "an appending descriptor must resume at the start of the truncated file"
        );

        assert!(
            !rotate_if_oversized(&log).expect("small log is left alone"),
            "a log within its cap must not be rotated"
        );
    }
}
