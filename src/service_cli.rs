//! Command implementations for the service-facing `nakode` CLI.
//!
//! `--workspace` selects the canonical workspace service every command here
//! reaches. None of these commands enumerate or act on a workspace the caller
//! did not name.

use std::fmt::Write;

use crate::{
    config::Config,
    control_service::{self, ControlError, ServicePaths, ServiceStatus, StartOutcome, StopOutcome},
    service_log,
};

/// Runs the service in the foreground until it is stopped.
///
/// # Errors
/// Returns an error when the service cannot acquire its sockets or a component
/// fails.
pub async fn run(config: Config) -> Result<(), ControlError> {
    control_service::run_service(config).await
}

/// Starts the service in the background, waiting for readiness.
///
/// # Errors
/// Returns an error when the service cannot be started.
pub async fn start(config: &Config) -> Result<(), ControlError> {
    let executable = current_executable()?;
    match control_service::start_service(&executable, config).await? {
        StartOutcome::AlreadyRunning => {
            println!("Nakode service: already running{}", pid_suffix(config));
        }
        StartOutcome::Started => {
            println!("Nakode service: started{}", pid_suffix(config));
        }
    }
    Ok(())
}

/// Stops the service.
///
/// # Errors
/// Returns an error when a live service rejects or ignores the request.
pub async fn stop(config: &Config) -> Result<(), ControlError> {
    match control_service::stop_service(&ServicePaths::of(config)?).await? {
        StopOutcome::AlreadyStopped => println!("Nakode service: already stopped"),
        StopOutcome::Stopped => println!("Nakode service: stopped"),
    }
    Ok(())
}

/// Restarts the service in the background.
///
/// # Errors
/// Returns an error when the service cannot be stopped or restarted.
pub async fn restart(config: &Config) -> Result<(), ControlError> {
    let executable = current_executable()?;
    control_service::restart_service(&executable, config).await?;
    println!("Nakode service: restarted{}", pid_suffix(config));
    Ok(())
}

/// Reports service state as text or JSON.
///
/// # Errors
/// Returns an error when lifecycle state cannot be read or serialized.
pub async fn status(config: &Config, json: bool) -> Result<(), ControlError> {
    let status = control_service::service_status(config).await?;
    if json {
        println!("{}", serde_json::to_string_pretty(&status)?);
    } else {
        print!("{}", render_status(&status));
    }
    Ok(())
}

/// Prints the tail of the service log, optionally following it.
///
/// # Errors
/// Returns an error when the log file exists but cannot be read.
pub async fn logs(config: &Config, follow: bool, lines: u32) -> Result<(), ControlError> {
    let log = ServicePaths::of(config)?.log().to_path_buf();
    let read_failed = |source| ControlError::Io {
        path: log.display().to_string(),
        source,
    };
    let mut offset = 0;
    if let Some(tail) = service_log::tail(&log, lines).map_err(read_failed)? {
        if !tail.text.is_empty() {
            println!("{}", tail.text);
        }
        offset = tail.offset;
    } else {
        eprintln!(
            "nakode: no service log yet at {}. A service started with `nakode run` writes to its terminal instead.",
            log.display()
        );
    }
    if follow {
        service_log::follow(&log, offset)
            .await
            .map_err(read_failed)?;
    }
    Ok(())
}

/// Prints the private gRPC endpoint descriptor consumed by native frontends.
///
/// The descriptor is the only thing written to standard output; every notice
/// belongs on standard error so connectors can parse this stream.
///
/// # Errors
/// Returns an error when the endpoint cannot be reached or started.
pub async fn endpoint(config: &Config) -> Result<(), ControlError> {
    let executable = current_executable()?;
    let endpoint = control_service::frontend_api_endpoint(&executable, config).await?;
    let descriptor = serde_json::json!({
        "version": 1,
        "transport": "grpc+unix",
        "workspace": config.workspace,
        "endpoint": endpoint,
    });
    println!("{}", serde_json::to_string(&descriptor)?);
    Ok(())
}

/// Warns that a `nakode service` spelling is deprecated and names its
/// replacement.
///
/// The notice goes to standard error so it never joins the machine-readable
/// output of the command it precedes.
pub fn report_deprecation(deprecated: &str, replacement: &str) {
    eprintln!("nakode: `{deprecated}` is deprecated; use `{replacement}`.");
}

fn render_status(status: &ServiceStatus) -> String {
    let mut rows = vec![
        ("Nakode", status.nakode_version.clone()),
        ("Workspace", status.workspace.display().to_string()),
        (
            "Service",
            if status.running { "running" } else { "stopped" }.to_owned(),
        ),
    ];
    if let Some(pid) = status.pid {
        rows.push(("PID", pid.to_string()));
    }
    if let Some(started) = &status.started_at_utc {
        let uptime = status.uptime_seconds.map_or_else(String::new, |seconds| {
            format!(" (up {})", duration(seconds))
        });
        rows.push(("Started", format!("{started}{uptime}")));
    }
    rows.push(("Endpoint", status.endpoint.display().to_string()));
    if let Some(server) = &status.server {
        rows.push((
            "Server",
            format!("{} (api {})", server.server_version, server.api_version),
        ));
        rows.push((
            "Capabilities",
            if server.capabilities.is_empty() {
                "none".to_owned()
            } else {
                server.capabilities.join(", ")
            },
        ));
    }
    rows.push(("Log", status.log.display().to_string()));

    let width = rows.iter().map(|(label, _)| label.len()).max().unwrap_or(0);
    let mut rendered = String::new();
    for (label, value) in rows {
        writeln!(rendered, "{label:width$}  {value}").expect("writing to a String cannot fail");
    }
    rendered
}

fn duration(seconds: u64) -> String {
    let (days, hours, minutes) = (
        seconds / 86_400,
        (seconds % 86_400) / 3_600,
        (seconds % 3_600) / 60,
    );
    if days > 0 {
        format!("{days}d {hours}h {minutes}m")
    } else if hours > 0 {
        format!("{hours}h {minutes}m")
    } else if minutes > 0 {
        format!("{minutes}m {}s", seconds % 60)
    } else {
        format!("{seconds}s")
    }
}

/// Describes the running service's process, when it published one.
fn pid_suffix(config: &Config) -> String {
    ServicePaths::of(config)
        .ok()
        .and_then(|paths| control_service::service_runtime_record(&paths))
        .map_or_else(String::new, |record| format!(" (pid {})", record.pid))
}

fn current_executable() -> Result<std::path::PathBuf, ControlError> {
    std::env::current_exe().map_err(|source| ControlError::Io {
        path: "the running nakode executable".to_owned(),
        source,
    })
}

#[cfg(test)]
mod tests {
    use super::{duration, render_status};
    use crate::control_service::{ServerReport, ServiceStatus};
    use std::path::PathBuf;

    fn status(running: bool) -> ServiceStatus {
        ServiceStatus {
            running,
            workspace: PathBuf::from("/workspace"),
            nakode_version: "0.3.0".to_owned(),
            pid: running.then_some(4242),
            started_at_unix_ms: running.then_some(1_754_000_000_000),
            started_at_utc: running.then(|| "2025-07-31T22:13:20Z".to_owned()),
            uptime_seconds: running.then_some(3_725),
            endpoint: PathBuf::from("/control/w/abcd/api.sock"),
            lifecycle_socket: PathBuf::from("/control/w/abcd/c.sock"),
            log: PathBuf::from("/control/w/abcd/service.log"),
            server: running.then(|| ServerReport {
                server_version: "0.3.0".to_owned(),
                api_version: "nakode.v1".to_owned(),
                capabilities: vec!["Resume".to_owned(), "Steering".to_owned()],
            }),
        }
    }

    #[test]
    fn running_status_reports_every_field_the_json_carries() {
        let rendered = render_status(&status(true));

        for expected in [
            "0.3.0",
            "/workspace",
            "running",
            "4242",
            "2025-07-31T22:13:20Z",
            "up 1h 2m",
            "/control/w/abcd/api.sock",
            "api nakode.v1",
            "Resume, Steering",
            "/control/w/abcd/service.log",
        ] {
            assert!(
                rendered.contains(expected),
                "status output is missing {expected:?}:\n{rendered}"
            );
        }
    }

    #[test]
    fn stopped_status_omits_process_and_server_detail() {
        let rendered = render_status(&status(false));

        assert!(rendered.contains("stopped"));
        assert!(!rendered.contains("PID"));
        assert!(!rendered.contains("Capabilities"));
        assert!(
            rendered.contains("/control/w/abcd/service.log"),
            "a stopped service still has a log path:\n{rendered}"
        );
    }

    #[test]
    fn uptime_reads_in_the_largest_useful_unit() {
        assert_eq!(duration(45), "45s");
        assert_eq!(duration(125), "2m 5s");
        assert_eq!(duration(3_725), "1h 2m");
        assert_eq!(duration(90_000), "1d 1h 0m");
    }
}
