#!/bin/sh
set -eu

script_directory="$(CDPATH= cd -P "$(dirname "$0")" && pwd -P)"
cd "$script_directory"

clean_install=false
launches_tui=true
workspace="${NAKODE_WORKSPACE:-${NAKO_AGENT_WORKSPACE:-.}}"
workspace_argument_follows=false
remaining_arguments=$#
while [ "$remaining_arguments" -gt 0 ]; do
  argument=$1
  shift
  remaining_arguments=$((remaining_arguments - 1))

  case "$argument" in
    --clean)
      clean_install=true
      continue
      ;;
  esac

  if [ "$workspace_argument_follows" = true ]; then
    workspace=$argument
    workspace_argument_follows=false
  fi

  case "$argument" in
    -h | --help | -V | --version | agent | help)
      launches_tui=false
      ;;
    --workspace)
      workspace_argument_follows=true
      ;;
    --workspace=*)
      workspace=${argument#--workspace=}
      ;;
  esac

  # Rotate each retained argument to the end. After the original argument count
  # is consumed, the positional parameters contain the same list without --clean.
  set -- "$@" "$argument"
done

if [ "$launches_tui" = true ]; then
  if [ "$(id -u)" -eq 0 ] && [ -n "${SUDO_USER:-}" ]; then
    printf '%s\n' \
      "dev.sh must run as ${SUDO_USER}, not through sudo." \
      "Running Nakode as root prevents it from reaching your desktop browser and user credentials."
    exit 2
  fi

  workspace="$(CDPATH= cd -P "$workspace" && pwd -P)"
  for control_socket in \
    "$workspace/.nakode/control.sock" \
    "$workspace/.nako-agent/control.sock"; do
    if [ ! -S "$control_socket" ]; then
      continue
    fi
    if ! command -v lsof >/dev/null 2>&1; then
      printf 'Cannot restart the existing Nakode at %s: lsof is not installed.\n' \
        "$control_socket" >&2
      exit 1
    fi
    # Keep the PID list newline-delimited so paths and shell word splitting do
    # not change which listener is inspected.
    listener_pids="$(
      { lsof -t -- "$control_socket" 2>/dev/null || :; } | sort -u
    )"
    original_ifs=$IFS
    IFS='
'
    for listener_pid in $listener_pids; do
      process_name="$(ps -p "$listener_pid" -o comm= 2>/dev/null || true)"
      process_name="${process_name#"${process_name%%[![:space:]]*}"}"
      process_name="${process_name%"${process_name##*[![:space:]]}"}"
      process_name="${process_name##*/}"
      if [ -z "$process_name" ] && ! kill -0 "$listener_pid" 2>/dev/null; then
        continue
      fi
      case "$process_name" in
        nakode | nako-agent) ;;
        *)
          printf 'Refusing to stop unexpected process %s (%s) listening at %s.\n' \
            "$listener_pid" "$process_name" "$control_socket" >&2
          exit 1
          ;;
      esac
      if ! kill -TERM "$listener_pid" 2>/dev/null \
        && kill -0 "$listener_pid" 2>/dev/null; then
        printf 'Could not stop Nakode process %s. Stop it manually and retry.\n' \
          "$listener_pid" >&2
        exit 1
      fi
    done

    for listener_pid in $listener_pids; do
      wait_seconds=0
      while kill -0 "$listener_pid" 2>/dev/null; do
        if [ "$wait_seconds" -ge 5 ]; then
          printf 'Nakode process %s did not stop within five seconds.\n' \
            "$listener_pid" >&2
          exit 1
        fi
        sleep 1
        wait_seconds=$((wait_seconds + 1))
      done
    done
    IFS=$original_ifs
  done
fi

if [ "$clean_install" = true ]; then
  temporary_root="${TMPDIR:-/tmp}"
  if [ "$temporary_root" != / ]; then
    temporary_root=${temporary_root%/}
  fi
  clean_root="$(mktemp -d "$temporary_root/nakode-dev.XXXXXXXX")"
  cleanup_clean_install() {
    rm -rf "$clean_root"
  }
  trap cleanup_clean_install 0
  trap 'exit 129' 1
  trap 'exit 130' 2
  trap 'exit 143' 15
  export XDG_DATA_HOME="$clean_root/data"
  export NAKODE_CONTROL_DIR="$clean_root/control"
  export NAKODE_AGENTS="$clean_root/agents"
fi

cargo run -- "$@"
