#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

clean_install=false
launches_tui=true
app_arguments=()
for argument in "$@"; do
  case "$argument" in
    --clean)
      clean_install=true
      ;;
    -h | --help | -V | --version | agent | help)
      launches_tui=false
      app_arguments+=("$argument")
      ;;
    *)
      app_arguments+=("$argument")
      ;;
  esac
done

if [[ "$launches_tui" == true ]]; then
  if [[ "${EUID}" -eq 0 && -n "${SUDO_USER:-}" ]]; then
    printf '%s\n' \
      "dev.sh must run as ${SUDO_USER}, not through sudo." \
      "Running Nako Agent as root prevents it from reaching your desktop browser and user credentials."
    exit 2
  fi

  workspace="${NAKO_AGENT_WORKSPACE:-.}"
  for ((index = 0; index < ${#app_arguments[@]}; index++)); do
    case "${app_arguments[index]}" in
      --workspace)
        ((index += 1))
        workspace="${app_arguments[index]:-}"
        ;;
      --workspace=*)
        workspace="${app_arguments[index]#--workspace=}"
        ;;
    esac
  done
  workspace="$(cd "$workspace" && pwd -P)"
  control_socket="$workspace/.nako-agent/control.sock"

  if [[ -S "$control_socket" ]]; then
    if ! command -v lsof >/dev/null 2>&1; then
      printf 'Cannot restart the existing Nako Agent at %s: lsof is not installed.\n' \
        "$control_socket" >&2
      exit 1
    fi
    mapfile -t listener_pids < <(lsof -t -- "$control_socket" 2>/dev/null | sort -u)
    for listener_pid in "${listener_pids[@]}"; do
      process_name="$(ps -p "$listener_pid" -o comm= | tr -d '[:space:]')"
      if [[ "$process_name" != "nako-agent" ]]; then
        printf 'Refusing to stop unexpected process %s (%s) listening at %s.\n' \
          "$listener_pid" "$process_name" "$control_socket" >&2
        exit 1
      fi
      if ! kill -TERM "$listener_pid"; then
        printf 'Could not stop Nako Agent process %s. Stop it manually and retry.\n' \
          "$listener_pid" >&2
        exit 1
      fi
    done

    deadline=$((SECONDS + 5))
    for listener_pid in "${listener_pids[@]}"; do
      while kill -0 "$listener_pid" 2>/dev/null; do
        if ((SECONDS >= deadline)); then
          printf 'Nako Agent process %s did not stop within five seconds.\n' \
            "$listener_pid" >&2
          exit 1
        fi
        sleep 0.1
      done
    done
  fi
fi

if [[ "$clean_install" == true ]]; then
  clean_root="$(mktemp -d "${TMPDIR:-/tmp}/nako-agent-dev.XXXXXXXX")"
  trap 'rm -rf -- "$clean_root"' EXIT
  export XDG_DATA_HOME="$clean_root/data"
  export NAKO_AGENT_AGENTS="$clean_root/agents"
fi

cargo run -- "${app_arguments[@]}"
