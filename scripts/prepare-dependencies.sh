#!/usr/bin/env bash
# Runs only as explicit preMaterialize preparation, never as an ordinary init hook.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p .tmp/dependency-fetch .dependencies/config
export CARGO_HOME="$PWD/.tmp/dependency-fetch/cargo-home"
export CARGO_NET_GIT_FETCH_WITH_CLI=true
# Existing execution credentials only; no credential files enter the declared output trees.
if [[ -n ${GH_TOKEN:-} ]]; then
  export GIT_CONFIG_COUNT=1
  export GIT_CONFIG_KEY_0=credential.https://github.com.helper
  export GIT_CONFIG_VALUE_0='!f() { if [ "$1" = get ]; then printf "username=x-access-token\npassword=%s\n" "$GH_TOKEN"; fi; }; f'
fi
cargo vendor --locked --versioned-dirs .dependencies/vendor >.dependencies/config/config.toml
if [[ -f apps/dashboard/package.json ]]; then
  expected="$(python3 -c 'import json; print(json.load(open("apps/dashboard/package.json"))["packageManager"].split("@")[1])')"
  [[ "$(bun --version)" == "$expected" ]] || {
    echo 'Guest tool image Bun differs from the managed dashboard version; refresh the pinned image.' >&2
    exit 1
  }
  NODE_AUTH_TOKEN="${NODE_AUTH_TOKEN:-${GH_PACKAGES_TOKEN:-${GH_TOKEN:-}}}" bash scripts/install-dashboard-deps.sh
fi
