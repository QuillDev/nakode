#!/bin/bash
# Runs inside the container as the unprivileged `bench` user.
set -euo pipefail
Xvfb :99 -screen 0 1280x900x24 -nolisten tcp >/tmp/xvfb.log 2>&1 &
sleep 1
export DISPLAY=:99
openbox >/tmp/openbox.log 2>&1 &
sleep 1
cd /src
export CARGO_TARGET_DIR=/target
export NAKODE_BROWSER_BENCH_OUT=/out/linux-results.json
cargo test --release --lib browser_control -- --include-ignored --nocapture --test-threads=1
