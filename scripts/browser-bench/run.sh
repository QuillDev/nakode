#!/bin/bash
# Benchmarks browser_control against the desktop (CUA) tools on a Linux X desktop in Docker.
# Results: .tmp/bench-out/linux-results.json, a desktop frame beside it, and a markdown table on stdout.
set -euo pipefail
root="$(cd "$(dirname "$0")/../.." && pwd)"
mkdir -p "$root/.tmp/bench-out"
docker build -t nakode-browser-bench "$root/scripts/browser-bench"
docker run --rm --security-opt seccomp=unconfined --shm-size=1g \
  -v "$root":/src:ro -v nakode-bench-target:/target -v "$root/.tmp/bench-out":/out \
  nakode-browser-bench bash -c \
  'chown bench /target /out && su bench -c "CARGO_HOME=/home/bench/.cargo PATH=/usr/local/cargo/bin:\$PATH RUSTUP_HOME=/usr/local/rustup /src/scripts/browser-bench/run-in-container.sh"'
