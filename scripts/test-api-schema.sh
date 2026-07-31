#!/bin/sh
set -eu

repository=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
CARGO_TARGET_DIR="$repository/target" \
  cargo test --manifest-path "$repository/crates/nakode-api/Cargo.toml"
