#!/bin/sh
set -eu

repository=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
binary=${1:-"$repository/target/debug/nakode"}

cd "$repository/sdks/python"
uv run --locked python tests/conformance.py "$binary"
