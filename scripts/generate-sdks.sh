#!/bin/sh
set -eu

repository=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
python_sdk="$repository/sdks/python"

cd "$python_sdk"
uv run --locked python -m grpc_tools.protoc \
  --proto_path="$repository/proto" \
  --python_out=. \
  --pyi_out=. \
  --grpc_python_out=. \
  "$repository/proto/nakode/v1/nakode.proto"
