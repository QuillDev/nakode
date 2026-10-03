#!/usr/bin/env python3
"""Activate restored vendored dependencies without reinstalling or replacing owner config."""
from pathlib import Path

root = Path(__file__).resolve().parents[1]
source = root / '.dependencies/config/config.toml'
if not source.is_file() or source.is_symlink():
    raise SystemExit('Explicit dependency preparation/restore is required before initialization')
directory = root / '.cargo'
if directory.is_symlink():
    raise SystemExit('Refusing aliased Cargo configuration')
directory.mkdir(exist_ok=True)
target = directory / 'config.toml'
if target.is_symlink() or (target.exists() and target.read_bytes() != source.read_bytes()):
    raise SystemExit('Existing Cargo configuration differs; retained for explicit reconciliation')
if not target.exists():
    with target.open('xb') as output:
        output.write(source.read_bytes())
