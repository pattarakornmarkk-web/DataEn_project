#!/usr/bin/env bash
# Local test runner mirroring the CI gate (ci.yml is the source of truth).
set -euo pipefail
ruff check . && ruff format --check .
pytest -m "(unit or dq) and not integration" --cov
