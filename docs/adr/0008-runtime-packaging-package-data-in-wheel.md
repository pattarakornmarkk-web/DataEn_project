# ADR-0008: Runtime packaging — package data in the wheel

**Status:** accepted
**Date:** 2026-07-12

## Context

`conf/` registries and `mock_data/` scenarios must be readable in three runtimes:
CI runners (pytest), serverless wheel tasks (emulator, audit, assertions), and the
SDP pipeline. Two options: (a) ship them inside the wheel as package data, or
(b) read bundle-synced workspace files. Workspace paths differ between dev
(development-mode prefixing, per-user root) and prod, are awkward to resolve from
serverless compute, and would make local tests read from a different mechanism than
deployed code.

## Decision

Package data in the wheel. `pyproject.toml` force-includes `conf/` and
`mock_data/{scenarios,seeds}/` into `retail_lakehouse/_data/`. All access goes through
`retail_lakehouse.config.loader.data_root()` (importlib.resources); no module may take
a filesystem path to registry or scenario data. Repo layout keeps `conf/` and
`mock_data/` top-level for reviewability — the mapping happens at build time.

## Consequences

- Identical data access in CI, wheel tasks, and SDP serverless; no path resolution logic.
- Config/scenario changes require a wheel rebuild + deploy — intentional: config rides
  the same CI gate (lint, registry meta-tests, validate) as code.
- Data version is atomically tied to code version (git_sha covers both).
- Revisit trigger: if registries need to change independently of releases (ops-managed
  thresholds), split those specific values into job/pipeline parameters — not files.
