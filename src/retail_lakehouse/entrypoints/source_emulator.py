"""source-emulator entry point (job.source_emulator wheel task).

Thin wrapper: parse args (R1 guard inside params.resolve) -> load + generate the
scenario -> write deterministic landing files into the UC volume.
"""

from __future__ import annotations

import sys

from retail_lakehouse.config import params
from retail_lakehouse.scenarios import generator, parser, writer


def _arg(args: list[str], key: str, default: str | None = None) -> str | None:
    for arg in args:
        token = arg.removeprefix("--")
        if token.startswith(f"{key}="):
            return token.partition("=")[2]
    return default


def run(args: list[str]) -> list[str]:
    resolved = params.from_task_args(args)  # R1 guard runs before any write
    scenario_name = _arg(args, "scenario", "day1_clean")
    base_path = _arg(
        args, "base_path", f"/Volumes/{resolved.catalog}/{resolved.landing_schema}/files"
    )
    files = generator.generate_batches(parser.load_scenario(scenario_name))
    written = writer.write_files(files, base_path)
    print(f"scenario={scenario_name} wrote {len(written)} file(s) under {base_path}")
    for path in written:
        print(f"  {path}")
    return written


def main() -> None:
    run(sys.argv[1:])
