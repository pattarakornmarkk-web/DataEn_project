"""Pipeline/job parameter access — the ONLY module that reads spark.conf / task params.

Import rule (verified by review): transforms/ and entrypoints/ import from HERE, never
from pipelines/ glue files — no cross-glob imports inside the SDP runtime.
Every accessor runs the R1 catalog guard before returning.
"""


def from_spark_conf(spark) -> dict:
    """SDP path: read pipeline configuration (catalog, environment_tag, lateness, git_sha),
    run assert_catalog_matches_environment, return typed params."""
    raise NotImplementedError


def from_task_args(args: list[str]) -> dict:
    """Wheel-task path: parse named parameters, run the same guard, return typed params."""
    raise NotImplementedError
