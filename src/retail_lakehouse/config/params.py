"""Pipeline/job parameter access — the ONLY module that reads spark.conf / task args.

Import rule: transforms/ and entrypoints/ import from HERE, never from pipelines/
glue files. Every resolution path runs the R1 catalog guard before returning, so a
mis-targeted deploy fails before any table is touched.

Testable core: resolve(mapping). from_spark_conf / from_task_args are thin adapters.
"""

from __future__ import annotations

from dataclasses import dataclass

from retail_lakehouse.config.contracts import ConfigError, assert_catalog_matches_environment

REQUIRED_PARAMS = (
    "catalog",
    "environment_tag",
    "git_sha",
    "lateness_window_days",
    "landing_schema",
    "bronze_schema",
    "silver_schema",
    "gold_schema",
    "ops_schema",
)


@dataclass(frozen=True)
class PipelineParams:
    catalog: str
    environment_tag: str
    git_sha: str
    lateness_window_days: int
    landing_schema: str
    bronze_schema: str
    silver_schema: str
    gold_schema: str
    ops_schema: str
    deployed_by: str = "unknown"

    def qualified(self, schema: str, table: str) -> str:
        """catalog.schema.table using the RESOLVED (possibly dev-prefixed) schema name."""
        return f"{self.catalog}.{getattr(self, f'{schema}_schema')}.{table}"


def resolve(values: dict) -> PipelineParams:
    """Validate a raw parameter mapping and run the R1 guard. Extra keys are ignored
    (jobs pass task-specific params like `scenario` alongside)."""
    missing = [key for key in REQUIRED_PARAMS if not values.get(key)]
    if missing:
        raise ConfigError(f"missing required pipeline parameter(s): {missing}")
    try:
        lateness = int(str(values["lateness_window_days"]))
    except ValueError as exc:
        raise ConfigError(
            f"lateness_window_days must be an integer, got {values['lateness_window_days']!r}"
        ) from exc
    if lateness <= 0:
        raise ConfigError(f"lateness_window_days must be positive, got {lateness}")

    assert_catalog_matches_environment(values["catalog"], values["environment_tag"])  # R1

    return PipelineParams(
        catalog=values["catalog"],
        environment_tag=values["environment_tag"],
        git_sha=values["git_sha"],
        lateness_window_days=lateness,
        landing_schema=values["landing_schema"],
        bronze_schema=values["bronze_schema"],
        silver_schema=values["silver_schema"],
        gold_schema=values["gold_schema"],
        ops_schema=values["ops_schema"],
        deployed_by=values.get("deployed_by") or "unknown",
    )


def from_spark_conf(spark) -> PipelineParams:
    """SDP path: pipeline `configuration` keys land in spark.conf (duck-typed for tests)."""
    values = {key: spark.conf.get(key, None) for key in (*REQUIRED_PARAMS, "deployed_by")}
    return resolve({k: v for k, v in values.items() if v is not None})


def from_task_args(args: list[str]) -> PipelineParams:
    """Wheel-task path: python_wheel_task named_parameters arrive as --key=value."""
    values: dict[str, str] = {}
    for arg in args:
        token = arg.removeprefix("--")
        if "=" in token:
            key, _, value = token.partition("=")
            values[key] = value
    return resolve(values)
