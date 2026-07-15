"""Pipeline parameter resolution + R1 guard integration."""

import pytest

from retail_lakehouse.config import params
from retail_lakehouse.config.contracts import ConfigError, EnvironmentMismatchError

pytestmark = pytest.mark.unit

GOOD = {
    "catalog": "retail_dev",
    "environment_tag": "dev",
    "git_sha": "abc1234",
    "lateness_window_days": "7",
    "landing_schema": "dev_x_landing",
    "bronze_schema": "dev_x_bronze",
    "silver_schema": "dev_x_silver",
    "gold_schema": "dev_x_gold",
    "ops_schema": "dev_x_ops",
}


class TestResolve:
    def test_happy_path_types_and_defaults(self):
        resolved = params.resolve(GOOD)
        assert resolved.lateness_window_days == 7  # int, not str
        assert resolved.deployed_by == "unknown"

    def test_qualified_uses_resolved_schema_names(self):
        resolved = params.resolve(GOOD)
        assert (
            resolved.qualified("bronze", "customers_raw") == "retail_dev.dev_x_bronze.customers_raw"
        )

    def test_missing_parameter_raises_with_names(self):
        broken = {k: v for k, v in GOOD.items() if k != "ops_schema"}
        with pytest.raises(ConfigError, match="ops_schema"):
            params.resolve(broken)

    @pytest.mark.parametrize("bad", ["0", "-1", "abc"])
    def test_invalid_lateness_rejected(self, bad):
        with pytest.raises(ConfigError, match="lateness_window_days"):
            params.resolve({**GOOD, "lateness_window_days": bad})

    def test_r1_guard_blocks_cross_environment(self):
        with pytest.raises(EnvironmentMismatchError):
            params.resolve({**GOOD, "catalog": "retail_prod"})  # prod catalog, dev tag

    def test_extra_keys_ignored(self):
        assert params.resolve({**GOOD, "scenario": "day1_clean"}).catalog == "retail_dev"


class TestAdapters:
    def test_from_task_args_parses_named_parameters(self):
        args = [f"--{k}={v}" for k, v in GOOD.items()] + ["--deployed_by=ci", "--scenario=x"]
        resolved = params.from_task_args(args)
        assert resolved.deployed_by == "ci"
        assert resolved.catalog == "retail_dev"

    def test_from_task_args_missing_params_raise(self):
        with pytest.raises(ConfigError, match="missing required"):
            params.from_task_args(["--catalog=retail_dev"])

    def test_from_spark_conf_duck_typed(self):
        class FakeConf:
            def get(self, key, default=None):
                return GOOD.get(key, default)

        class FakeSpark:
            conf = FakeConf()

        resolved = params.from_spark_conf(FakeSpark())
        assert resolved.environment_tag == "dev"
        assert resolved.lateness_window_days == 7
