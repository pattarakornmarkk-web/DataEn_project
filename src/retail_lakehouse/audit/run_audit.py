"""Builds ops.run_audit rows: run id, git_sha, environment_tag, counts, status.

Every prod table row is traceable to the commit that produced it via git_sha.
"""

from pyspark.sql import DataFrame


def build_run_audit_row(flow_progress: DataFrame, git_sha: str, environment_tag: str) -> DataFrame:
    raise NotImplementedError
