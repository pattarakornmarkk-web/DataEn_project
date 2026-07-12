"""post_run_audit job task: harvest event log -> write ops.run_audit + ops.dq_results.

Thin wrapper around retail_lakehouse.audit — no logic here.
"""


def main() -> None:
    # TODO: parse task params (catalog, git_sha, environment_tag)
    # TODO: assert_catalog_matches_environment (R1 guard) before any write
    # TODO: harvest event log, build audit rows, append to ops tables
    raise NotImplementedError
