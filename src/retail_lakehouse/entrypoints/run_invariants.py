"""run-invariants entry point (orchestrator invariant_checks wheel task).

Thin wrapper: params.from_task_args -> run audit.invariants checks against the catalog
-> write results to ops.dq_results -> exit non-zero on violations.
Same definitions pytest runs in tests/data_quality/invariants/.
"""


def main() -> None:
    raise NotImplementedError
