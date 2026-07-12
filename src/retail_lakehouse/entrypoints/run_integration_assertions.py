"""integration-assertions entry point (job.integration_tests wheel task).

Thin wrapper: params.from_task_args -> run every integration.assertions function ->
print structured results -> exit non-zero if any check failed (fails the job task,
which fails deploy-dev, which blocks promotion).
"""


def main() -> None:
    raise NotImplementedError
