"""source-emulator entry point (job.source_emulator wheel task).

Thin wrapper: params.from_task_args (includes R1 guard) -> scenarios.parser.load_scenario
-> scenarios.generator.generate_batches -> write files into the landing volume.
No interpretation logic here — the scenarios module owns it.
"""


def main() -> None:
    raise NotImplementedError
