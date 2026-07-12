"""Single parser for scenario YAML — no other module may read scenario files.

Contract: validates on load (unknown fault names, missing logical_clock, unknown
source names against conf/sources.yml are load-time errors, never silent no-ops).
"""


def list_scenarios() -> list[str]:
    """Names of all packaged scenarios (drives the README-vs-catalog drift meta-test)."""
    raise NotImplementedError


def load_scenario(name: str) -> dict:
    """Parse + validate one scenario into a typed spec (logical_clock, batches, faults)."""
    raise NotImplementedError
