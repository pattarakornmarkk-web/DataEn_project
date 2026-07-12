"""Load conf/ registries (sources, contracts, dq_rules) into typed config objects.

ADR-0008: registries are read from PACKAGED data (retail_lakehouse/_data/conf via
importlib.resources), never from repo/workspace paths — identical behavior in CI,
wheel tasks, and SDP serverless. No function here takes a filesystem path.

Blueprint ref: Repository Structure §conf/, Architecture §2 (config as data in git).
"""


def data_root():
    """importlib.resources anchor for retail_lakehouse/_data — the only data accessor."""
    raise NotImplementedError


def load_sources() -> dict:
    """Parse packaged conf/sources.yml -> per-source landing paths, formats, patterns."""
    raise NotImplementedError


def load_contracts() -> dict:
    """Parse packaged conf/contracts.yml -> (source_contracts, entity_contracts).
    Enforces: sequence_by always a list; tiebreak required on scd_type-2 entities
    (ADR-0009); unknown fields are load-time errors."""
    raise NotImplementedError


def load_dq_rules() -> dict:
    """Parse packaged conf/dq_rules.yml -> rule registry. Unknown rule types raise at load."""
    raise NotImplementedError
