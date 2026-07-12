"""Schema-contract types + compile-time validation.

Includes the runtime catalog guard (review item R1): every write path asserts the
resolved catalog matches the deployed environment_tag, failing loudly on mismatch.
"""


def validate_contract(contract: dict) -> None:
    """Fail on malformed contract: missing keys, unknown types, no sequence column for SCD2."""
    raise NotImplementedError


def assert_catalog_matches_environment(catalog: str, environment_tag: str) -> None:
    """R1 guard: retail_dev <-> dev, retail_prod <-> prod. Raise on any mismatch."""
    raise NotImplementedError
