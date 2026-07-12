"""Environment and configuration safety primitives.

Home of the R1 runtime catalog guard: every write path (pipeline bootstrap, wheel-task
params) asserts the resolved catalog belongs to the deployed environment BEFORE any
table is touched, so a mis-targeted deploy fails loudly instead of writing cross-env.

Contract-shape validation itself lives in config.loader (single owner of parsing).
"""


class ConfigError(ValueError):
    """Malformed packaged configuration — raised at load time, never a silent no-op."""


class EnvironmentMismatchError(RuntimeError):
    """R1 guard failure: resolved catalog does not belong to the deployed environment."""


_CATALOG_BY_ENVIRONMENT = {
    "dev": "retail_dev",
    "prod": "retail_prod",
}


def assert_catalog_matches_environment(catalog: str, environment_tag: str) -> None:
    """Fail unless (catalog, environment_tag) is exactly retail_dev<->dev or retail_prod<->prod."""
    expected = _CATALOG_BY_ENVIRONMENT.get(environment_tag)
    if expected is None:
        raise EnvironmentMismatchError(
            f"Unknown environment_tag {environment_tag!r}; "
            f"expected one of {sorted(_CATALOG_BY_ENVIRONMENT)}"
        )
    if catalog != expected:
        raise EnvironmentMismatchError(
            f"Catalog {catalog!r} does not belong to environment {environment_tag!r} "
            f"(expected {expected!r}). Refusing to run — check deploy target and variables."
        )
